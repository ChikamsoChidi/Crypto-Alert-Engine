# src/crypto_alerts/pipeline/coordinator.py
#
# PipelineCoordinator is the single place where all components are
# assembled and run together. It owns the queues, creates the tasks,
# and ensures every component shuts down cleanly in the right order.
#
# Shutdown order matters:
#   1. Cancel the feed first -- stops new ticks from entering the pipeline
#   2. Wait for the evaluator to drain the inbound queue
#   3. Wait for the dispatcher to drain the outbound queue
#   4. Cancel evaluator and dispatcher tasks
#
# This order guarantees that no ticks or events are lost on shutdown.

import asyncio
import logging
from typing import Sequence

from crypto_alerts.config.settings import get_settings
from crypto_alerts.dispatcher.base import AbstractDispatcher
from crypto_alerts.dispatcher.webhook_dispatcher import WebhookDispatcher
from crypto_alerts.engine.evaluator import Evaluator
from crypto_alerts.engine.rule_repository import RuleRepository
from crypto_alerts.feed.base import AbstractFeed
from crypto_alerts.models.alert_event import AlertEvent
from crypto_alerts.models.price_tick import PriceTick

logger = logging.getLogger(__name__)


class PipelineCoordinator:
    """
    Wires the feed, evaluator, and dispatchers into a single runnable
    pipeline and manages the lifecycle of every async task.

    The coordinator owns the two queues that connect the stages:
      - inbound_queue:  feed  --> evaluator
      - outbound_queue: evaluator --> dispatchers

    Both queues are bounded by PIPELINE_QUEUE_MAX_SIZE from settings.
    The same outbound queue is shared by all dispatchers. Each dispatcher
    independently consumes events from it, so only one dispatcher will
    receive any given event. If you need fan-out (every dispatcher gets
    every event), the coordinator would need one queue per dispatcher --
    a deliberate design decision documented here for interview discussion.
    """

    def __init__(
        self,
        feed: AbstractFeed,
        repository: RuleRepository,
        dispatchers: Sequence[AbstractDispatcher],
    ) -> None:
        self._feed = feed
        self._repository = repository
        self._dispatchers = dispatchers
        self._settings = get_settings()

        queue_size = self._settings.pipeline_queue_max_size

        self._inbound_queue: asyncio.Queue[PriceTick] = asyncio.Queue(
            maxsize=queue_size
        )
        self._outbound_queue: asyncio.Queue[AlertEvent] = asyncio.Queue(
            maxsize=queue_size
        )

        # Task references are stored so we can cancel them on shutdown
        self._feed_task: asyncio.Task[None] | None = None
        self._evaluator_task: asyncio.Task[None] | None = None
        self._dispatcher_tasks: list[asyncio.Task[None]] = []

        # The evaluator is constructed here because it needs both queues,
        # which are created in __init__. The feed and dispatchers are
        # injected from outside because they carry external configuration
        # (WebSocket URL, webhook URL) that the coordinator should not
        # need to know about.
        self._evaluator = Evaluator(
            repository=repository,
            inbound_queue=self._inbound_queue,
            outbound_queue=self._outbound_queue,
        )

    async def run(self) -> None:
        """
        Starts all pipeline tasks and waits until one of them fails or
        the coordinator itself is cancelled.

        asyncio.gather() runs all tasks concurrently. The
        return_exceptions=False default means the first task to raise
        an unhandled exception cancels all others immediately -- this
        is the correct behavior for a pipeline where any stage failure
        should halt the whole system.
        """
        await self._start_tasks()

        logger.info(
            "Pipeline running -- inbound_queue_max=%d outbound_queue_max=%d",
            self._settings.pipeline_queue_max_size,
            self._settings.pipeline_queue_max_size,
        )

        all_tasks = [
            t
            for t in [
                self._feed_task,
                self._evaluator_task,
                *self._dispatcher_tasks,
            ]
            if t is not None
        ]

        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            logger.info("Pipeline received cancellation signal")
            raise
        except Exception as exc:
            logger.critical(
                "Pipeline task failed with unhandled exception -- %s: %s",
                type(exc).__name__,
                exc,
            )
            raise
        finally:
            await self._shutdown()

    async def _start_tasks(self) -> None:
        """
        Creates and starts all pipeline tasks.

        We must inject the queues into the feed and dispatchers here
        rather than at construction time because the queues are created
        in __init__ -- a chicken-and-egg problem solved by wiring at
        start time.
        """
        # Re-wire the feed and dispatchers to use our queues.
        # AbstractFeed and AbstractDispatcher store their queues as
        # self._queue so we set them directly here.
        self._feed._queue = self._inbound_queue
        for dispatcher in self._dispatchers:
            dispatcher._queue = self._outbound_queue

        self._feed_task = asyncio.create_task(
            self._feed.run(), name="feed"
        )
        self._evaluator_task = asyncio.create_task(
            self._evaluator.run(), name="evaluator"
        )
        self._dispatcher_tasks = [
            asyncio.create_task(
                d.run(), name=f"dispatcher-{type(d).__name__}"
            )
            for d in self._dispatchers
        ]

        logger.info(
            "Started %d pipeline task(s) -- feed + evaluator + %d dispatcher(s)",
            2 + len(self._dispatcher_tasks),
            len(self._dispatcher_tasks),
        )

    async def _shutdown(self) -> None:
        """
        Graceful shutdown sequence.

        Stage 1 -- stop the feed so no new ticks enter the pipeline.
        Stage 2 -- drain the inbound queue so the evaluator processes
                   every tick already received.
        Stage 3 -- drain the outbound queue so dispatchers deliver every
                   event already fired.
        Stage 4 -- cancel remaining tasks and close external resources.
        """
        logger.info("Pipeline shutdown initiated")

        # Stage 1: stop the feed
        if self._feed_task and not self._feed_task.done():
            self._feed_task.cancel()
            try:
                await self._feed_task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("Feed stopped")

        # Stage 2: drain inbound queue
        if not self._inbound_queue.empty():
            logger.info(
                "Draining inbound queue -- %d ticks remaining",
                self._inbound_queue.qsize(),
            )
            await self._inbound_queue.join()

        # Stage 3: drain outbound queue
        if not self._outbound_queue.empty():
            logger.info(
                "Draining outbound queue -- %d events remaining",
                self._outbound_queue.qsize(),
            )
            await self._outbound_queue.join()

        # Stage 4: cancel evaluator and dispatchers
        remaining: list[asyncio.Task[None]] = [
            t
            for t in [self._evaluator_task, *self._dispatcher_tasks]
            if t is not None and not t.done()
        ]

        for task in remaining:
            task.cancel()

        if remaining:
            await asyncio.gather(*remaining, return_exceptions=True)

        # Close any dispatcher resources (e.g. aiohttp sessions)
        for dispatcher in self._dispatchers:
            if isinstance(dispatcher, WebhookDispatcher):
                await dispatcher.close()

        logger.info(
            "Pipeline shutdown complete -- evaluator_stats=%s",
            self._evaluator.stats,
        )