# src/crypto_alerts/dispatcher/base.py
#
# AbstractDispatcher defines the contract every dispatcher must satisfy.
# The pipeline coordinator holds a list of AbstractDispatcher instances
# and calls dispatch() on each one when an alert fires. It never imports
# ConsoleDispatcher or WebhookDispatcher directly.

import asyncio
from abc import ABC, abstractmethod

from crypto_alerts.models.alert_event import AlertEvent


class AbstractDispatcher(ABC):
    """
    Base class for all alert dispatchers.

    A dispatcher has one job: receive an AlertEvent and deliver it
    somewhere. Where that somewhere is (console, webhook, email, SMS)
    is entirely up to the implementation.

    The run() method is a long-running loop that pulls events off the
    shared outbound queue. dispatch() handles a single event and is
    the method subclasses must implement.
    """

    def __init__(self, inbound_queue: asyncio.Queue[AlertEvent]) -> None:
        self._queue = inbound_queue
        self._dispatched_count: int = 0
        self._failed_count: int = 0

    async def run(self) -> None:
        """
        Main loop. Pulls AlertEvents off the queue and dispatches them.
        Runs until the task is cancelled.

        Dispatch failures are caught and logged here rather than in each
        subclass -- this guarantees a single consistent error handling
        policy regardless of which dispatcher is running. A failed
        dispatch never crashes the loop or causes event loss for other
        dispatchers.
        """
        import logging
        logger = logging.getLogger(self.__class__.__name__)

        logger.info("%s started", self.__class__.__name__)

        try:
            while True:
                event: AlertEvent = await self._queue.get()
                try:
                    await self.dispatch(event)
                    self._dispatched_count += 1
                except Exception as exc:
                    self._failed_count += 1
                    logger.error(
                        "Dispatch failed -- event_id=%s error=%s: %s",
                        event.event_id,
                        type(exc).__name__,
                        exc,
                    )
                finally:
                    self._queue.task_done()

        except asyncio.CancelledError:
            logger.info(
                "%s shutting down -- dispatched=%d failed=%d",
                self.__class__.__name__,
                self._dispatched_count,
                self._failed_count,
            )
            raise

    @abstractmethod
    async def dispatch(self, event: AlertEvent) -> None:
        """
        Delivers a single AlertEvent to the target channel.
        Implementations should raise on unrecoverable errors -- the
        base run() loop will catch, log, and continue.
        """
        ...

    @property
    def stats(self) -> dict[str, int]:
        """Returns runtime statistics for monitoring and diagnostics."""
        return {
            "dispatched": self._dispatched_count,
            "failed": self._failed_count,
        }