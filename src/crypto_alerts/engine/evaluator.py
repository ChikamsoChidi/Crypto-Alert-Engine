# src/crypto_alerts/engine/evaluator.py
#
# The Evaluator is a long-running async worker that sits between the
# feed queue and the dispatcher queue.
#
# Its job is simple and focused:
#   1. Pull a PriceTick off the inbound queue
#   2. Look up all rules for that tick's symbol
#   3. Evaluate each rule against the tick
#   4. Push any fired AlertEvents onto the outbound queue
#
# It deliberately does no I/O of its own. All I/O (WebSocket reading,
# notification sending) happens in other components. This makes the
# evaluator fully synchronous in its logic, which makes it trivial to
# unit test without any async machinery.

import asyncio
import logging

from crypto_alerts.engine.rule_repository import RuleRepository
from crypto_alerts.models.alert_event import AlertEvent
from crypto_alerts.models.price_tick import PriceTick

logger = logging.getLogger(__name__)


class Evaluator:
    """
    Async pipeline stage that evaluates price ticks against alert rules.

    The Evaluator owns two queues:
      - inbound_queue:  receives PriceTick objects from the feed
      - outbound_queue: sends AlertEvent objects to the dispatcher

    Both queues are bounded (max size set at construction time). A bounded
    inbound queue applies back-pressure to the feed -- if the evaluator
    falls behind, the feed will block rather than letting memory grow
    without limit. A bounded outbound queue does the same to the evaluator
    if the dispatcher is slow.
    """

    def __init__(
        self,
        repository: RuleRepository,
        inbound_queue: asyncio.Queue[PriceTick],
        outbound_queue: asyncio.Queue[AlertEvent],
    ) -> None:
        self._repository = repository
        self._inbound = inbound_queue
        self._outbound = outbound_queue
        self._processed_count: int = 0
        self._fired_count: int = 0

    async def run(self) -> None:
        """
        Main loop. Runs until the task is cancelled.

        We catch CancelledError explicitly to log a clean shutdown
        message rather than letting the exception propagate silently.
        asyncio.CancelledError is re-raised after logging because the
        asyncio runtime requires it to properly cancel the task -- 
        swallowing it would cause the task to hang on shutdown.
        """
        logger.info("Evaluator started")
        try:
            while True:
                await self._process_next_tick()
        except asyncio.CancelledError:
            logger.info(
                "Evaluator shutting down -- processed=%d fired=%d",
                self._processed_count,
                self._fired_count,
            )
            raise

    async def _process_next_tick(self) -> None:
        """
        Pulls one tick from the inbound queue and evaluates it.

        This is a separate method (not inlined into run()) so it can be
        called directly in unit tests without running the infinite loop.
        """
        tick: PriceTick = await self._inbound.get()

        try:
            rules = self._repository.get_rules_for_symbol(tick.symbol)

            if not rules:
                # No rules for this symbol -- common case, exit fast
                return

            fired_count = 0
            for rule in rules:
                if rule.evaluate(tick):
                    event = AlertEvent(rule=rule, tick=tick)
                    await self._outbound.put(event)
                    fired_count += 1
                    logger.debug(
                        "Rule fired -- rule_id=%s symbol=%s price=%s",
                        rule.rule_id,
                        tick.symbol,
                        tick.price,
                    )

            self._processed_count += 1

            if fired_count > 0:
                self._fired_count += fired_count

        finally:
            # Always mark the task done, even if evaluation raised an
            # exception. Failing to call task_done() would cause
            # queue.join() to block forever in tests that use it.
            self._inbound.task_done()

    @property
    def stats(self) -> dict[str, int]:
        """Returns runtime statistics for monitoring and diagnostics."""
        return {
            "processed": self._processed_count,
            "fired": self._fired_count,
        }