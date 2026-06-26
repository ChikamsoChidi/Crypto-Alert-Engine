# src/crypto_alerts/dispatcher/console_dispatcher.py
#
# ConsoleDispatcher is the simplest possible dispatcher -- it prints
# alert events to stdout. Its value is in development and debugging:
# you see exactly what the engine is firing and when, with zero
# external dependencies.

import asyncio
import logging

from crypto_alerts.dispatcher.base import AbstractDispatcher
from crypto_alerts.models.alert_event import AlertEvent

logger = logging.getLogger(__name__)


class ConsoleDispatcher(AbstractDispatcher):
    """
    Writes alert events to stdout using the standard logging system.

    We write through the logger rather than print() for two reasons:
      1. Log output can be redirected, filtered, and formatted
         without changing this code.
      2. print() is not thread-safe in all environments -- the logging
         module uses internal locks to prevent interleaved output.
    """

    def __init__(self, inbound_queue: asyncio.Queue[AlertEvent]) -> None:
        super().__init__(inbound_queue)

    async def dispatch(self, event: AlertEvent) -> None:
        """
        Logs the alert event summary at WARNING level so it stands out
        visually in a log stream that is otherwise INFO-level noise.
        """
        logger.warning(event.summary())