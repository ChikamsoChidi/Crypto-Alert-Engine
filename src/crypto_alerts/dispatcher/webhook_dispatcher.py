# src/crypto_alerts/dispatcher/webhook_dispatcher.py
#
# WebhookDispatcher delivers alert events to an external HTTP endpoint
# via a POST request. This is the pattern used by real alerting systems:
# PagerDuty, Slack, and most monitoring platforms accept webhook payloads.
#
# Retry strategy:
#   On a transient HTTP error (5xx, connection timeout), the dispatcher
#   retries up to max_retries times with an exponential backoff delay.
#   On a permanent client error (4xx), it logs and gives up immediately
#   since retrying a bad request will never succeed.

import asyncio
import logging

import aiohttp

from crypto_alerts.dispatcher.base import AbstractDispatcher
from crypto_alerts.models.alert_event import AlertEvent

logger = logging.getLogger(__name__)

# Maximum number of retry attempts on transient failures
DEFAULT_MAX_RETRIES: int = 3

# Base delay in seconds for exponential backoff
# Attempt 1: 1s, Attempt 2: 2s, Attempt 3: 4s
DEFAULT_BACKOFF_BASE: float = 1.0

# Total seconds to wait for a server response before giving up
DEFAULT_TIMEOUT_SECONDS: float = 10.0


class WebhookDispatcher(AbstractDispatcher):
    """
    POSTs alert events as JSON to a configurable HTTP endpoint.

    The aiohttp ClientSession is created once and reused across all
    dispatch calls. Creating a new session per request is a common
    mistake -- it defeats connection pooling and adds significant
    latency on high-volume alert streams.

    The session is created lazily on the first dispatch call rather
    than in __init__ because __init__ is synchronous and creating an
    aiohttp session requires a running event loop.
    """

    def __init__(
        self,
        inbound_queue: asyncio.Queue[AlertEvent],
        webhook_url: str,
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_base: float = DEFAULT_BACKOFF_BASE,
        timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        super().__init__(inbound_queue)
        self._webhook_url = webhook_url
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: aiohttp.ClientSession | None = None

    def _get_session(self) -> aiohttp.ClientSession:
        """
        Returns the shared aiohttp session, creating it on first call.
        Lazy initialization avoids event loop issues at construction time.
        """
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    def _build_payload(self, event: AlertEvent) -> dict[str, str]:
        """
        Serializes an AlertEvent into a JSON-safe flat dictionary.

        We convert Decimal to str rather than float to preserve exact
        numeric representation in the payload. The receiver should parse
        price fields as Decimal or a fixed-point type, not float.
        """
        return {
            "event_id": event.event_id,
            "fired_at": event.fired_at.isoformat(),
            "symbol": event.rule.symbol,
            "operator": str(event.rule.operator),
            "threshold": str(event.rule.threshold),
            "current_price": str(event.tick.price),
            "rule_label": event.rule.label,
            "summary": event.summary(),
        }

    async def dispatch(self, event: AlertEvent) -> None:
        """
        Sends the alert event to the webhook URL with retry logic.
        Raises on permanent failure after all retries are exhausted.
        """
        payload = self._build_payload(event)
        session = self._get_session()
        last_exc: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            try:
                async with session.post(
                    self._webhook_url, json=payload
                ) as response:

                    if response.status < 400:
                        logger.info(
                            "Webhook delivered -- event_id=%s status=%d attempt=%d",
                            event.event_id,
                            response.status,
                            attempt,
                        )
                        return

                    if 400 <= response.status < 500:
                        # Client errors are permanent -- retrying will not help
                        body = await response.text()
                        logger.error(
                            "Webhook permanent failure -- event_id=%s status=%d body=%s",
                            event.event_id,
                            response.status,
                            body[:200],
                        )
                        raise RuntimeError(
                            f"Webhook returned permanent error {response.status}"
                        )

                    # 5xx -- transient server error, fall through to retry
                    logger.warning(
                        "Webhook transient failure -- event_id=%s status=%d attempt=%d/%d",
                        event.event_id,
                        response.status,
                        attempt,
                        self._max_retries,
                    )
                    last_exc = RuntimeError(
                        f"Webhook returned server error {response.status}"
                    )

            except aiohttp.ClientError as exc:
                # Network-level errors (DNS failure, connection refused, timeout)
                logger.warning(
                    "Webhook network error -- event_id=%s error=%s attempt=%d/%d",
                    event.event_id,
                    exc,
                    attempt,
                    self._max_retries,
                )
                last_exc = exc

            if attempt < self._max_retries:
                # Exponential backoff: 1s, 2s, 4s, ...
                delay = self._backoff_base * (2 ** (attempt - 1))
                logger.info(
                    "Retrying webhook in %.1fs -- event_id=%s",
                    delay,
                    event.event_id,
                )
                await asyncio.sleep(delay)

        raise RuntimeError(
            f"Webhook failed after {self._max_retries} attempts"
        ) from last_exc

    async def close(self) -> None:
        """
        Closes the underlying aiohttp session.
        Must be called on shutdown to release the connection pool.
        """
        if self._session and not self._session.closed:
            await self._session.close()
            logger.info("WebhookDispatcher session closed")