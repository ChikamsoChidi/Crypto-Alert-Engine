# src/crypto_alerts/feed/binance_feed.py
#
# BinanceFeed connects to the Binance WebSocket stream API and converts
# raw trade messages into PriceTick objects.
#
# Binance stream URL format for multiple symbols:
#   wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
#
# Raw Binance trade message shape (fields we care about):
#   {
#     "stream": "btcusdt@trade",
#     "data": {
#       "s": "BTCUSDT",   -- symbol
#       "p": "71000.00",  -- price as string
#       "q": "0.5",       -- quantity (volume) as string
#       "T": 1700000000000 -- trade time as millisecond epoch int
#     }
#   }

import asyncio
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any
from datetime import datetime, timezone

import websockets
import websockets.exceptions
from websockets.asyncio.client import ClientConnection

from crypto_alerts.config.settings import get_settings
from crypto_alerts.feed.base import AbstractFeed
from crypto_alerts.models.price_tick import PriceTick

logger = logging.getLogger(__name__)


class BinanceFeed(AbstractFeed):
    """
    Connects to the Binance combined stream endpoint and emits PriceTick
    objects for every trade event received.

    Reconnection strategy:
        On any connection error, the feed waits for a configurable delay
        then reconnects. It tracks the number of consecutive failures and
        stops retrying after a configurable maximum. A successful connection
        resets the failure counter back to zero.

    Back-pressure:
        If the evaluator queue is full, await self._queue.put() will block
        here. This naturally slows ingestion rather than dropping ticks or
        growing memory unboundedly.
    """

    def __init__(self, outbound_queue: asyncio.Queue[PriceTick]) -> None:
        super().__init__(outbound_queue)
        self._settings = get_settings()
        self._symbols: set[str] = set()
        self._consecutive_failures: int = 0

    async def subscribe(self, symbols: set[str]) -> None:
        """
        Sets the symbols this feed will stream.
        Must be called before run() starts.
        Symbols are stored in lowercase because Binance stream names are
        lowercase even though trade message symbols are uppercase.
        """
        self._symbols = {s.lower() for s in symbols}
        logger.info("Subscribed to symbols -- %s", self._symbols)

    async def run(self) -> None:
        """
        Main loop with reconnection logic.
        Runs until cancelled or max reconnect attempts are exhausted.
        """
        logger.info("BinanceFeed starting")

        while True:
            try:
                await self._connect_and_stream()
                # If _connect_and_stream returns cleanly (no exception),
                # the connection closed gracefully -- reconnect immediately
                logger.warning("WebSocket closed cleanly, reconnecting")
                self._consecutive_failures = 0

            except asyncio.CancelledError:
                # Shutdown signal -- exit the loop cleanly
                logger.info("BinanceFeed received cancellation, shutting down")
                raise

            except Exception as exc:
                self._consecutive_failures += 1
                logger.error(
                    "Feed connection error (attempt %d/%d) -- %s: %s",
                    self._consecutive_failures,
                    self._settings.feed_max_reconnect_attempts,
                    type(exc).__name__,
                    exc,
                )

                if (
                    self._consecutive_failures
                    >= self._settings.feed_max_reconnect_attempts
                ):
                    logger.critical(
                        "Max reconnect attempts reached, feed is stopping"
                    )
                    raise

                logger.info(
                    "Waiting %.1fs before reconnect",
                    self._settings.feed_reconnect_delay_seconds,
                )
                await asyncio.sleep(self._settings.feed_reconnect_delay_seconds)

    async def _connect_and_stream(self) -> None:
        """
        Opens a single WebSocket connection and streams messages until
        the connection drops or is closed.

        Separated from run() so reconnection logic stays clean and this
        method has a single responsibility: handle one connection lifetime.
        """
        if not self._symbols:
            raise ValueError(
                "No symbols subscribed. Call subscribe() before run()."
            )

        url = self._build_stream_url()
        logger.info("Connecting to Binance WebSocket -- %s", url)

        async with websockets.connect(url) as websocket:  
            logger.info("Connected to Binance WebSocket successfully")
            self._consecutive_failures = 0
            await self._stream_messages(websocket)

    async def _stream_messages(self, websocket: ClientConnection) -> None:
        """
        Reads messages from an open WebSocket connection in a tight loop.
        Each message is parsed and validated before being queued.
        """
        async for raw_message in websocket:
            if isinstance(raw_message, bytes):
                raw_message = raw_message.decode("utf-8")

            tick = self._parse_message(raw_message)

            if tick is not None:
                await self._queue.put(tick)

    def _parse_message(self, raw: str) -> PriceTick | None:
        """
        Parses a raw Binance JSON string into a PriceTick.

        Returns None on any parse or validation failure rather than
        raising -- a single malformed message should never crash the feed.
        All failures are logged so they can be monitored and investigated.
        """
        try:
            payload: dict[str, Any] = json.loads(raw)
            data: dict[str, Any] = payload.get("data", payload)

            symbol: str = data["s"]
            price = Decimal(str(data["p"]))
            volume = Decimal(str(data["q"]))
            timestamp_seconds = int(data["T"]) / 1000.0

            # 2. Convert the raw seconds into a timezone-aware UTC datetime object
            dt_timestamp = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)

            return PriceTick(
                symbol=symbol,
                price=price,
                volume=volume,
                timestamp=dt_timestamp,  # Pass the datetime object here instead of the int
            )

        except (KeyError, InvalidOperation, ValueError) as exc:
            logger.warning(
                "Failed to parse Binance message -- %s: %s | raw=%s",
                type(exc).__name__,
                exc,
                raw[:200],  # Truncate to avoid flooding logs with huge payloads
            )
            return None

        except json.JSONDecodeError as exc:
            logger.warning(
                "Received non-JSON message from Binance -- %s | raw=%s",
                exc,
                raw[:200],
            )
            return None

    def _build_stream_url(self) -> str:
        """
        Constructs the Binance combined stream URL from subscribed symbols.

        Example output:
        wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade
        """
        streams = "/".join(f"{symbol}@trade" for symbol in sorted(self._symbols))
        return f"{self._settings.binance_ws_base_url}/stream?streams={streams}"