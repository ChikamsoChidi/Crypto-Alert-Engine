# src/crypto_alerts/feed/__init__.py

from crypto_alerts.feed.base import AbstractFeed
from crypto_alerts.feed.binance_feed import BinanceFeed

__all__ = ["AbstractFeed", "BinanceFeed"]