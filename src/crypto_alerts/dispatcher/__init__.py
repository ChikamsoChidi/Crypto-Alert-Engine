# src/crypto_alerts/dispatcher/__init__.py

from crypto_alerts.dispatcher.base import AbstractDispatcher
from crypto_alerts.dispatcher.console_dispatcher import ConsoleDispatcher
from crypto_alerts.dispatcher.webhook_dispatcher import WebhookDispatcher

__all__ = ["AbstractDispatcher", "ConsoleDispatcher", "WebhookDispatcher"]