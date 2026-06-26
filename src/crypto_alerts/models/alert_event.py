# src/crypto_alerts/models/alert_event.py
#
# AlertEvent is produced by the engine when a rule fires.
# It carries both the rule that triggered and the tick that caused it,
# giving the dispatcher everything it needs to format a notification.

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, Field

from crypto_alerts.models.alert_rule import AlertRule
from crypto_alerts.models.price_tick import PriceTick


class AlertEvent(BaseModel):
    """
    A record of a rule condition being satisfied by a live price tick.

    AlertEvent is intentionally append-only -- once created it is never
    modified. It represents a historical fact: at this moment, this rule
    fired because of this tick. This makes it safe to pass to multiple
    dispatchers concurrently without any locking.
    """

    model_config = {"frozen": True}

    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for this alert event",
    )
    fired_at: datetime = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc),
        description="UTC timestamp of when this alert was generated",
    )
    rule: AlertRule = Field(
        ...,
        description="The rule whose condition was satisfied",
    )
    tick: PriceTick = Field(
        ...,
        description="The price tick that triggered the rule",
    )

    def summary(self) -> str:
        """
        Returns a human-readable one-line summary of this alert event.
        Used by the console dispatcher and for logging.
        """
        label_part = f" ({self.rule.label})" if self.rule.label else ""
        return (
            f"[ALERT]{label_part} {self.rule.symbol} is {self.rule.operator} "
            f"threshold {self.rule.threshold} -- "
            f"current price: {self.tick.price} "
            f"at {self.fired_at.strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )