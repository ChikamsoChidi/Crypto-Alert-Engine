# src/crypto_alerts/models/alert_rule.py
#
# AlertRule defines what a user wants to be notified about.
# The engine holds a collection of these and tests each incoming
# PriceTick against all active rules.

import uuid
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, Field


class ConditionOperator(StrEnum):
    """
    The comparison operator for a rule condition.

    Using StrEnum means the value serializes as a plain string
    ("ABOVE") rather than an integer enum, which makes logs and
    API responses human-readable without extra serialization logic.
    """

    ABOVE = "ABOVE"
    BELOW = "BELOW"


class AlertRule(BaseModel):
    """
    A user-defined rule that triggers an alert when its condition is met.

    Each rule is scoped to a single symbol and compares the live price
    against a user-supplied threshold using the given operator.

    Example: "Alert me when BTCUSDT goes ABOVE 70000.00"
        AlertRule(
            symbol="BTCUSDT",
            operator=ConditionOperator.ABOVE,
            threshold=Decimal("70000.00"),
            label="BTC all-time high watch"
        )
    """

    model_config = {"frozen": True}

    rule_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for this rule, auto-generated if not provided",
    )
    symbol: str = Field(
        ...,
        min_length=1,
        max_length=20,
        description="The trading pair this rule watches, e.g. BTCUSDT",
    )
    operator: ConditionOperator = Field(
        ...,
        description="The comparison operator: ABOVE or BELOW",
    )
    threshold: Decimal = Field(
        ...,
        gt=Decimal("0"),
        description="The price level that triggers this rule",
    )
    label: str = Field(
        default="",
        max_length=200,
        description="Optional human-readable description of this rule",
    )

    def evaluate(self, tick: "PriceTick") -> bool:
        """
        Returns True if the given tick satisfies this rule's condition.

        Keeping evaluation logic on the model itself follows the
        principle that behavior belongs with the data it operates on.
        The engine calls this method -- it does not reimplement the
        comparison logic itself.
        """
        if self.operator == ConditionOperator.ABOVE:
            return tick.price > self.threshold
        return tick.price < self.threshold


# Resolve the forward reference used in evaluate()
from crypto_alerts.models.price_tick import PriceTick  # noqa: E402

AlertRule.model_rebuild()