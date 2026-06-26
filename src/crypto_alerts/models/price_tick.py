# src/crypto_alerts/models/price_tick.py
#
# PriceTick is the canonical representation of a single price update.
# It is created by the feed layer and consumed by the engine layer.
# Nothing outside the feed layer ever touches raw Binance JSON -- all
# downstream code works with this model exclusively.

from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class PriceTick(BaseModel):
    """
    A single price update for a trading symbol.

    We use Decimal for price and volume instead of float because float
    arithmetic is imprecise for financial data. For example:
        0.1 + 0.2 == 0.30000000000000004  # float
        Decimal("0.1") + Decimal("0.2") == Decimal("0.3")  # correct

    The model is frozen (immutable) because a price tick is a historical
    fact -- it should never be modified after creation.
    """

    model_config = {"frozen": True}

    symbol: str = Field(
        ...,
        min_length=1,
        max_length=20,
        description="Trading pair symbol, e.g. BTCUSDT",
    )
    price: Decimal = Field(
        ...,
        gt=Decimal("0"),
        description="Current trade price, must be positive",
    )
    volume: Decimal = Field(
        ...,
        ge=Decimal("0"),
        description="Trade volume at this price tick",
    )
    timestamp: datetime = Field(
        ...,
        description="UTC timestamp of when this tick was received",
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def normalize_symbol(cls, v: object) -> str:
        # Binance symbols are always uppercase but we enforce it here
        # so the engine can do direct string comparisons safely
        if isinstance(v, str):
            return v.upper().strip()
        raise ValueError(f"symbol must be a string, got {type(v)}")

    @field_validator("timestamp", mode="before")
    @classmethod
    def ensure_utc(cls, v: object) -> datetime:
        # Binance sends timestamps as millisecond epoch integers.
        # We normalize everything to timezone-aware UTC datetimes here
        # so the rest of the application never has to think about it.
        if isinstance(v, int):
            return datetime.fromtimestamp(v / 1000, tz=timezone.utc)
        if isinstance(v, datetime):
            return v if v.tzinfo is not None else v.replace(tzinfo=timezone.utc)
        raise ValueError(f"timestamp must be an int or datetime, got {type(v)}")