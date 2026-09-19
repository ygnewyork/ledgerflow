"""Request bodies.

Amounts are integers in the currency's minor unit. A float is rejected rather
than rounded -- it is the first thing a developer hits, and the first thing it
teaches them about the system.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a typo'd field is an error, not a silent no-op


class AmountMixin(_Base):
    amount: int = Field(..., description="integer minor units: 8437 = $84.37")
    currency: str = Field("usd", min_length=3, max_length=3)

    @field_validator("amount", mode="before")
    @classmethod
    def reject_floats(cls, v: Any) -> Any:
        if isinstance(v, float) or (isinstance(v, str) and "." in v):
            raise ValueError(
                "amount must be an integer in the currency's minor unit "
                "(8437 = $84.37), not a decimal"
            )
        return v

    @field_validator("amount")
    @classmethod
    def positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("amount must be positive")
        return v


class CreateAccount(_Base):
    name: str
    type: Literal["asset", "liability", "equity", "revenue", "expense"]
    currency: str = "usd"
    external_id: str | None = None
    minimum_balance: int | None = Field(
        None, description="floor in minor units; null means no floor and no lock on writes"
    )
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateTransaction(AmountMixin):
    kind: str = "card_purchase"
    accounts: dict[str, str] = Field(
        ..., description="role -> account id or external id, e.g. {'expense': ..., 'funding': ...}"
    )
    effective_at: datetime | None = Field(
        None, description="business time; defaults to now. may be backdated."
    )
    descriptor: str | None = Field(
        None, description="raw merchant string, kept verbatim for the normalizer"
    )
    metadata: dict[str, str] = Field(default_factory=dict)


class CreateTransfer(AmountMixin):
    source: str
    destination: str
    effective_at: datetime | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class CreateReversal(_Base):
    effective_at: datetime | None = None


class CreateWebhookEndpoint(_Base):
    url: str
    enabled_events: list[str] = Field(default_factory=lambda: ["*"])


class CreateReplay(_Base):
    consumer_group: str
    topic: str
    from_offset: int = 0
    reason: str
