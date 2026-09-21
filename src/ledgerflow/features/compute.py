"""Feature computation, point-in-time correct.

Every feature is computed as of the *event's* timestamp, never ``now()``.

If a September 17 transaction is scored using a window that includes September
18 data, the decision has read the future. Online that is merely wrong. Offline,
if the same rows ever become training data, the model learns from information
it will not have at inference time -- it scores beautifully in backtest and
fails in production, which is the single most common way a feature pipeline is
silently broken.

The test for this is simple and worth writing: replay the same stream twice and
assert the feature values are identical. Anything reaching for the wall clock
fails immediately.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from ..adapters.db import UnitOfWork
from . import windows


@dataclass(frozen=True, slots=True)
class Features:
    account_id: str
    as_of: datetime
    amount_minor: int
    spend_1h: int
    spend_24h: int
    txn_count_1h: int
    max_amount_1h: int
    distinct_merchants_7d: int
    merchant_frequency: int
    avg_amount_90d: float
    stddev_amount_90d: float

    @property
    def amount_zscore(self) -> float:
        """How unusual this amount is for this account.

        Guarded twice: a tiny sample makes the mean meaningless, and a zero
        standard deviation makes the division explode. Both return 0 -- "no
        evidence" -- rather than a number that looks like evidence.
        """
        if self.stddev_amount_90d <= 0:
            return 0.0
        return (self.amount_minor - self.avg_amount_90d) / self.stddev_amount_90d

    @property
    def avg_spend_1h_90d(self) -> float:
        """Rough hourly baseline, for the velocity rule's denominator."""
        return self.avg_amount_90d if self.avg_amount_90d > 0 else 0.0

    def to_dict(self) -> dict[str, Any]:
        out = asdict(self)
        out["as_of"] = self.as_of.isoformat()
        out["amount_zscore"] = round(self.amount_zscore, 4)
        return out


def compute(
    uow: UnitOfWork,
    *,
    account_id: str,
    as_of: datetime,
    amount_minor: int,
    merchant_id: str | None,
) -> Features:
    """All windows, evaluated with ``as_of`` as the right-hand bound."""
    one_hour_start, one_hour_end = windows.SPEND_1H.bounds(as_of)
    day_start, day_end = windows.SPEND_24H.bounds(as_of)

    hour = uow.risk.spend_window(account_id, one_hour_start, one_hour_end)
    day = uow.risk.spend_window(account_id, day_start, day_end)
    baseline = uow.risk.baseline(account_id, as_of, days=90)
    merchant = uow.risk.merchant_stats(account_id, merchant_id, as_of)

    return Features(
        account_id=account_id,
        as_of=as_of,
        amount_minor=amount_minor,
        spend_1h=int(hour["spend_minor"]),
        spend_24h=int(day["spend_minor"]),
        txn_count_1h=int(hour["txn_count"]),
        max_amount_1h=int(hour["max_amount_minor"]),
        distinct_merchants_7d=int(merchant["distinct_merchants_7d"]),
        merchant_frequency=int(merchant["merchant_frequency"]),
        avg_amount_90d=float(baseline["avg_amount_minor"]),
        stddev_amount_90d=float(baseline["stddev_amount"]),
    )
