"""Synthetic transaction generator.

Produces data with the shape real data has -- recurring subscriptions, a daily
coffee habit, weekly groceries, a biweekly paycheck, occasional large purchases
-- because uniformly random amounts make every fraud rule either fire constantly
or never, and make the dashboard look like static.

It also injects two deliberate anomalies so the risk rules have something true
to find: a card-testing burst (many tiny charges in minutes) and a single
outlier purchase far outside the account's own distribution.
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone
from typing import Iterator

from . import ids
from .adapters.db import unit_of_work
from .application.services import TenantContext, post_transaction
from .domain.money import Money

# (descriptor template, expense account, min cents, max cents, days between)
HABITS = [
    ("SQ *TST* STARBUCKS 800-782-7282 CA", "food", 450, 850, 1),
    ("CHIPOTLE {n} AUSTIN TX", "food", 1100, 1800, 4),
    ("WHOLEFDS MKT #{n} AUSTIN TX", "groceries", 4200, 14500, 7),
    ("H-E-B #{n} AUSTIN TX", "groceries", 3100, 11200, 5),
    ("AMZN Mktp US*{code}", "shopping", 1200, 8900, 3),
    ("UBER   *TRIP HELP.UBER.COM CA", "transport", 900, 3400, 6),
    ("SHELL OIL {n} HOUSTON TX", "transport", 3500, 7200, 9),
    ("TARGET        T-{n}", "shopping", 2200, 12000, 11),
    ("CVS/PHARMACY #{n}", "health", 800, 4500, 14),
]

SUBSCRIPTIONS = [
    ("NETFLIX.COM 866-579-7172 CA", "entertainment", 1599, 30),
    ("SPOTIFY USA 8887771111 NY", "entertainment", 1199, 30),
    ("AT&T *PAYMENT 800-288-2020 TX", "bills", 8500, 30),
    ("COMCAST CABLE COMM 800-COMCAST", "bills", 7999, 30),
]

# Descriptors with no dictionary entry. Real ingest always has a tail the
# normalizer cannot resolve, and a demo where everything resolves is lying.
UNKNOWN = [
    "SQ *BLUE BOTTLE 4411 OAKLAND CA",
    "TST* PINTHOUSE PIZZA AUSTIN",
    "PY *LOCAL FARMERS MKT",
    "ZELLE TO J SMITH 20260817",
]


def _descriptor(template: str, rng: random.Random) -> str:
    return template.format(
        n=rng.randint(1000, 99999),
        code="".join(rng.choices("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", k=9)),
    )


def _events(start: datetime, days: int, rng: random.Random) -> Iterator[tuple[datetime, str, str, int]]:
    """Yield (when, descriptor, expense account, amount) in time order."""
    out: list[tuple[datetime, str, str, int]] = []

    for template, account, low, high, cadence in HABITS:
        when = start
        while when < start + timedelta(days=days):
            # jitter the cadence and the hour: nobody buys coffee at exactly
            # 09:00 every 24 hours
            when += timedelta(days=max(1, rng.gauss(cadence, cadence * 0.3)))
            if when >= start + timedelta(days=days):
                break
            moment = when.replace(
                hour=rng.randint(7, 21), minute=rng.randint(0, 59), second=rng.randint(0, 59)
            )
            out.append((moment, _descriptor(template, rng), account, rng.randint(low, high)))

    for template, account, amount, cadence in SUBSCRIPTIONS:
        when = start + timedelta(days=rng.randint(0, 28))
        while when < start + timedelta(days=days):
            out.append((when.replace(hour=3, minute=rng.randint(0, 59)), template, account, amount))
            when += timedelta(days=cadence)

    for _ in range(max(1, days // 20)):
        when = start + timedelta(days=rng.uniform(0, days))
        out.append((when, rng.choice(UNKNOWN), "general", rng.randint(900, 6500)))

    # --- anomaly 1: card testing. many tiny charges inside a few minutes.
    burst_at = start + timedelta(days=days * 0.8)
    for i in range(14):
        out.append((
            burst_at + timedelta(seconds=i * 40),
            f"WL *DIGITALGOODS {rng.randint(100, 999)}",
            "general",
            rng.randint(95, 320),
        ))

    # --- anomaly 2: one purchase far outside this account's distribution
    out.append((
        start + timedelta(days=days * 0.6),
        "MARRIOTT HOTELS 8882367687 MD",
        "travel",
        rng.randint(180_000, 320_000),
    ))

    out.sort(key=lambda row: row[0])
    yield from out


def generate(
    *, tenant_id: str, count: int = 1000, days: int = 90, seed: int = 17, mode: str = "test"
) -> dict[str, int]:
    """Write synthetic history straight through the service layer.

    Deliberately not through HTTP: this is about filling the ledger, not about
    measuring the API. The load test that measures the API lives in
    ``tests/test_load.py`` and goes over the wire.
    """
    rng = random.Random(seed)
    ctx = TenantContext(tenant_id=tenant_id, api_key_id="loadgen", mode=mode)
    start = datetime.now(timezone.utc) - timedelta(days=days)

    written = 0
    paychecks = 0
    started = time.perf_counter()

    with unit_of_work() as uow:
        # fund the account first, in the same history: a ledger whose balance
        # floor is enforced will reject purchases from an empty account, and
        # that rejection is correct behaviour, not a generator bug
        when = start
        while when < datetime.now(timezone.utc):
            post_transaction(
                uow, ctx, kind="deposit",
                accounts={"destination": "checking", "income": "income"},
                amount=Money(rng.randint(240_000, 320_000), "usd"),
                effective_at=when,
                metadata={"source": "loadgen"},
            )
            paychecks += 1
            when += timedelta(days=14)

        for moment, descriptor, account, amount in _events(start, days, rng):
            if written >= count:
                break
            post_transaction(
                uow, ctx, kind="card_purchase",
                accounts={"expense": account, "funding": "checking"},
                amount=Money(amount, "usd"),
                effective_at=moment,
                descriptor=descriptor,
                metadata={"source": "loadgen"},
            )
            written += 1

    elapsed = time.perf_counter() - started
    if written < count:
        # say so rather than silently under-delivering: the habit cadences,
        # not the cap, decide how many purchases 90 days contains
        print(
            f"note: {days} days of these habits yields {written} purchases; "
            f"--count {count} was not reached. raise --days for more history."
        )
    result = {
        "transactions": written + paychecks,
        "paychecks": paychecks,
        "purchases": written,
        "seconds": round(elapsed, 2),
        "per_second": round((written + paychecks) / elapsed) if elapsed else 0,
    }
    print(result)
    return result
