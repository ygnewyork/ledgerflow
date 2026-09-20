"""Synthetic history for one person's finances.

Uniformly random amounts make every fraud rule either fire constantly or never,
and make a dashboard look like static. Real money has structure: a paycheck
every two weeks, rent on the 1st, coffee most mornings, groceries most weeks,
a card you run up and pay off, and a slow drift into savings and investments.
This generates that structure.

The money model, which is the point:

    Revenue:Income ──> Assets:Checking ──┬──> Expenses:*        (debit card)
                                         ├──> Assets:Savings    (transfer)
                                         ├──> Assets:Investments(transfer)
                                         └──> Liabilities:Card  (card payoff)

    Liabilities:Card ──> Expenses:*                             (credit card)

Most day-to-day spending goes on the card, which makes the card balance GROW --
a credit on a liability increases it. Once a month that balance is paid down
from checking. Watching those two accounts move against each other on the
dashboard is the clearest illustration of why liabilities and assets have
opposite normal balances.

Two anomalies are planted on purpose so the fraud rules have something true to
find: a card-testing burst, and one purchase far outside the account's own
distribution.
"""

from __future__ import annotations

import random
import time
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterator

from . import ids
from .adapters.db import unit_of_work
from .application.services import TenantContext, post_transaction
from .domain.money import Money

# ---------------------------------------------------------------------------
# What this person buys
# ---------------------------------------------------------------------------

# (descriptor template, expense account, min cents, max cents, days between)
HABITS = [
    ("SQ *TST* STARBUCKS 800-782-7282 CA",  "food",          450,   850,  1.4),
    ("CHIPOTLE {n} AUSTIN TX",              "food",         1100,  1800,  4),
    ("TST* PINTHOUSE PIZZA AUSTIN",         "food",         1800,  4200,  9),
    ("WHOLEFDS MKT #{n} AUSTIN TX",         "groceries",    4200, 14500,  7),
    ("H-E-B #{n} AUSTIN TX",                "groceries",    3100, 11200,  5),
    ("AMZN Mktp US*{code}",                 "shopping",     1200,  8900,  3),
    ("TARGET        T-{n}",                 "shopping",     2200, 12000, 11),
    ("UBER   *TRIP HELP.UBER.COM CA",       "transport",     900,  3400,  6),
    ("SHELL OIL {n} HOUSTON TX",            "transport",    3500,  7200,  9),
    ("CVS/PHARMACY #{n}",                   "health",        800,  4500, 14),
    ("STEAMGAMES.COM 4259522985 WA",        "entertainment", 999,  5999, 21),
]

# Charged to the card on the same day each month.
SUBSCRIPTIONS = [
    ("NETFLIX.COM 866-579-7172 CA",         "subscriptions", 1599),
    ("SPOTIFY USA 8887771111 NY",           "subscriptions", 1199),
    ("AT&T *PAYMENT 800-288-2020 TX",       "bills",         8500),
    ("COMCAST CABLE COMM 800-COMCAST",      "bills",         7999),
]

# The tail every real ingest has: descriptors the dictionary does not know.
# A demo where everything resolves is lying about the problem.
UNKNOWN = [
    "SQ *BLUE BOTTLE 4411 OAKLAND CA",
    "PY *LOCAL FARMERS MKT",
    "ZELLE TO J SMITH {n}",
    "VENMO *ROOMMATE UTILITIES",
]


@dataclass(frozen=True, slots=True)
class Event:
    """One posting to make, at one instant."""

    at: datetime
    kind: str
    accounts: dict[str, str]
    amount: int
    descriptor: str | None = None
    note: str = ""


def _descriptor(template: str, rng: random.Random) -> str:
    return template.format(
        n=rng.randint(1000, 99999),
        code="".join(rng.choices("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", k=9)),
    )


# ---------------------------------------------------------------------------
# The life
# ---------------------------------------------------------------------------


#: What the accounts were worth on day one. Starting every account at zero is
#: not a neutral choice -- it means the first paycheck has to cover rent, the
#: savings transfer and two weeks of groceries before the second one arrives,
#: which is not how anyone's finances actually work and made the 90-day run
#: overdraw.
OPENING = {
    "checking": 384_000,
    "savings": 612_000,
    "investments": 1_140_000,
    "cash": 8_000,
}


def _timeline(start: datetime, days: int, rng: random.Random) -> list[Event]:
    end = start + timedelta(days=days)
    events: list[Event] = []

    # --- opening balances, booked to equity --------------------------------
    for account, amount in OPENING.items():
        if amount <= 0:
            # An account that opened at zero has no opening entry to record.
            # The domain rejects a zero-amount entry, correctly -- "nothing
            # happened" is the absence of a posting, not a posting of nothing.
            continue
        events.append(Event(
            at=start - timedelta(minutes=5),
            kind="opening_balance",
            accounts={"destination": account, "equity": "opening"},
            amount=amount, note="opening balance",
        ))

    # --- income: a paycheck every two weeks, into checking ----------------
    # Slightly variable, the way hourly or bonus-inclusive pay is.
    pay_day = start
    while pay_day < end:
        events.append(Event(
            at=pay_day.replace(hour=6, minute=rng.randint(0, 40)),
            kind="deposit",
            accounts={"destination": "checking", "income": "income"},
            amount=rng.randint(241_000, 289_000),
            note="biweekly paycheck",
        ))
        pay_day += timedelta(days=14)

    # --- rent: the 1st, the largest recurring outflow ---------------------
    rent = rng.choice([132_500, 139_500, 145_000])
    month = start.replace(day=1)
    while month < end:
        due = month.replace(hour=9, minute=5)
        if due >= start:
            events.append(Event(
                at=due, kind="card_purchase",
                accounts={"expense": "rent", "funding": "checking"},
                amount=rent,
                descriptor="WEST CAMPUS PROPERTIES RENT",
                note="rent",
            ))
        month = (month + timedelta(days=32)).replace(day=1)

    # --- paying yourself first: savings and investments -------------------
    # Two days after the first paycheck of the month, which is when most
    # people's automatic transfers are actually scheduled.
    month = start.replace(day=1)
    while month < end:
        when = month.replace(day=3, hour=7, minute=0)
        if start <= when < end:
            events.append(Event(
                at=when, kind="transfer",
                accounts={"source": "checking", "destination": "savings"},
                amount=rng.randint(35_000, 60_000), note="monthly savings",
            ))
            events.append(Event(
                at=when + timedelta(minutes=3), kind="transfer",
                accounts={"source": "checking", "destination": "investments"},
                amount=rng.randint(25_000, 45_000), note="brokerage contribution",
            ))
        month = (month + timedelta(days=32)).replace(day=1)

    # --- subscriptions, on the card ---------------------------------------
    for template, account, amount in SUBSCRIPTIONS:
        when = start + timedelta(days=rng.randint(0, 27))
        while when < end:
            events.append(Event(
                at=when.replace(hour=3, minute=rng.randint(0, 59)),
                kind="card_purchase",
                accounts={"expense": account, "funding": "card"},
                amount=amount, descriptor=template, note="subscription",
            ))
            when += timedelta(days=30)

    # --- everyday spending -------------------------------------------------
    # Most of it on the card, some on the debit card. Mixing the two is what
    # makes the card payoff below mean anything.
    for template, account, low, high, cadence in HABITS:
        when = start
        while when < end:
            when += timedelta(days=max(0.5, rng.gauss(cadence, cadence * 0.35)))
            if when >= end:
                break
            funding = "card" if rng.random() < 0.72 else "checking"
            events.append(Event(
                at=when.replace(hour=rng.randint(7, 21), minute=rng.randint(0, 59)),
                kind="card_purchase",
                accounts={"expense": account, "funding": funding},
                amount=rng.randint(low, high),
                descriptor=_descriptor(template, rng),
            ))

    # --- the market moves --------------------------------------------------
    # Contributions are only half of an investment account. The other half is
    # what the market does to the money already in there, which is why the
    # balance has to be marked to market rather than left equal to the sum of
    # deposits -- otherwise it is a savings account with extra steps.
    #
    # Booked the accounting-correct way: an unrealized gain debits the asset
    # and credits revenue; a loss debits an expense and credits the asset.
    # Both are ordinary balanced postings. Nothing special is required of the
    # ledger to represent a portfolio.
    contributions = sorted(
        (e.at, e.amount) for e in events
        if e.kind == "transfer" and e.accounts.get("destination") == "investments"
    )

    held = OPENING.get("investments", 0)
    consumed = 0
    month = start.replace(day=1)
    while month < end:
        mark = month.replace(day=27, hour=16, minute=0)
        if mark >= end:
            break
        # everything contributed since the last mark is now invested
        while consumed < len(contributions) and contributions[consumed][0] <= mark:
            held += contributions[consumed][1]
            consumed += 1

        if start <= mark and held > 0:
            # index-like: roughly 8%/yr drift and 15%/yr volatility, monthly
            monthly_return = rng.gauss(0.0065, 0.042)
            change = int(held * monthly_return)
            if change > 0:
                events.append(Event(
                    at=mark, kind="deposit",
                    accounts={"destination": "investments", "income": "gains"},
                    amount=change, note=f"market {monthly_return:+.2%}",
                ))
            elif change < 0:
                events.append(Event(
                    at=mark, kind="card_purchase",
                    accounts={"expense": "losses", "funding": "investments"},
                    amount=-change, note=f"market {monthly_return:+.2%}",
                ))
            held += change

        month = (month + timedelta(days=32)).replace(day=1)

    # --- cash: withdraw from checking, then spend it -----------------------
    # Without this the Cash account exists and never moves, which is worse than
    # not having it: an account that is always zero is noise on every screen.
    when = start + timedelta(days=rng.randint(1, 9))
    while when < end:
        events.append(Event(
            at=when.replace(hour=12, minute=rng.randint(0, 59)),
            kind="transfer",
            accounts={"source": "checking", "destination": "cash"},
            amount=rng.choice([4_000, 6_000, 10_000]),
            note="ATM withdrawal",
        ))
        # a couple of small cash purchases follow, the way they do
        for _ in range(rng.randint(1, 3)):
            spent = when + timedelta(days=rng.uniform(0.2, 9))
            if spent >= end:
                break
            events.append(Event(
                at=spent, kind="card_purchase",
                accounts={"expense": rng.choice(["food", "general", "transport"]),
                          "funding": "cash"},
                amount=rng.randint(400, 2_500),
                descriptor=None, note="cash",
            ))
        when += timedelta(days=rng.randint(16, 28))

    # --- the unresolvable tail ---------------------------------------------
    for _ in range(max(2, days // 12)):
        when = start + timedelta(days=rng.uniform(0, days))
        events.append(Event(
            at=when, kind="card_purchase",
            accounts={"expense": "general", "funding": "checking"},
            amount=rng.randint(900, 6500),
            descriptor=_descriptor(rng.choice(UNKNOWN), rng),
        ))

    # --- interest, and the occasional refund -------------------------------
    month = start.replace(day=1)
    while month < end:
        when = month.replace(day=28, hour=23, minute=30)
        if start <= when < end:
            events.append(Event(
                at=when, kind="deposit",
                accounts={"destination": "savings", "income": "interest"},
                amount=rng.randint(180, 1_400), note="savings interest",
            ))
        month = (month + timedelta(days=32)).replace(day=1)

    for _ in range(max(1, days // 45)):
        when = start + timedelta(days=rng.uniform(5, days))
        events.append(Event(
            at=when, kind="refund",
            accounts={"expense": "shopping", "funding": "card"},
            amount=rng.randint(1_800, 9_500),
            descriptor="AMZN Mktp US*{} REFUND".format(
                "".join(rng.choices("ABCDEFGHJKLMNPQRSTUVWXYZ0123456789", k=9))),
            note="returned an order",
        ))

    # --- anomaly 1: card testing -------------------------------------------
    burst = start + timedelta(days=days * 0.82)
    for i in range(14):
        events.append(Event(
            at=burst + timedelta(seconds=i * 40), kind="card_purchase",
            accounts={"expense": "general", "funding": "card"},
            amount=rng.randint(95, 320),
            descriptor=f"WL *DIGITALGOODS {rng.randint(100, 999)}",
            note="card-testing burst",
        ))

    # --- anomaly 2: one purchase unlike the rest ---------------------------
    events.append(Event(
        at=start + timedelta(days=days * 0.62), kind="card_purchase",
        accounts={"expense": "travel", "funding": "card"},
        amount=rng.randint(180_000, 320_000),
        descriptor="MARRIOTT HOTELS 8882367687 MD",
        note="outlier purchase",
    ))

    events.sort(key=lambda e: e.at)
    return events


def _with_card_payoffs(events: list[Event], start: datetime, days: int,
                       rng: random.Random) -> list[Event]:
    """Pay the card down on the 15th, by what was actually charged to it.

    Computed from the events themselves rather than a fixed number, so the
    payment tracks real spending -- and so a month with a big travel charge
    produces a visibly bigger payment, which is the behaviour you want the
    dashboard to show.
    """
    end = start + timedelta(days=days)
    out = list(events)

    month = start.replace(day=1)
    while month < end:
        due = month.replace(day=15, hour=8, minute=0)
        window_start = due - timedelta(days=30)
        if start <= due < end:
            charged = sum(
                e.amount for e in events
                if window_start <= e.at < due
                and e.accounts.get("funding") == "card"
                and e.kind == "card_purchase"
            )
            refunded = sum(
                e.amount for e in events
                if window_start <= e.at < due
                and e.accounts.get("funding") == "card"
                and e.kind == "refund"
            )
            owed = charged - refunded
            if owed > 0:
                # Most months paid in full; occasionally a partial payment,
                # which is what leaves a carried balance to look at.
                paid = owed if rng.random() < 0.8 else int(owed * rng.uniform(0.45, 0.8))
                out.append(Event(
                    at=due, kind="transfer",
                    accounts={"source": "checking", "destination": "card"},
                    amount=paid, note="credit card payment",
                ))
        month = (month + timedelta(days=32)).replace(day=1)

    out.sort(key=lambda e: e.at)
    return out


def _fit_to_checking(events: list[Event], rng: random.Random) -> tuple[list[Event], dict[str, int]]:
    """Replay the timeline against a running checking balance, and adapt.

    The generator builds each habit independently and then sorts, so nothing
    knows the balance at the moment it spends. Without this pass a lean week
    produces a posting the ledger correctly rejects, and the whole run dies on
    an overdraft that is the generator's fault, not the ledger's.

    Rather than inflate the paycheck until the problem disappears, this does
    what a person does when checking is low:

      * a discretionary transfer (savings, brokerage, ATM) is shrunk to what
        is actually there, and dropped if that is nothing
      * a purchase falls back to the credit card
      * a card payment pays what it can

    Which is also why the card balance grows in exactly the months you would
    expect it to.
    """
    # a cushion, because people do not run their checking account to zero
    BUFFER = 12_000

    balance = 0
    out: list[Event] = []
    adapted = {"transfers_reduced": 0, "transfers_skipped": 0, "moved_to_card": 0}

    for event in events:
        accounts = event.accounts
        into_checking = (
            (event.kind in ("deposit", "opening_balance") and accounts.get("destination") == "checking")
            or (event.kind == "transfer" and accounts.get("destination") == "checking")
            or (event.kind == "refund" and accounts.get("funding") == "checking")
        )
        if into_checking:
            balance += event.amount
            out.append(event)
            continue

        spends_checking = (
            accounts.get("source") == "checking"
            or (event.kind == "card_purchase" and accounts.get("funding") == "checking")
        )
        if not spends_checking:
            out.append(event)
            continue

        available = balance - BUFFER

        if event.amount <= available:
            balance -= event.amount
            out.append(event)
            continue

        if event.kind == "transfer":
            # shrink it to what is there, in round hundreds
            trimmed = max(0, (available // 10_000) * 10_000)
            if trimmed <= 0:
                adapted["transfers_skipped"] += 1
                continue
            balance -= trimmed
            adapted["transfers_reduced"] += 1
            out.append(replace(event, amount=trimmed))
            continue

        # rent cannot move to a credit card; everything else can
        if accounts.get("expense") == "rent":
            balance -= event.amount
            out.append(event)
            continue

        adapted["moved_to_card"] += 1
        out.append(replace(event, accounts={**accounts, "funding": "card"}))

    return out, adapted


def generate(
    *, tenant_id: str, count: int = 1000, days: int = 90, seed: int = 17, mode: str = "test"
) -> dict[str, int]:
    """Write a person's financial history straight through the service layer.

    Deliberately not over HTTP: this fills the ledger, it does not measure the
    API. The load test that measures the API is `python -m ledgerflow.bench`.
    """
    rng = random.Random(seed)
    ctx = TenantContext(tenant_id=tenant_id, api_key_id="loadgen", mode=mode)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    events = _with_card_payoffs(_timeline(start, days, rng), start, days, rng)
    events, adapted = _fit_to_checking(events, rng)
    events = [e for e in events if e.at <= now][:count]

    tally: dict[str, int] = {}
    started = time.perf_counter()

    with unit_of_work() as uow:
        for event in events:
            post_transaction(
                uow, ctx,
                kind=event.kind,
                accounts=event.accounts,
                amount=Money(event.amount, "usd"),
                effective_at=event.at,
                descriptor=event.descriptor,
                metadata={"source": "loadgen", **({"note": event.note} if event.note else {})},
            )
            tally[event.kind] = tally.get(event.kind, 0) + 1

    elapsed = time.perf_counter() - started
    result = {
        "transactions": len(events),
        **tally,
        **{k: v for k, v in adapted.items() if v},
        "seconds": round(elapsed, 2),
        "per_second": round(len(events) / elapsed) if elapsed else 0,
    }
    print(result)
    return result
