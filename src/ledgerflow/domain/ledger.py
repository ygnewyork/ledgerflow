"""The ledger core: accounts, entries, and balanced transactions.

Nothing here touches a database, a broker, or a web framework. That is what
makes the accounting rules testable in milliseconds and readable by a human who
does not know this codebase.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from types import MappingProxyType

from .money import CurrencyMismatch, Money


class LedgerError(Exception):
    pass


class UnbalancedTransaction(LedgerError):
    """Sum of debits != sum of credits, for some currency."""


class InvalidEntry(LedgerError):
    pass


class Direction(str, Enum):
    DEBIT = "debit"
    CREDIT = "credit"

    @property
    def opposite(self) -> Direction:
        return Direction.CREDIT if self is Direction.DEBIT else Direction.DEBIT


class AccountType(str, Enum):
    ASSET = "asset"
    LIABILITY = "liability"
    EQUITY = "equity"
    REVENUE = "revenue"
    EXPENSE = "expense"

    @property
    def normal_balance(self) -> Direction:
        # Assets and expenses increase on the debit side; everything else
        # increases on the credit side. This single mapping removes every
        # "wait, is a credit positive here?" bug downstream.
        if self in (AccountType.ASSET, AccountType.EXPENSE):
            return Direction.DEBIT
        return Direction.CREDIT


@dataclass(frozen=True, slots=True)
class Account:
    id: str
    name: str
    type: AccountType
    currency: str
    # None means no floor: expense and revenue accounts are unbounded, and
    # only accounts with a floor need to take a lock on the write path.
    minimum_balance: Money | None = None

    @property
    def normal_balance(self) -> Direction:
        return self.type.normal_balance

    def signed(self, entry: Entry) -> Money:
        """This entry's effect on the account's balance.

        Positive when the entry moves the account in its natural direction.
        """
        if entry.account_id != self.id:
            raise InvalidEntry(f"entry targets {entry.account_id}, not {self.id}")
        if entry.amount.currency != self.currency:
            raise CurrencyMismatch(
                f"{entry.amount.currency} entry on a {self.currency} account"
            )
        return entry.amount if entry.direction is self.normal_balance else -entry.amount


@dataclass(frozen=True, slots=True)
class Entry:
    """One side of a posting. Immutable, and always a positive amount.

    The sign lives in ``direction``, never in the amount. Signed amounts invite
    double-negation bugs and make ``SUM()`` ambiguous about which side of the
    ledger you are looking at.
    """

    account_id: str
    direction: Direction
    amount: Money

    def __post_init__(self) -> None:
        if not self.amount.is_positive:
            raise InvalidEntry(
                f"entry amount must be positive, got {self.amount}; "
                "use the direction to express which way money moved"
            )

    def reversed(self) -> Entry:
        return Entry(self.account_id, self.direction.opposite, self.amount)


@dataclass(frozen=True, slots=True)
class JournalTransaction:
    """A set of entries that must balance, per currency.

    The invariant is checked here *and* by a deferred constraint trigger in
    Postgres. Not redundancy for its own sake: this check produces a good error
    message at the API edge, while the database check guarantees the invariant
    holds even for a migration script or a hand-written psql session.
    """

    id: str
    kind: str
    effective_at: datetime
    entries: tuple[Entry, ...]
    metadata: Mapping[str, str] = field(default_factory=dict)
    reverses_id: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "entries", tuple(self.entries))
        object.__setattr__(self, "metadata", MappingProxyType(dict(self.metadata)))

        if len(self.entries) < 2:
            raise UnbalancedTransaction(
                f"{self.id}: a transaction needs at least two entries, "
                f"got {len(self.entries)}"
            )
        if self.effective_at.tzinfo is None:
            raise LedgerError(
                f"{self.id}: effective_at must be timezone-aware; "
                "a naive timestamp is a bug waiting for a DST boundary"
            )

        # Per currency, not across. A transaction with USD and EUR legs must
        # balance within each; a single sum across currencies is meaningless.
        drift: dict[str, int] = defaultdict(int)
        for entry in self.entries:
            sign = 1 if entry.direction is Direction.DEBIT else -1
            drift[entry.amount.currency] += sign * entry.amount.minor

        for currency, delta in drift.items():
            if delta != 0:
                raise UnbalancedTransaction(
                    f"{self.id}: {currency} is off by {delta} minor units "
                    f"(debits - credits)"
                )

    # -- derived views -----------------------------------------------------

    @property
    def account_ids(self) -> frozenset[str]:
        return frozenset(e.account_id for e in self.entries)

    def total(self, direction: Direction = Direction.DEBIT) -> dict[str, Money]:
        """Total moved, by currency. Debits and credits agree by construction."""
        totals: dict[str, Money] = {}
        for entry in self.entries:
            if entry.direction is direction:
                current = totals.get(entry.amount.currency)
                totals[entry.amount.currency] = (
                    entry.amount if current is None else current + entry.amount
                )
        return totals

    def reverse(self, new_id: str, effective_at: datetime | None = None) -> JournalTransaction:
        """The correcting transaction.

        There is no undo. The original stays in the ledger forever; this adds a
        mirrored posting so the net balance returns to where it was and the
        history records both that a mistake happened and when it was corrected.
        """
        if self.reverses_id is not None:
            raise LedgerError(
                f"{self.id} is itself a reversal; reversing a reversal is "
                "almost always a bug -- post a fresh transaction instead"
            )
        return JournalTransaction(
            id=new_id,
            kind=f"{self.kind}.reversal",
            effective_at=effective_at or self.effective_at,
            entries=tuple(e.reversed() for e in self.entries),
            metadata=dict(self.metadata),
            reverses_id=self.id,
        )


def balance_of(account: Account, entries: Iterable[Entry]) -> Money:
    """Fold entries into a balance. The reference implementation.

    Production reads use snapshot + delta (see docs/02-schema.sql), but that is
    a cache: this function is the definition it must agree with, and the
    reconciliation job asserts exactly that.
    """
    total = Money.zero(account.currency)
    for entry in entries:
        if entry.account_id != account.id:
            continue
        total = total + account.signed(entry)
    return total
