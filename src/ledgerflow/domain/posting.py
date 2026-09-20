"""Posting rules: the map from a business event to ledger entries.

``card_purchase`` is not a ledger transaction. Something has to decide which
accounts get debited and credited, and that decision is the business logic. It
lives here, in one registry, instead of spreading through a growing
``if kind == ...`` chain in a request handler.

Three things fall out of the registry that do not fall out of a conditional:
adding an event type is a new class rather than an edit to shared code; each
rule is unit-testable in isolation against the balance invariant; and the set
of legal postings is *enumerable* -- ``describe_rules()`` generates the posting
table in the docs from the code, so they cannot drift apart.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping

from .ledger import Direction, Entry, JournalTransaction, LedgerError
from .money import Money


class UnknownPostingKind(LedgerError):
    pass


@dataclass(frozen=True, slots=True)
class PostingRequest:
    """Everything a rule may look at. Accounts are already resolved to ids."""

    amount: Money
    effective_at: datetime
    # role -> account_id, e.g. {"funding": "acct_1", "expense": "acct_2"}
    accounts: Mapping[str, str]
    metadata: Mapping[str, str] = field(default_factory=dict)

    def account(self, role: str) -> str:
        try:
            return self.accounts[role]
        except KeyError:
            raise LedgerError(
                f"posting requires a {role!r} account; got {sorted(self.accounts)}"
            ) from None


class PostingRule(ABC):
    kind: str
    description: str = ""
    #: (debit role, credit role) -- documentation, and asserted by the tests
    shape: tuple[str, str] = ("", "")

    @abstractmethod
    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        ...


_REGISTRY: dict[str, PostingRule] = {}


def posting_rule(cls: type[PostingRule]) -> type[PostingRule]:
    """Class decorator; registers an instance under ``cls.kind``."""
    if cls.kind in _REGISTRY:
        raise LedgerError(f"duplicate posting rule for {cls.kind!r}")
    _REGISTRY[cls.kind] = cls()
    return cls


def build_transaction(
    txn_id: str,
    kind: str,
    request: PostingRequest,
) -> JournalTransaction:
    """Turn an event into a balanced transaction, or raise trying.

    The ``JournalTransaction`` constructor re-checks the balance, so a buggy
    rule fails here rather than reaching the database.
    """
    try:
        rule = _REGISTRY[kind]
    except KeyError:
        raise UnknownPostingKind(
            f"no posting rule for {kind!r}; known kinds: {sorted(_REGISTRY)}"
        ) from None

    return JournalTransaction(
        id=txn_id,
        kind=kind,
        effective_at=request.effective_at,
        entries=rule.build(request),
        metadata=request.metadata,
    )


def describe_rules() -> list[tuple[str, str, str, str]]:
    """(kind, debit role, credit role, description) -- for generated docs."""
    return sorted(
        (r.kind, r.shape[0], r.shape[1], r.description) for r in _REGISTRY.values()
    )


# ---------------------------------------------------------------------------
# The rules
# ---------------------------------------------------------------------------


def _simple(debit: str, credit: str, request: PostingRequest) -> tuple[Entry, ...]:
    """Two-legged posting. Balanced by construction."""
    return (
        Entry(request.account(debit), Direction.DEBIT, request.amount),
        Entry(request.account(credit), Direction.CREDIT, request.amount),
    )


@posting_rule
class CardPurchase(PostingRule):
    kind = "card_purchase"
    description = "Money leaves a funding account and becomes an expense."
    shape = ("expense", "funding")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        return _simple("expense", "funding", request)


@posting_rule
class Deposit(PostingRule):
    kind = "deposit"
    description = "Income arrives in an asset account."
    shape = ("destination", "income")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        return _simple("destination", "income", request)


@posting_rule
class OpeningBalance(PostingRule):
    kind = "opening_balance"
    description = "What an account was worth when the books were opened."
    shape = ("destination", "equity")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        return _simple("destination", "equity", request)


@posting_rule
class Transfer(PostingRule):
    kind = "transfer"
    description = "Money moves between two accounts under the same tenant."
    shape = ("destination", "source")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        if request.account("source") == request.account("destination"):
            raise LedgerError("a transfer to the same account is a no-op, not a posting")
        return _simple("destination", "source", request)


@posting_rule
class Fee(PostingRule):
    kind = "fee"
    description = "A charge we levy; expense to the customer, revenue to us."
    shape = ("fee_expense", "funding")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        return _simple("fee_expense", "funding", request)


@posting_rule
class Refund(PostingRule):
    kind = "refund"
    description = "A purchase comes back: the mirror of card_purchase."
    shape = ("funding", "expense")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        return _simple("funding", "expense", request)


@posting_rule
class SplitPurchase(PostingRule):
    kind = "split_purchase"
    description = (
        "One charge across several expense categories -- the case that proves "
        "a transaction is N entries, not two."
    )
    shape = ("expense_*", "funding")

    def build(self, request: PostingRequest) -> tuple[Entry, ...]:
        # roles like expense_groceries, expense_household
        legs = sorted(r for r in request.accounts if r.startswith("expense_"))
        if not legs:
            raise LedgerError("split_purchase needs at least one expense_* account")

        raw = request.metadata.get("split")  # "expense_groceries:6000,expense_household:2437"
        if not raw:
            raise LedgerError("split_purchase requires a 'split' metadata entry")

        entries: list[Entry] = []
        allocated = 0
        for part in raw.split(","):
            role, _, minor = part.partition(":")
            amount = Money(int(minor), request.amount.currency)
            entries.append(Entry(request.account(role.strip()), Direction.DEBIT, amount))
            allocated += amount.minor

        # Catch the split/total mismatch here, with a message naming both
        # numbers, rather than letting the balance check report an opaque drift.
        if allocated != request.amount.minor:
            raise LedgerError(
                f"split allocates {allocated} minor units but the charge is "
                f"{request.amount.minor}"
            )

        entries.append(
            Entry(request.account("funding"), Direction.CREDIT, request.amount)
        )
        return tuple(entries)
