"""Domain tests.

Plain unittest so the core can be verified with no dependencies installed:

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import dataclasses
import random
import sys
import unittest
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ledgerflow.domain import (
    Account,
    AccountType,
    CurrencyMismatch,
    Direction,
    Entry,
    JournalTransaction,
    LedgerError,
    Money,
    MoneyError,
    PostingRequest,
    PrecisionError,
    UnbalancedTransaction,
    UnknownPostingKind,
    UnsupportedCurrency,
    balance_of,
    build_transaction,
    describe_rules,
)

NOW = datetime(2026, 9, 17, 16, 21, tzinfo=UTC)


class TestMoney(unittest.TestCase):
    def test_parses_decimal_strings(self):
        self.assertEqual(Money.parse("84.37", "usd"), Money(8437, "usd"))
        self.assertEqual(Money.parse("0.01", "usd"), Money(1, "usd"))
        self.assertEqual(Money.parse(Decimal("1200"), "jpy"), Money(1200, "jpy"))

    def test_rejects_floats_outright(self):
        with self.assertRaises(PrecisionError):
            Money.parse(84.37, "usd")

    def test_rejects_excess_precision_instead_of_rounding(self):
        # the whole point: this is a schema disagreement, not a rounding call
        with self.assertRaises(PrecisionError):
            Money.parse("84.375", "usd")
        with self.assertRaises(PrecisionError):
            Money.parse("100.5", "jpy")  # yen has no minor unit

    def test_rejects_non_int_minor(self):
        with self.assertRaises(MoneyError):
            Money(84.37, "usd")  # type: ignore[arg-type]
        with self.assertRaises(MoneyError):
            Money(True, "usd")  # bool is an int subclass; not one cent

    def test_currency_is_normalized_and_validated(self):
        self.assertEqual(Money(1, "USD").currency, "usd")
        with self.assertRaises(UnsupportedCurrency):
            Money(1, "xyz")

    def test_arithmetic_is_currency_safe(self):
        self.assertEqual(Money(100, "usd") + Money(50, "usd"), Money(150, "usd"))
        with self.assertRaises(CurrencyMismatch):
            Money(100, "usd") + Money(50, "eur")
        with self.assertRaises(CurrencyMismatch):
            _ = Money(100, "usd") < Money(50, "eur")

    def test_no_floating_point_drift(self):
        total = Money.zero("usd")
        for _ in range(1_000):
            total = total + Money.parse("0.10", "usd") + Money.parse("0.20", "usd")
        self.assertEqual(total, Money(30_000, "usd"))  # exactly $300.00
        self.assertEqual(total.to_decimal(), Decimal("300.00"))


class TestTransactionInvariant(unittest.TestCase):
    def _txn(self, entries, **kw):
        return JournalTransaction(
            id=kw.pop("id", "txn_test"),
            kind=kw.pop("kind", "transfer"),
            effective_at=kw.pop("effective_at", NOW),
            entries=entries,
            **kw,
        )

    def test_balanced_transaction_constructs(self):
        txn = self._txn(
            (
                Entry("acct_expense", Direction.DEBIT, Money(8437, "usd")),
                Entry("acct_checking", Direction.CREDIT, Money(8437, "usd")),
            )
        )
        self.assertEqual(txn.total(Direction.DEBIT), {"usd": Money(8437, "usd")})
        self.assertEqual(txn.total(Direction.CREDIT), {"usd": Money(8437, "usd")})

    def test_unbalanced_transaction_is_impossible_to_construct(self):
        with self.assertRaises(UnbalancedTransaction) as ctx:
            self._txn(
                (
                    Entry("acct_expense", Direction.DEBIT, Money(8437, "usd")),
                    Entry("acct_checking", Direction.CREDIT, Money(8000, "usd")),
                )
            )
        self.assertIn("437", str(ctx.exception))

    def test_must_balance_within_each_currency_not_across(self):
        # sums to zero if you ignore currency; still wrong
        with self.assertRaises(UnbalancedTransaction):
            self._txn(
                (
                    Entry("acct_a", Direction.DEBIT, Money(100, "usd")),
                    Entry("acct_b", Direction.CREDIT, Money(100, "eur")),
                )
            )

    def test_multi_leg_transaction_balances(self):
        txn = self._txn(
            (
                Entry("acct_groceries", Direction.DEBIT, Money(6000, "usd")),
                Entry("acct_household", Direction.DEBIT, Money(2437, "usd")),
                Entry("acct_checking", Direction.CREDIT, Money(8437, "usd")),
            )
        )
        self.assertEqual(len(txn.entries), 3)

    def test_single_entry_is_rejected(self):
        with self.assertRaises(UnbalancedTransaction):
            self._txn((Entry("acct_a", Direction.DEBIT, Money(1, "usd")),))

    def test_negative_and_zero_entries_are_rejected(self):
        with self.assertRaises(LedgerError):
            Entry("acct_a", Direction.DEBIT, Money(-100, "usd"))
        with self.assertRaises(LedgerError):
            Entry("acct_a", Direction.DEBIT, Money(0, "usd"))

    def test_naive_timestamps_are_rejected(self):
        with self.assertRaises(LedgerError):
            self._txn(
                (
                    Entry("acct_a", Direction.DEBIT, Money(1, "usd")),
                    Entry("acct_b", Direction.CREDIT, Money(1, "usd")),
                ),
                effective_at=datetime(2026, 9, 17, 16, 21),  # noqa: DTZ001 -- the point
            )

    def test_entries_are_immutable(self):
        entry = Entry("acct_a", Direction.DEBIT, Money(100, "usd"))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            entry.amount = Money(1, "usd")  # type: ignore[misc]


class TestBalances(unittest.TestCase):
    def setUp(self):
        self.checking = Account("acct_checking", "Assets:Checking", AccountType.ASSET, "usd")
        self.card = Account("acct_card", "Liabilities:Card", AccountType.LIABILITY, "usd")

    def test_asset_rises_on_debit(self):
        entries = [
            Entry("acct_checking", Direction.DEBIT, Money(250_000, "usd")),
            Entry("acct_checking", Direction.CREDIT, Money(120_000, "usd")),
        ]
        self.assertEqual(balance_of(self.checking, entries), Money(130_000, "usd"))

    def test_liability_rises_on_credit(self):
        entries = [Entry("acct_card", Direction.CREDIT, Money(8437, "usd"))]
        self.assertEqual(balance_of(self.card, entries), Money(8437, "usd"))

    def test_entries_for_other_accounts_are_ignored(self):
        entries = [
            Entry("acct_checking", Direction.DEBIT, Money(100, "usd")),
            Entry("acct_somewhere_else", Direction.DEBIT, Money(999_999, "usd")),
        ]
        self.assertEqual(balance_of(self.checking, entries), Money(100, "usd"))

    def test_wrong_currency_entry_is_rejected(self):
        with self.assertRaises(CurrencyMismatch):
            balance_of(self.checking, [Entry("acct_checking", Direction.DEBIT, Money(1, "eur"))])


class TestPostingRules(unittest.TestCase):
    def test_card_purchase_debits_expense_credits_funding(self):
        txn = build_transaction(
            "txn_1",
            "card_purchase",
            PostingRequest(
                amount=Money.parse("84.37", "usd"),
                effective_at=NOW,
                accounts={"expense": "acct_groceries", "funding": "acct_checking"},
                metadata={"merchant": "H-E-B"},
            ),
        )
        by_dir = {e.direction: e.account_id for e in txn.entries}
        self.assertEqual(by_dir[Direction.DEBIT], "acct_groceries")
        self.assertEqual(by_dir[Direction.CREDIT], "acct_checking")

    def test_refund_is_the_mirror_of_a_purchase(self):
        accounts = {"expense": "acct_groceries", "funding": "acct_checking"}
        req = PostingRequest(Money(8437, "usd"), NOW, accounts)
        purchase = build_transaction("txn_1", "card_purchase", req)
        refund = build_transaction("txn_2", "refund", req)

        checking = Account("acct_checking", "Assets:Checking", AccountType.ASSET, "usd")
        net = balance_of(checking, [*purchase.entries, *refund.entries])
        self.assertTrue(net.is_zero)

    def test_split_purchase_allocations_must_sum_to_the_charge(self):
        accounts = {
            "expense_groceries": "acct_g",
            "expense_household": "acct_h",
            "funding": "acct_checking",
        }
        ok = build_transaction(
            "txn_1",
            "split_purchase",
            PostingRequest(
                Money(8437, "usd"), NOW, accounts,
                metadata={"split": "expense_groceries:6000,expense_household:2437"},
            ),
        )
        self.assertEqual(len(ok.entries), 3)

        with self.assertRaises(LedgerError):
            build_transaction(
                "txn_2",
                "split_purchase",
                PostingRequest(
                    Money(8437, "usd"), NOW, accounts,
                    metadata={"split": "expense_groceries:6000,expense_household:1000"},
                ),
            )

    def test_transfer_to_self_is_rejected(self):
        with self.assertRaises(LedgerError):
            build_transaction(
                "txn_1",
                "transfer",
                PostingRequest(
                    Money(100, "usd"), NOW,
                    {"source": "acct_a", "destination": "acct_a"},
                ),
            )

    def test_unknown_kind_names_the_known_ones(self):
        with self.assertRaises(UnknownPostingKind) as ctx:
            build_transaction(
                "txn_1", "wire_to_mars",
                PostingRequest(Money(1, "usd"), NOW, {}),
            )
        self.assertIn("card_purchase", str(ctx.exception))

    def test_every_registered_rule_is_documented(self):
        for kind, debit, credit, description in describe_rules():
            self.assertTrue(description, f"{kind} has no description")
            self.assertTrue(debit and credit, f"{kind} has no declared shape")


class TestReversal(unittest.TestCase):
    def setUp(self):
        self.txn = JournalTransaction(
            id="txn_1",
            kind="card_purchase",
            effective_at=NOW,
            entries=(
                Entry("acct_groceries", Direction.DEBIT, Money(8437, "usd")),
                Entry("acct_checking", Direction.CREDIT, Money(8437, "usd")),
            ),
        )

    def test_reversal_nets_to_zero_and_keeps_the_history(self):
        reversal = self.txn.reverse("txn_2", effective_at=NOW + timedelta(days=1))
        self.assertEqual(reversal.reverses_id, "txn_1")

        checking = Account("acct_checking", "Assets:Checking", AccountType.ASSET, "usd")
        net = balance_of(checking, [*self.txn.entries, *reversal.entries])
        self.assertTrue(net.is_zero)
        # the original is untouched -- correction, not deletion
        self.assertEqual(len(self.txn.entries), 2)

    def test_reversing_a_reversal_is_refused(self):
        reversal = self.txn.reverse("txn_2")
        with self.assertRaises(LedgerError):
            reversal.reverse("txn_3")


class TestLedgerProperties(unittest.TestCase):
    """The global invariant, over randomized posting sequences.

    A stand-in for the Hypothesis suite; the shape of the assertion is the same.
    """

    def test_random_postings_never_unbalance_the_ledger(self):
        rng = random.Random(20260917)
        kinds = ["card_purchase", "deposit", "transfer", "fee", "refund"]
        roles = {
            "card_purchase": {"expense": "acct_exp", "funding": "acct_chk"},
            "deposit": {"destination": "acct_chk", "income": "acct_inc"},
            "transfer": {"source": "acct_chk", "destination": "acct_sav"},
            "fee": {"fee_expense": "acct_fee", "funding": "acct_chk"},
            "refund": {"expense": "acct_exp", "funding": "acct_chk"},
        }

        all_entries: list[Entry] = []
        for i in range(2_000):
            kind = rng.choice(kinds)
            txn = build_transaction(
                f"txn_{i}",
                kind,
                PostingRequest(
                    amount=Money(rng.randint(1, 5_000_00), "usd"),
                    effective_at=NOW + timedelta(seconds=i),
                    accounts=roles[kind],
                ),
            )
            all_entries.extend(txn.entries)

        # The invariant that must hold across the entire ledger, for every
        # currency: debits and credits cancel. If this ever fails, money was
        # created or destroyed.
        drift = sum(
            e.amount.minor if e.direction is Direction.DEBIT else -e.amount.minor
            for e in all_entries
        )
        self.assertEqual(drift, 0)

        # And the accounting identity: every account's balance, summed with the
        # correct sign convention, also nets to zero.
        accounts = [
            Account("acct_chk", "Assets:Checking", AccountType.ASSET, "usd"),
            Account("acct_sav", "Assets:Savings", AccountType.ASSET, "usd"),
            Account("acct_exp", "Expenses:General", AccountType.EXPENSE, "usd"),
            Account("acct_fee", "Expenses:Fees", AccountType.EXPENSE, "usd"),
            Account("acct_inc", "Revenue:Income", AccountType.REVENUE, "usd"),
        ]
        signed_total = 0
        for account in accounts:
            balance = balance_of(account, all_entries)
            sign = 1 if account.normal_balance is Direction.DEBIT else -1
            signed_total += sign * balance.minor
        self.assertEqual(signed_total, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
