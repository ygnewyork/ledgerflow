# Domain model

The rule: **objects in `domain/` know nothing about the database, the API, or
the broker.** They take values, enforce invariants, and return values. Every
piece of I/O lives in an adapter behind an interface.

---

## Value objects

### `Money`

Integer minor units plus a currency. Never a float, never a bare `Decimal`.

```python
Money(8437, "usd")             # $84.37
Money.parse("84.37", "usd")    # same
Money.parse("84.375", "usd")   # raises — sub-cent precision is a bug, not a rounding opportunity
Money(1, "usd") + Money(1, "eur")   # raises CurrencyMismatch
```

Three decisions worth defending in an interview:

1. **Minor units.** `0.1 + 0.2 != 0.3` in binary floating point. A ledger that
   drifts by a cent per million transactions is a ledger nobody trusts.
2. **Parsing rejects excess precision** rather than rounding. If an upstream
   system sends `84.375`, that is a schema disagreement; silently resolving it
   makes the discrepancy invisible until reconciliation.
3. **Currency is part of the type.** Cross-currency arithmetic is not an
   operation you want to be possible by accident.

### `Direction`

`"debit" | "credit"`. Amounts are always positive; the direction carries the
sign. Signed amounts invite `-(-x)` bugs and make `SUM()` queries ambiguous
about which side of the ledger you are on.

---

## Entities

### `Account`

```
id              acct_01J…            (ULID, sortable by creation time)
tenant_id       the API key's owner
external_id     caller's own id, unique per tenant
type            asset | liability | equity | revenue | expense
normal_balance  debit | credit      (derived from type, stored explicitly)
currency        usd
status          open | frozen | closed
```

`normal_balance` is stored rather than computed so that balance queries are a
plain join, and so a future exotic account type does not require a code deploy
to be summed correctly.

Balance sign convention:

```
signed(entry) = +amount  if entry.direction == account.normal_balance
                -amount  otherwise
```

An asset account (normal balance: debit) goes up on debits. A liability
(normal: credit) goes up on credits. This one line removes every "wait, is a
credit positive?" bug.

### `Entry`

```
id            bigserial   ← the global ordering of the ledger
transaction_id
account_id
direction
amount        Money
effective_at  when the money moved
recorded_at   when we learned about it
```

Immutable. No `UPDATE`, no `DELETE` — enforced by a trigger, not by discipline.

### `JournalTransaction`

The atomic unit. Two or more entries that must balance:

```python
JournalTransaction(
    id="txn_01J…",
    kind="card_purchase",
    effective_at=datetime(2026, 9, 17, 16, 21, tzinfo=UTC),
    entries=(
        Entry("acct_expenses_groceries", "debit",  Money(8437, "usd")),
        Entry("acct_checking",           "credit", Money(8437, "usd")),
    ),
)
```

The constructor validates:

- at least two entries
- every amount strictly positive
- **per currency**, `Σ debits == Σ credits`
- `effective_at` is timezone-aware

Per-currency is the subtle one: a transaction touching USD and EUR legs must
balance *within* each currency. A single number summed across currencies is
meaningless.

This invariant is enforced **twice** — in the constructor and by a deferred
constraint trigger in Postgres. That is not redundancy for its own sake: the
Python check gives a good error message at the API edge, and the database check
guarantees the invariant holds even for a migration script, a `psql` session, or
a future service that bypasses this code.

---

## Posting rules

An event like `card_purchase` is not itself a ledger transaction. Something has
to decide *which accounts get debited and credited*. That mapping is the
business logic, and it belongs in one place.

```python
@posting_rule("card_purchase")
class CardPurchase(PostingRule):
    """Money leaves a funding account and becomes an expense."""
    def build(self, req: PostingRequest) -> tuple[Entry, ...]:
        return (
            Entry(req.expense_account, "debit",  req.amount),
            Entry(req.funding_account, "credit", req.amount),
        )
```

Registered rules, one per event kind:

| Kind | Debit | Credit |
|---|---|---|
| `card_purchase` | `Expenses:<category>` | `Assets:<funding>` |
| `deposit` | `Assets:Checking` | `Revenue:Income` |
| `transfer` | destination | source |
| `fee` | `Expenses:Fees` | `Assets:Checking` |
| `refund` | `Assets:Checking` | `Expenses:<category>` |
| `reversal` | *(mirror of the original, directions flipped)* |

Why a registry instead of `if kind == ...`:

- adding an event type is a new class, not an edit to a growing conditional
- each rule is unit-testable in isolation against the balance invariant
- the set of legal postings is *enumerable* — you can generate the table above
  from the code, which is exactly the kind of thing auditors and interviewers
  both like

### Reversals

There is no "undo". `reverse(txn)` produces a new transaction whose entries are
the original's with directions flipped, carrying `reverses_transaction_id`. The
original stays in the ledger forever. Net balance returns to where it was; the
history records that a mistake happened and when it was corrected.

---

## Bitemporality

Two timestamps on every entry:

- `effective_at` — business time. When the purchase happened.
- `recorded_at` — system time. When our ledger learned about it.

A card authorization on Sep 17 that settles and posts on Sep 19 has
`effective_at = Sep 17`, `recorded_at = Sep 19`. So:

```
balance(account, as_of=Sep 18)                   → includes it
balance(account, as_of=Sep 18, as_known_at=Sep 18) → does not
```

The first is "what is true". The second is "what we believed at the time" —
which is the only honest way to answer *why did the dashboard show a different
number yesterday?*, and the thing that makes the time-travel slider in the demo
a real feature rather than a chart animation.

---

## Balance computation

Naïve: `SUM` every entry for the account. Correct, and O(entries) forever.

Used instead: **snapshot + delta**.

```
balance(acct, t) = snapshot.amount                      -- latest snapshot with entry_id <= cursor
                 + Σ signed(entries after snapshot up to t)
```

A background job writes a snapshot every N entries per account. Reads touch a
bounded number of rows regardless of account age, time-travel queries pick the
newest snapshot at or before the target and walk forward, and — critically —
snapshots are a **cache that can be deleted**, because the entries are the
truth. The reconciliation job does exactly that: recompute from scratch, diff
against the snapshot, alert on any nonzero difference.

For hot-path reads, the current balance is also kept in Redis, written by the
ledger consumer. It is allowed to be stale; the API says which one it served:

```
GET /v1/accounts/acct_123/balance          → Redis, marked "available"
GET /v1/accounts/acct_123/balance?strict=true → Postgres, marked "settled"
```

Being explicit that an eventually-consistent read *is* eventually consistent —
rather than hoping nobody notices — is the actual engineering content here.

---

## Concurrency

Two transfers out of the same account at the same instant:

- The ledger itself does not care. Entries are inserts; they cannot conflict.
- Overdraft prevention does. `SELECT ... FOR UPDATE` on the account row
  serializes balance checks for that account, at the cost of contention on hot
  accounts.
- The deliberate choice: **the ledger never blocks; only accounts with an
  enforced floor take the lock.** Expense and revenue accounts have no floor and
  never serialize.

If a `balance >= amount` check ran without that lock, two concurrent transfers
could each read a sufficient balance and both commit — the textbook write-skew
anomaly, which `READ COMMITTED` will happily allow. Worth writing the failing
test for.
