"""Repositories: every SQL statement the platform runs, in one place.

Written against psycopg directly rather than an ORM. The queries here are the
interesting part of the system -- the deferred balance trigger, the snapshot +
delta balance read, the idempotency claim, ``FOR UPDATE SKIP LOCKED`` -- and an
ORM would hide exactly the parts worth reading.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterable, Sequence

import psycopg

from ..domain.ledger import Account, AccountType, Direction, Entry, JournalTransaction
from ..domain.money import Money


class _Repo:
    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn

    def _all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else []

    def _one(self, sql: str, params: Any = None) -> dict[str, Any] | None:
        rows = self._all(sql, params)
        return rows[0] if rows else None


# ---------------------------------------------------------------------------


class AccountRepository(_Repo):
    def create(
        self,
        *,
        account_id: str,
        tenant_id: str,
        mode: str,
        name: str,
        type: AccountType,
        currency: str,
        external_id: str | None = None,
        minimum_balance: int | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO accounts
                (id, tenant_id, mode, external_id, name, type, normal_balance,
                 currency, minimum_balance, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                account_id,
                tenant_id,
                mode,
                external_id,
                name,
                type.value,
                type.normal_balance.value,
                currency.lower(),
                minimum_balance,
                json.dumps(metadata or {}),
            ),
        )  # type: ignore[return-value]

    def get(self, account_id: str, tenant_id: str, mode: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM accounts WHERE id = %s AND tenant_id = %s AND mode = %s",
            (account_id, tenant_id, mode),
        )

    def get_by_external(self, external_id: str, tenant_id: str, mode: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM accounts WHERE external_id = %s AND tenant_id = %s AND mode = %s",
            (external_id, tenant_id, mode),
        )

    def resolve(self, ref: str, tenant_id: str, mode: str) -> dict[str, Any] | None:
        """Find by our id or the caller's external id, in one round trip.

        Callers may use either, so this used to try one and then the other --
        two queries every time an external id was used, which is the common
        case. One query with both predicates does the same work once. The
        ORDER BY makes the precedence explicit rather than incidental: our own
        id wins if a caller ever sets an external_id that collides with one.
        """
        return self._one(
            """
            SELECT * FROM accounts
             WHERE tenant_id = %(tenant)s AND mode = %(mode)s
               AND (id = %(ref)s OR external_id = %(ref)s)
             ORDER BY (id = %(ref)s) DESC
             LIMIT 1
            """,
            {"ref": ref, "tenant": tenant_id, "mode": mode},
        )

    def list(self, tenant_id: str, mode: str, limit: int = 100) -> list[dict[str, Any]]:
        return self._all(
            "SELECT * FROM accounts WHERE tenant_id = %s AND mode = %s "
            "ORDER BY id LIMIT %s",
            (tenant_id, mode, limit),
        )

    def lock(self, account_id: str) -> dict[str, Any] | None:
        """Serialize balance checks for one account.

        Only accounts with a ``minimum_balance`` need this. Without the lock,
        two concurrent transfers can each read a sufficient balance and both
        commit -- write skew, which READ COMMITTED permits. Accounts with no
        floor never take the lock, so expense and revenue postings never
        contend.
        """
        return self._one("SELECT * FROM accounts WHERE id = %s FOR UPDATE", (account_id,))

    def to_domain(self, row: dict[str, Any]) -> Account:
        return Account(
            id=row["id"],
            name=row["name"],
            type=AccountType(row["type"]),
            currency=row["currency"],
            minimum_balance=(
                Money(row["minimum_balance"], row["currency"])
                if row["minimum_balance"] is not None
                else None
            ),
        )

    # -- balances ----------------------------------------------------------

    def balance(self, account_id: str) -> dict[str, Any]:
        """Current balance: newest snapshot plus the entries after it.

        Bounded work regardless of how old the account is. The snapshot is only
        a cache -- deleting balance_snapshots costs speed, never correctness,
        and the reconciliation job proves that by recomputing from entries.
        """
        row = self._one(
            """
            WITH snap AS (
                SELECT up_to_entry_id, balance_minor
                  FROM balance_snapshots
                 WHERE account_id = %(account)s
                 ORDER BY up_to_entry_id DESC
                 LIMIT 1
            )
            SELECT a.currency,
                   (COALESCE((SELECT balance_minor FROM snap), 0)
                  + COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
                                      THEN e.amount_minor ELSE -e.amount_minor END), 0)
                   )::bigint AS balance_minor,
                   COALESCE((SELECT up_to_entry_id FROM snap), 0) AS snapshot_cursor,
                   COALESCE(MAX(e.id), (SELECT up_to_entry_id FROM snap), 0) AS cursor
              FROM accounts a
              LEFT JOIN entries e
                     ON e.account_id = a.id
                    AND e.id > COALESCE((SELECT up_to_entry_id FROM snap), 0)
             WHERE a.id = %(account)s
             GROUP BY a.currency
            """,
            {"account": account_id},
        )
        return row or {"balance_minor": 0, "currency": "usd", "cursor": 0}

    def balance_at(
        self,
        account_id: str,
        as_of: datetime | None = None,
        as_known_at: datetime | None = None,
    ) -> dict[str, Any]:
        """Bitemporal balance.

        ``as_of`` filters on business time (when money moved); ``as_known_at``
        on system time (when we learned). Pass only as_of for the corrected
        history; pass both to reproduce exactly what the dashboard showed on a
        past date, with backdated corrections excluded.

        No snapshot shortcut here: snapshots are cursor-ordered, and business
        time does not follow entry order once anything is backdated.
        """
        row = self._one(
            """
            SELECT a.currency,
                   COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
                                     THEN e.amount_minor ELSE -e.amount_minor END), 0)::bigint
                       AS balance_minor
              FROM accounts a
              LEFT JOIN entries e
                     ON e.account_id = a.id
                    AND e.effective_at <= COALESCE(%(as_of)s, 'infinity'::timestamptz)
                    AND e.recorded_at  <= COALESCE(%(known)s, 'infinity'::timestamptz)
             WHERE a.id = %(account)s
             GROUP BY a.currency
            """,
            {"account": account_id, "as_of": as_of, "known": as_known_at},
        )
        return row or {"balance_minor": 0, "currency": "usd"}

    def write_snapshot(self, account_id: str) -> dict[str, Any] | None:
        """Fold everything up to the newest entry into a snapshot row."""
        return self._one(
            """
            WITH computed AS (
                SELECT a.id AS account_id,
                       a.currency,
                       COALESCE(MAX(e.id), 0) AS up_to,
                       COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
                                         THEN e.amount_minor ELSE -e.amount_minor END), 0)::bigint
                           AS balance_minor
                  FROM accounts a
                  LEFT JOIN entries e ON e.account_id = a.id
                 WHERE a.id = %s
                 GROUP BY a.id, a.currency
            )
            INSERT INTO balance_snapshots (account_id, up_to_entry_id, balance_minor, currency)
            SELECT account_id, up_to, balance_minor, currency FROM computed
             WHERE up_to > 0
            ON CONFLICT (account_id, up_to_entry_id) DO NOTHING
            RETURNING *
            """,
            (account_id,),
        )

    def balance_history(
        self, tenant_id: str, mode: str, *,
        start: datetime, end: datetime, points: int = 60,
        types: Sequence[str] = ("asset", "liability"),
        account_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """Each account's balance at a series of instants.

        Cumulative to each bucket, not per-bucket: a balance is a stock, not a
        flow. Bucketing the flow and summing it client-side would match only if
        the window began at the account's first entry, which it does not.

        The window is an explicit start and end rather than "last N days" so a
        caller can ask about a fixed past period without the answer moving
        under them as the clock advances.
        """
        clauses = ["a.tenant_id = %(tenant)s", "a.mode = %(mode)s", "a.type = ANY(%(types)s)"]
        params: dict[str, Any] = {
            "tenant": tenant_id, "mode": mode, "start": start, "end": end,
            "points": max(2, points), "types": list(types),
        }
        if account_id:
            clauses.append("a.id = %(account)s")
            params["account"] = account_id

        return self._all(
            f"""
            WITH bucket AS (
                SELECT generate_series(
                    %(start)s::timestamptz, %(end)s::timestamptz,
                    make_interval(secs => GREATEST(1, EXTRACT(EPOCH FROM
                        %(end)s::timestamptz - %(start)s::timestamptz) / %(points)s))
                ) AS at
            )
            SELECT a.id, a.name, a.type, b.at,
                   COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
                                     THEN e.amount_minor ELSE -e.amount_minor END), 0)::bigint
                       AS balance_minor
              FROM accounts a
             CROSS JOIN bucket b
              LEFT JOIN entries e
                     ON e.account_id = a.id AND e.effective_at <= b.at
             WHERE {' AND '.join(clauses)}
             GROUP BY a.id, a.name, a.type, b.at
             ORDER BY a.name, b.at
            """,
            params,
        )

    def reconcile(self) -> list[dict[str, Any]]:
        """Recompute every balance from entries and diff against the cache.

        Any row returned is a bug. Runs nightly; the demo runs it on demand.
        """
        return self._all(
            """
            WITH truth AS (
                SELECT a.id AS account_id,
                       COALESCE(SUM(CASE WHEN e.direction = a.normal_balance
                                         THEN e.amount_minor ELSE -e.amount_minor END), 0)::bigint
                           AS recomputed
                  FROM accounts a
                  LEFT JOIN entries e ON e.account_id = a.id
                 GROUP BY a.id
            ),
            cached AS (
                SELECT DISTINCT ON (account_id) account_id, balance_minor, up_to_entry_id
                  FROM balance_snapshots
                 ORDER BY account_id, up_to_entry_id DESC
            )
            SELECT t.account_id, t.recomputed, c.balance_minor AS cached,
                   c.up_to_entry_id,
                   COALESCE(tail.delta, 0) AS uncached_delta
              FROM truth t
              LEFT JOIN cached c ON c.account_id = t.account_id
              LEFT JOIN LATERAL (
                   SELECT SUM(CASE WHEN e.direction = a.normal_balance
                                   THEN e.amount_minor ELSE -e.amount_minor END)::bigint AS delta
                     FROM entries e JOIN accounts a ON a.id = e.account_id
                    WHERE e.account_id = t.account_id AND e.id > c.up_to_entry_id
              ) tail ON TRUE
             WHERE c.balance_minor IS NOT NULL
               AND t.recomputed <> c.balance_minor + COALESCE(tail.delta, 0)
            """
        )

    def global_drift(self) -> list[dict[str, Any]]:
        """Across the whole ledger, debits minus credits, per currency.

        Must always be empty. If it is not, money was created or destroyed.
        """
        return self._all(
            """
            SELECT currency,
                   SUM(CASE WHEN direction = 'debit' THEN amount_minor
                            ELSE -amount_minor END)::bigint AS drift
              FROM entries
             GROUP BY currency
            HAVING SUM(CASE WHEN direction = 'debit' THEN amount_minor
                            ELSE -amount_minor END) <> 0
            """
        )


# ---------------------------------------------------------------------------


class TransactionRepository(_Repo):
    def insert(
        self,
        txn: JournalTransaction,
        *,
        tenant_id: str,
        mode: str,
    ) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO transactions
                (id, tenant_id, mode, kind, effective_at, reverses_id, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                txn.id,
                tenant_id,
                mode,
                txn.kind,
                txn.effective_at,
                txn.reverses_id,
                json.dumps(dict(txn.metadata)),
            ),
        )  # type: ignore[return-value]

    def get(self, txn_id: str, tenant_id: str, mode: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM transactions WHERE id = %s AND tenant_id = %s AND mode = %s",
            (txn_id, tenant_id, mode),
        )

    def mark_reversed(self, txn_id: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute("UPDATE transactions SET status = 'reversed' WHERE id = %s", (txn_id,))

    def list(
        self,
        *,
        tenant_id: str,
        mode: str,
        account_id: str | None = None,
        category: str | None = None,
        starting_after: str | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        """Cursor pagination on a monotonic id, with the normalized view attached.

        Offset pagination over an append-only log silently skips rows: new
        entries arrive at the head between requests, so page 2 is not the page
        2 the caller expected. A cursor cannot skip.

        The LATERAL picks the newest normalizer version per transaction, so
        shipping a new normalizer changes what this returns without a
        migration -- and without this query needing to know the version exists.
        """
        clauses = ["t.tenant_id = %(tenant)s", "t.mode = %(mode)s"]
        params: dict[str, Any] = {"tenant": tenant_id, "mode": mode, "limit": limit}
        if starting_after:
            clauses.append("t.id < %(after)s")
            params["after"] = starting_after
        if account_id:
            clauses.append(
                "EXISTS (SELECT 1 FROM entries e "
                "WHERE e.transaction_id = t.id AND e.account_id = %(account)s)"
            )
            params["account"] = account_id
        if category:
            # Same definition the breakdown uses: the expense account the money
            # was booked to. Filtering on the normalizer's category instead
            # would return a different set of rows than the bar you clicked.
            clauses.append(
                "EXISTS (SELECT 1 FROM entries e2 JOIN accounts a2 ON a2.id = e2.account_id "
                "WHERE e2.transaction_id = t.id AND a2.type = 'expense' "
                "AND split_part(a2.name, ':', 2) = %(category)s)"
            )
            params["category"] = category

        return self._all(
            f"""
            SELECT t.*,
                   norm.merchant_name, norm.confidence,
                   raw.descriptor,
                   -- the LEDGER's category: the expense account this posting
                   -- was booked to. The same definition the breakdown groups
                   -- by and the filter matches on, so a row can never appear
                   -- under a category whose chip says something else.
                   booked.category
              FROM transactions t
              LEFT JOIN raw_transactions raw ON raw.transaction_id = t.id
              LEFT JOIN LATERAL (
                   SELECT n.merchant_name, n.confidence
                     FROM normalized_transactions n
                    WHERE n.raw_transaction_id = raw.id
                    ORDER BY n.normalizer_version DESC
                    LIMIT 1
              ) norm ON TRUE
              LEFT JOIN LATERAL (
                   SELECT split_part(a2.name, ':', 2) AS category
                     FROM entries e2 JOIN accounts a2 ON a2.id = e2.account_id
                    WHERE e2.transaction_id = t.id AND a2.type = 'expense'
                    LIMIT 1
              ) booked ON TRUE
             WHERE {' AND '.join(clauses)}
             ORDER BY t.id DESC LIMIT %(limit)s
            """,
            params,
        )


class EntryRepository(_Repo):
    def insert_many(
        self,
        txn: JournalTransaction,
        *,
        recorded_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Insert every leg.

        The deferred constraint trigger checks the balance at COMMIT, so a
        half-inserted transaction is legal mid-statement and illegal at the
        boundary -- which is exactly the semantics double-entry needs.
        """
        # One multi-row INSERT rather than one per leg. The deferred trigger
        # fires at COMMIT either way, so this changes nothing about when the
        # balance is checked -- only how many round trips it takes to get there.
        values: list[Any] = []
        placeholders: list[str] = []
        for entry in txn.entries:
            placeholders.append("(%s, %s, %s, %s, %s, %s, COALESCE(%s, now()))")
            values.extend([
                txn.id, entry.account_id, entry.direction.value,
                entry.amount.minor, entry.amount.currency,
                txn.effective_at, recorded_at,
            ])

        with self.conn.cursor() as cur:
            cur.execute(
                "INSERT INTO entries (transaction_id, account_id, direction, "
                "amount_minor, currency, effective_at, recorded_at) VALUES "
                + ", ".join(placeholders)
                + " RETURNING *",
                values,
            )
            return cur.fetchall()  # type: ignore[return-value]

    def for_transaction(self, txn_id: str) -> list[dict[str, Any]]:
        return self._all(
            "SELECT * FROM entries WHERE transaction_id = %s ORDER BY id", (txn_id,)
        )

    def list(
        self,
        *,
        account_id: str | None = None,
        since_entry_id: int = 0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        clauses = ["e.id > %(since)s"]
        params: dict[str, Any] = {"since": since_entry_id, "limit": limit}
        if account_id:
            clauses.append("e.account_id = %(account)s")
            params["account"] = account_id
        return self._all(
            f"""
            SELECT e.*, t.kind, t.metadata
              FROM entries e JOIN transactions t ON t.id = e.transaction_id
             WHERE {' AND '.join(clauses)}
             ORDER BY e.id LIMIT %(limit)s
            """,
            params,
        )

    def to_domain(self, row: dict[str, Any]) -> Entry:
        return Entry(
            account_id=row["account_id"],
            direction=Direction(row["direction"]),
            amount=Money(row["amount_minor"], row["currency"]),
        )


# ---------------------------------------------------------------------------


class EventRepository(_Repo):
    """The outbox, the stream, and the consumer-side bookkeeping."""

    def append_outbox(
        self,
        *,
        event_id: str,
        tenant_id: str,
        mode: str,
        partition_key: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Write the event as a row, in the caller's transaction.

        This is the load-bearing line of the whole system. Publishing to a
        broker after COMMIT leaves a window where the ledger moved and the
        event vanished; there is no way to close that window from application
        code. Writing the event as a row makes it atomic with the money, and
        pushes the at-least-once problem to the relay, where a duplicate is
        survivable.
        """
        return self._one(
            """
            INSERT INTO outbox
                (event_id, tenant_id, mode, partition_key, event_type, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (event_id, tenant_id, mode, partition_key, event_type, json.dumps(payload)),
        )  # type: ignore[return-value]

    def claim_unpublished(self, limit: int = 500) -> list[dict[str, Any]]:
        """Grab a batch for this relay instance.

        FOR UPDATE SKIP LOCKED turns the outbox into a work queue: several relay
        processes each take a disjoint batch with no leader election and no
        coordination service.
        """
        return self._all(
            """
            SELECT * FROM outbox
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT %s
             FOR UPDATE SKIP LOCKED
            """,
            (limit,),
        )

    def mark_published(self, ids: Sequence[int]) -> None:
        if not ids:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE outbox SET published_at = now(), attempts = attempts + 1 "
                "WHERE id = ANY(%s)",
                (list(ids),),
            )

    def outbox_lag(self) -> dict[str, Any]:
        return self._one(
            """
            SELECT COUNT(*) AS pending,
                   COALESCE(EXTRACT(EPOCH FROM now() - MIN(created_at)), 0)::float8
                       AS oldest_seconds
              FROM outbox WHERE published_at IS NULL
            """
        ) or {"pending": 0, "oldest_seconds": 0}

    def get(self, event_id: str, tenant_id: str, mode: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM outbox WHERE event_id = %s AND tenant_id = %s AND mode = %s",
            (event_id, tenant_id, mode),
        )

    def list(
        self, *, tenant_id: str, mode: str, starting_after: str | None = None, limit: int = 25
    ) -> list[dict[str, Any]]:
        clauses = ["tenant_id = %(tenant)s", "mode = %(mode)s"]
        params: dict[str, Any] = {"tenant": tenant_id, "mode": mode, "limit": limit}
        if starting_after:
            clauses.append("event_id < %(after)s")
            params["after"] = starting_after
        return self._all(
            f"SELECT * FROM outbox WHERE {' AND '.join(clauses)} "
            "ORDER BY event_id DESC LIMIT %(limit)s",
            params,
        )

    # -- consumer side -----------------------------------------------------

    def claim_event(self, consumer_group: str, event_id: str) -> bool:
        """Claim an event for a consumer group.

        Returns False when this group already processed it. Called inside the
        consumer's work transaction, so the claim and the side effect commit
        together: at-least-once delivery plus this claim is what "effectively
        once" actually means.
        """
        row = self._one(
            "INSERT INTO processed_events (consumer_group, event_id) VALUES (%s, %s) "
            "ON CONFLICT DO NOTHING RETURNING 1 AS claimed",
            (consumer_group, event_id),
        )
        return row is not None

    def release_claim(self, consumer_group: str, event_id: str) -> None:
        """Undo a claim, for replays that intentionally reprocess history."""
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM processed_events WHERE consumer_group = %s AND event_id = %s",
                (consumer_group, event_id),
            )

    def dead_letter(
        self,
        *,
        dlq_id: str,
        consumer_group: str,
        event_id: str,
        topic: str,
        payload: dict[str, Any],
        error_class: str,
        error_detail: str,
        attempts: int,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dead_letters
                    (id, consumer_group, event_id, topic, payload, error_class,
                     error_detail, attempts, first_failed_at, last_failed_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
                """,
                (
                    dlq_id,
                    consumer_group,
                    event_id,
                    topic,
                    json.dumps(payload),
                    error_class,
                    error_detail[:2000],
                    attempts,
                ),
            )

    def list_dead_letters(
        self, *, consumer_group: str | None = None, unresolved: bool = True, limit: int = 50
    ) -> list[dict[str, Any]]:
        clauses = []
        params: dict[str, Any] = {"limit": limit}
        if unresolved:
            clauses.append("resolved_at IS NULL")
        if consumer_group:
            clauses.append("consumer_group = %(group)s")
            params["group"] = consumer_group
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return self._all(
            f"SELECT * FROM dead_letters {where} ORDER BY last_failed_at DESC LIMIT %(limit)s",
            params,
        )

    def resolve_dead_letter(self, dlq_id: str) -> dict[str, Any] | None:
        return self._one(
            "UPDATE dead_letters SET resolved_at = now() WHERE id = %s AND resolved_at IS NULL "
            "RETURNING *",
            (dlq_id,),
        )

    def dlq_depth(self) -> int:
        row = self._one("SELECT COUNT(*) AS n FROM dead_letters WHERE resolved_at IS NULL")
        return int(row["n"]) if row else 0


class IdempotencyRepository(_Repo):
    def claim(
        self,
        *,
        record_id: str,
        tenant_id: str,
        api_key_id: str,
        key: str,
        request_hash: bytes,
        lease_seconds: int,
        ttl_hours: int,
    ) -> dict[str, Any] | None:
        """Try to take ownership of an idempotency key.

        Returns the new row if we claimed it, or None if someone else holds it.
        Runs inside the caller's transaction, so the claim commits with the work
        it protects -- see docs/03-api.md for why no other placement survives a
        crash.
        """
        return self._one(
            """
            INSERT INTO idempotency_keys
                (id, tenant_id, api_key_id, idempotency_key, request_hash, status,
                 lease_expires_at, expires_at)
            VALUES (%s, %s, %s, %s, %s, 'in_progress',
                    now() + make_interval(secs => %s), now() + make_interval(hours => %s))
            ON CONFLICT (api_key_id, idempotency_key) DO NOTHING
            RETURNING *
            """,
            (record_id, tenant_id, api_key_id, key, request_hash, lease_seconds, ttl_hours),
        )

    def get(self, api_key_id: str, key: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT *, lease_expires_at < now() AS lease_expired "
            "FROM idempotency_keys WHERE api_key_id = %s AND idempotency_key = %s",
            (api_key_id, key),
        )

    def take_over_expired(
        self, *, api_key_id: str, key: str, request_hash: bytes, lease_seconds: int
    ) -> dict[str, Any] | None:
        """Reclaim a key whose owner died mid-request.

        The WHERE clause re-checks the lease, so two processes racing to
        reclaim the same abandoned key cannot both win.
        """
        return self._one(
            """
            UPDATE idempotency_keys
               SET request_hash = %s,
                   lease_expires_at = now() + make_interval(secs => %s)
             WHERE api_key_id = %s AND idempotency_key = %s
               AND status = 'in_progress' AND lease_expires_at < now()
            RETURNING *
            """,
            (request_hash, lease_seconds, api_key_id, key),
        )

    def complete(
        self, *, record_id: str, response_code: int, response_body: dict[str, Any], resource_id: str | None
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE idempotency_keys
                   SET status = 'completed', response_code = %s,
                       response_body = %s, resource_id = %s
                 WHERE id = %s
                """,
                (response_code, json.dumps(response_body), resource_id, record_id),
            )

    def purge_expired(self) -> int:
        row = self._one(
            "WITH gone AS (DELETE FROM idempotency_keys WHERE expires_at < now() RETURNING 1) "
            "SELECT COUNT(*) AS n FROM gone"
        )
        return int(row["n"]) if row else 0


class NormalizationRepository(_Repo):
    def insert_raw(
        self,
        *,
        raw_id: str,
        tenant_id: str,
        mode: str,
        account_id: str,
        transaction_id: str | None,
        payload: dict[str, Any],
        descriptor: str,
        amount_minor: int,
        currency: str,
        occurred_at: datetime,
    ) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO raw_transactions
                (id, tenant_id, mode, account_id, transaction_id, payload,
                 descriptor, amount_minor, currency, occurred_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (
                raw_id, tenant_id, mode, account_id, transaction_id, json.dumps(payload),
                descriptor, amount_minor, currency, occurred_at,
            ),
        )  # type: ignore[return-value]

    def raw_for_transaction(self, transaction_id: str) -> dict[str, Any] | None:
        return self._one(
            "SELECT * FROM raw_transactions WHERE transaction_id = %s", (transaction_id,)
        )

    def candidates(self, cleaned: str, limit: int = 5) -> list[dict[str, Any]]:
        """Trigram similarity against the alias dictionary.

        pg_trgm turns "which merchant is this garbage string" into an indexed
        search. The GIN index means adding merchants does not slow this down
        linearly, which a LIKE scan would.
        """
        return self._all(
            """
            SELECT ma.merchant_id, ma.pattern, ma.weight,
                   m.display_name, m.category,
                   similarity(ma.pattern, %(text)s) AS score
              FROM merchant_aliases ma
              JOIN merchants m ON m.id = ma.merchant_id
             WHERE ma.pattern %% %(text)s
             ORDER BY similarity(ma.pattern, %(text)s) * ma.weight DESC
             LIMIT %(limit)s
            """,
            {"text": cleaned, "limit": limit},
        )

    def merchant_prior(self, tenant_id: str, merchant_id: str) -> int:
        row = self._one(
            "SELECT COUNT(*) AS n FROM normalized_transactions "
            "WHERE tenant_id = %s AND merchant_id = %s",
            (tenant_id, merchant_id),
        )
        return int(row["n"]) if row else 0

    def upsert_normalized(
        self,
        *,
        normalized_id: str,
        raw_transaction_id: str,
        tenant_id: str,
        account_id: str,
        merchant_id: str | None,
        merchant_name: str | None,
        category: str | None,
        confidence: float | None,
        normalizer_version: int,
    ) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO normalized_transactions
                (id, raw_transaction_id, tenant_id, account_id, merchant_id,
                 merchant_name, category, confidence, normalizer_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (raw_transaction_id, normalizer_version) DO UPDATE
                SET merchant_id = EXCLUDED.merchant_id,
                    merchant_name = EXCLUDED.merchant_name,
                    category = EXCLUDED.category,
                    confidence = EXCLUDED.confidence,
                    normalized_at = now()
            RETURNING *
            """,
            (
                normalized_id, raw_transaction_id, tenant_id, account_id, merchant_id,
                merchant_name, category, confidence, normalizer_version,
            ),
        )  # type: ignore[return-value]

    def spend_by_category(
        self, tenant_id: str, account_id: str | None = None, days: int = 90,
        mode: str = "test",
    ) -> list[dict[str, Any]]:
        """Spend grouped by the expense account it was booked to.

        Deliberately the LEDGER's categories, not the normalizer's merchant
        categories. The normalizer guesses a category from a descriptor and is
        allowed to say "I don't know"; the posting rule already decided which
        expense account the money hit, and it is never unknown. Grouping by the
        guess put rent -- whose landlord no merchant dictionary contains --
        into "Uncategorized" as the single largest line, which is both wrong
        and the opposite of what the panel is for.

        So the amounts reconcile exactly with the ledger, and the normalizer's
        accuracy is reported alongside as `unresolved` rather than by silently
        eating a category.
        """
        clauses = [
            "a.tenant_id = %(tenant)s",
            "a.mode = %(mode)s",
            "a.type = 'expense'",
            # A mark-to-market loss is booked as an expense because that is
            # what keeps the ledger balanced, but it is not money that left to
            # buy anything. Showing it beside groceries in a "where did it go"
            # panel answers a question nobody asked.
            "a.name <> 'Expenses:Investment Losses'",
            "e.effective_at > now() - make_interval(days => %(days)s)",
        ]
        params: dict[str, Any] = {"tenant": tenant_id, "days": days, "mode": mode}
        if account_id:
            # spend funded BY this account, wherever it was booked
            clauses.append(
                "EXISTS (SELECT 1 FROM entries f WHERE f.transaction_id = e.transaction_id "
                "AND f.account_id = %(account)s)"
            )
            params["account"] = account_id

        return self._all(
            f"""
            SELECT split_part(a.name, ':', 2) AS category,
                   -- debits increase an expense, refunds credit it back; the
                   -- net is what was actually spent
                   SUM(CASE WHEN e.direction = 'debit'
                            THEN e.amount_minor ELSE -e.amount_minor END)::bigint AS spend_minor,
                   COUNT(*)::int AS txn_count,
                   (COUNT(*) FILTER (
                       WHERE raw.id IS NOT NULL AND norm.merchant_id IS NULL
                   ))::int AS unresolved
              FROM entries e
              JOIN accounts a ON a.id = e.account_id
              LEFT JOIN raw_transactions raw ON raw.transaction_id = e.transaction_id
              LEFT JOIN LATERAL (
                   SELECT n.merchant_id FROM normalized_transactions n
                    WHERE n.raw_transaction_id = raw.id
                    ORDER BY n.normalizer_version DESC LIMIT 1
              ) norm ON TRUE
             WHERE {' AND '.join(clauses)}
             GROUP BY 1
            HAVING SUM(CASE WHEN e.direction = 'debit'
                            THEN e.amount_minor ELSE -e.amount_minor END) > 0
             ORDER BY 2 DESC
            """,
            params,
        )

    def spending_history(
        self, tenant_id: str, mode: str, *,
        start: datetime, end: datetime, points: int = 60,
    ) -> list[dict[str, Any]]:
        """Cumulative spend per category, within the window.

        Bounded to the window on purpose. An expense account never decreases,
        so a to-date cumulative would start each series at whatever had already
        accumulated and flatten the part being asked about.
        """
        return self._all(
            """
            WITH bucket AS (
                SELECT generate_series(
                    %(start)s::timestamptz, %(end)s::timestamptz,
                    make_interval(secs => GREATEST(1, EXTRACT(EPOCH FROM
                        %(end)s::timestamptz - %(start)s::timestamptz) / %(points)s))
                ) AS at
            )
            SELECT split_part(a.name, ':', 2) AS category, b.at,
                   COALESCE(SUM(CASE WHEN e.direction = 'debit'
                                     THEN e.amount_minor ELSE -e.amount_minor END), 0)::bigint
                       AS spend_minor
              FROM accounts a
             CROSS JOIN bucket b
              LEFT JOIN entries e
                     ON e.account_id = a.id
                    AND e.effective_at <= b.at
                    AND e.effective_at > %(start)s::timestamptz
             WHERE a.tenant_id = %(tenant)s AND a.mode = %(mode)s AND a.type = 'expense'
               AND a.name <> 'Expenses:Investment Losses'
             GROUP BY 1, b.at
             ORDER BY 1, b.at
            """,
            {"tenant": tenant_id, "mode": mode, "start": start, "end": end,
             "points": max(2, points)},
        )

    def version_diff(self, left: int, right: int) -> list[dict[str, Any]]:
        """What changed between two normalizer versions, on identical inputs.

        The reason normalizer_version exists: you can see the blast radius of a
        release before trusting it.
        """
        return self._all(
            """
            SELECT r.descriptor,
                   l.merchant_name AS left_merchant, l.confidence AS left_confidence,
                   rt.merchant_name AS right_merchant, rt.confidence AS right_confidence
              FROM normalized_transactions l
              JOIN normalized_transactions rt
                ON rt.raw_transaction_id = l.raw_transaction_id
               AND rt.normalizer_version = %s
              JOIN raw_transactions r ON r.id = l.raw_transaction_id
             WHERE l.normalizer_version = %s
               AND l.merchant_id IS DISTINCT FROM rt.merchant_id
            """,
            (right, left),
        )


class RiskRepository(_Repo):
    def upsert_features(self, account_id: str, window_end: datetime, features: dict[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO account_features
                    (account_id, window_end, spend_1h_minor, spend_24h_minor,
                     txn_count_1h, max_amount_1h_minor, distinct_merchants_7d,
                     avg_amount_90d_minor, stddev_amount_90d)
                VALUES (%(account)s, %(window_end)s, %(spend_1h)s, %(spend_24h)s,
                        %(txn_count_1h)s, %(max_1h)s, %(distinct_7d)s,
                        %(avg_90d)s, %(stddev_90d)s)
                ON CONFLICT (account_id, window_end) DO UPDATE SET
                    spend_1h_minor = EXCLUDED.spend_1h_minor,
                    spend_24h_minor = EXCLUDED.spend_24h_minor,
                    txn_count_1h = EXCLUDED.txn_count_1h,
                    max_amount_1h_minor = EXCLUDED.max_amount_1h_minor,
                    distinct_merchants_7d = EXCLUDED.distinct_merchants_7d,
                    avg_amount_90d_minor = EXCLUDED.avg_amount_90d_minor,
                    stddev_amount_90d = EXCLUDED.stddev_amount_90d,
                    computed_at = now()
                """,
                {"account": account_id, "window_end": window_end, **features},
            )

    def insert_signal(
        self,
        *,
        signal_id: str,
        tenant_id: str,
        account_id: str,
        transaction_id: str | None,
        rule: str,
        score: float,
        features: dict[str, Any],
    ) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO fraud_signals
                (id, tenant_id, account_id, transaction_id, rule, score, features)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (signal_id, tenant_id, account_id, transaction_id, rule, score,
             json.dumps(features, default=str)),
        )  # type: ignore[return-value]

    def list_signals(
        self, *, tenant_id: str, account_id: str | None = None, limit: int = 50
    ) -> list[dict[str, Any]]:
        clauses = ["s.tenant_id = %(tenant)s"]
        params: dict[str, Any] = {"tenant": tenant_id, "limit": limit}
        if account_id:
            clauses.append("s.account_id = %(account)s")
            params["account"] = account_id
        return self._all(
            f"SELECT s.*, a.name AS account_name FROM fraud_signals s "
            f"JOIN accounts a ON a.id = s.account_id "
            f"WHERE {' AND '.join(clauses)} ORDER BY s.evaluated_at DESC LIMIT %(limit)s",
            params,
        )

    def record_late_arrival(
        self, *, event_id: str, account_id: str, effective_at: datetime, watermark_at: datetime
    ) -> None:
        """An event too late for its window.

        Dropping it from the aggregate is correct. Dropping it silently is not:
        a rising late-arrival rate means the watermark is too tight or an
        upstream producer is lagging, and you only find out if you count them.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO late_arrivals
                    (event_id, account_id, effective_at, watermark_at, lateness)
                VALUES (%s, %s, %s, %s, %s - %s)
                """,
                (event_id, account_id, effective_at, watermark_at, watermark_at, effective_at),
            )

    def spend_window(
        self, account_id: str, start: datetime, end: datetime
    ) -> dict[str, Any]:
        """Windowed aggregate straight from the ledger.

        The durable counterpart of the Redis sliding window: same window, same
        bounds, recomputable. When the two disagree, this one is right.
        """
        return self._one(
            """
            SELECT COALESCE(SUM(e.amount_minor), 0)::bigint AS spend_minor,
                   COUNT(*)::int AS txn_count,
                   COALESCE(MAX(e.amount_minor), 0)::bigint AS max_amount_minor
              FROM entries e
              JOIN accounts a ON a.id = e.account_id
             WHERE e.account_id = %s
               AND e.direction <> a.normal_balance      -- money leaving the account
               AND e.effective_at > %s AND e.effective_at <= %s
            """,
            (account_id, start, end),
        ) or {"spend_minor": 0, "txn_count": 0, "max_amount_minor": 0}

    def baseline(self, account_id: str, end: datetime, days: int = 90) -> dict[str, Any]:
        return self._one(
            """
            SELECT COALESCE(AVG(e.amount_minor), 0)::float8 AS avg_amount_minor,
                   COALESCE(STDDEV_POP(e.amount_minor), 0)::float8 AS stddev_amount,
                   COUNT(*)::int AS sample_size
              FROM entries e
              JOIN accounts a ON a.id = e.account_id
             WHERE e.account_id = %s
               AND e.direction <> a.normal_balance
               AND e.effective_at > %s - make_interval(days => %s)
               AND e.effective_at <= %s
            """,
            (account_id, end, days, end),
        ) or {"avg_amount_minor": 0, "stddev_amount": 0, "sample_size": 0}

    def merchant_stats(
        self, account_id: str, merchant_id: str | None, end: datetime
    ) -> dict[str, Any]:
        return self._one(
            """
            SELECT (COUNT(*) FILTER (WHERE n.merchant_id = %(merchant)s))::int
                       AS merchant_frequency,
                   (COUNT(DISTINCT n.merchant_id) FILTER (
                       WHERE r.occurred_at > %(end)s - interval '7 days'
                   ))::int AS distinct_merchants_7d
              FROM normalized_transactions n
              JOIN raw_transactions r ON r.id = n.raw_transaction_id
             WHERE n.account_id = %(account)s AND r.occurred_at <= %(end)s
            """,
            {"account": account_id, "merchant": merchant_id, "end": end},
        ) or {"merchant_frequency": 0, "distinct_merchants_7d": 0}


class WebhookRepository(_Repo):
    def create_endpoint(
        self,
        *,
        endpoint_id: str,
        tenant_id: str,
        mode: str,
        url: str,
        secret_hash: bytes,
        enabled_events: Sequence[str],
    ) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO webhook_endpoints
                (id, tenant_id, mode, url, secret_hash, enabled_events)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING *
            """,
            (endpoint_id, tenant_id, mode, url, secret_hash, list(enabled_events)),
        )  # type: ignore[return-value]

    def endpoints_for(self, tenant_id: str, mode: str, event_type: str) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT * FROM webhook_endpoints
             WHERE tenant_id = %s AND mode = %s AND status = 'enabled'
               AND (%s = ANY(enabled_events) OR '*' = ANY(enabled_events))
            """,
            (tenant_id, mode, event_type),
        )

    def get_endpoint(self, endpoint_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM webhook_endpoints WHERE id = %s", (endpoint_id,))

    def enqueue(self, *, delivery_id: str, endpoint_id: str, event_id: str) -> dict[str, Any]:
        return self._one(
            """
            INSERT INTO webhook_deliveries
                (id, endpoint_id, event_id, status, next_attempt_at)
            VALUES (%s, %s, %s, 'pending', now())
            RETURNING *
            """,
            (delivery_id, endpoint_id, event_id),
        )  # type: ignore[return-value]

    def claim_due(self, limit: int = 50) -> list[dict[str, Any]]:
        return self._all(
            """
            SELECT d.*, e.url, e.secret_hash, e.tenant_id, e.mode
              FROM webhook_deliveries d
              JOIN webhook_endpoints e ON e.id = d.endpoint_id
             WHERE d.status = 'pending' AND d.next_attempt_at <= now()
             ORDER BY d.next_attempt_at
             LIMIT %s
             FOR UPDATE OF d SKIP LOCKED
            """,
            (limit,),
        )

    def record_attempt(
        self,
        *,
        delivery_id: str,
        attempt: int,
        status: str,
        response_code: int | None,
        response_body: str | None,
        error: str | None,
        next_attempt_seconds: int | None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE webhook_deliveries
                   SET attempt = %s,
                       status = %s,
                       response_code = %s,
                       response_body = LEFT(%s, 2000),
                       error = LEFT(%s, 2000),
                       next_attempt_at = CASE WHEN %s IS NULL THEN NULL
                                              ELSE now() + make_interval(secs => %s) END,
                       completed_at = CASE WHEN %s IN ('succeeded', 'exhausted')
                                           THEN now() ELSE NULL END
                 WHERE id = %s
                """,
                (attempt, status, response_code, response_body, error,
                 next_attempt_seconds, next_attempt_seconds, status, delivery_id),
            )

    def disable_endpoint(self, endpoint_id: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE webhook_endpoints SET status = 'disabled' WHERE id = %s", (endpoint_id,)
            )

    def deliveries_for(self, endpoint_id: str, limit: int = 50) -> list[dict[str, Any]]:
        return self._all(
            "SELECT * FROM webhook_deliveries WHERE endpoint_id = %s "
            "ORDER BY created_at DESC LIMIT %s",
            (endpoint_id, limit),
        )

    def get_delivery(self, delivery_id: str) -> dict[str, Any] | None:
        return self._one("SELECT * FROM webhook_deliveries WHERE id = %s", (delivery_id,))

    def retry(self, delivery_id: str) -> dict[str, Any] | None:
        return self._one(
            "UPDATE webhook_deliveries SET status = 'pending', next_attempt_at = now(), "
            "completed_at = NULL WHERE id = %s RETURNING *",
            (delivery_id,),
        )

    def success_rate(self) -> dict[str, Any]:
        return self._one(
            """
            SELECT COUNT(*) FILTER (WHERE status = 'succeeded') AS succeeded,
                   COUNT(*) FILTER (WHERE status = 'exhausted') AS exhausted,
                   COUNT(*) FILTER (WHERE status = 'pending')   AS pending,
                   COUNT(*) AS total
              FROM webhook_deliveries
             WHERE created_at > now() - interval '24 hours'
            """
        ) or {"succeeded": 0, "exhausted": 0, "pending": 0, "total": 0}
