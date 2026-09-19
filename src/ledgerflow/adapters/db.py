"""Database access: a pool, and one transaction boundary.

The whole platform's correctness argument rests on "these writes happen in one
transaction", so the transaction boundary is a single explicit object rather
than something implied by decorators scattered across the code. If you can see
where ``with unit_of_work() as uow:`` opens and closes, you can see what is
atomic.
"""

from __future__ import annotations

import contextlib
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..config import settings

_pool: ConnectionPool | None = None


def pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            settings.database_url,
            min_size=1,
            max_size=10,
            kwargs={"row_factory": dict_row},
            open=True,
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None


class UnitOfWork:
    """One connection, one transaction, and the repositories that share it.

    Repositories are attributes rather than separate objects constructed with a
    connection, so there is no way to accidentally use two of them against two
    different transactions.
    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        # imported here to avoid a circular import at module load
        from .repositories import (
            AccountRepository,
            EntryRepository,
            EventRepository,
            IdempotencyRepository,
            NormalizationRepository,
            RiskRepository,
            TransactionRepository,
            WebhookRepository,
        )

        self.accounts = AccountRepository(conn)
        self.transactions = TransactionRepository(conn)
        self.entries = EntryRepository(conn)
        self.events = EventRepository(conn)
        self.idempotency = IdempotencyRepository(conn)
        self.normalization = NormalizationRepository(conn)
        self.risk = RiskRepository(conn)
        self.webhooks = WebhookRepository(conn)

    def execute(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description is None:
                return []
            return cur.fetchall()

    def one(self, sql: str, params: Any = None) -> dict[str, Any] | None:
        rows = self.execute(sql, params)
        return rows[0] if rows else None


@contextlib.contextmanager
def unit_of_work() -> Iterator[UnitOfWork]:
    """Open a transaction. Commits on clean exit, rolls back on any exception.

    Note what is NOT here: no nested-transaction helper, no savepoint API. The
    deferred balance trigger fires at COMMIT, so a savepoint that "succeeds"
    inside a transaction proves nothing about whether the ledger is balanced.
    One boundary, one answer.
    """
    with pool().connection() as conn:
        with conn.transaction():
            yield UnitOfWork(conn)


@contextlib.contextmanager
def read_only() -> Iterator[UnitOfWork]:
    """A connection for queries. Still transactional, just never writes."""
    with pool().connection() as conn:
        yield UnitOfWork(conn)


def migrate(directory: str = "migrations") -> list[str]:
    """Apply every .sql file in order. Idempotent via a ledger of applied files."""
    import pathlib

    applied: list[str] = []
    with pool().connection() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations ("
            " filename TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())"
        )
        conn.commit()
        done = {
            r["filename"]
            for r in conn.execute("SELECT filename FROM schema_migrations").fetchall()
        }
        for path in sorted(pathlib.Path(directory).glob("*.sql")):
            if path.name in done:
                continue
            # each migration is its own transaction: a failure leaves the
            # previous ones applied and names the file that broke
            with conn.transaction():
                conn.execute(path.read_text())
                conn.execute(
                    "INSERT INTO schema_migrations (filename) VALUES (%s)", (path.name,)
                )
            applied.append(path.name)
    return applied
