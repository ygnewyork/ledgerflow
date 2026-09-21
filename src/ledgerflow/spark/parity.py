"""Does the offline feature equal the online one?

The pipeline docs claim "offline/online parity". This module is what makes
that claim checkable instead of aspirational: it recomputes every window the
Spark job produced, asks Postgres for the same window through the code path
the risk worker actually uses, and reports the rows that disagree.

    python -m ledgerflow.spark.parity --gold ./data/gold/account_features

A real feature store runs this on a schedule. Skew is not a thing you prevent
once by writing the window sizes in a shared module -- that only guarantees the
two sides agree about *when* a window starts. They can still disagree about
which rows go in it, which is exactly the failure this found.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Any

from ..adapters.db import read_only


@dataclass(frozen=True, slots=True)
class Disagreement:
    account_id: str
    account_name: str
    account_type: str
    window_start: Any
    window_end: Any
    offline_spend: int
    online_spend: int
    offline_txns: int
    online_txns: int

    @property
    def direction(self) -> str:
        """Which side saw more, which is what identifies the mechanism."""
        return "offline>online" if self.offline_spend > self.online_spend else "online>offline"


def compare(gold_rows: list[dict[str, Any]]) -> list[Disagreement]:
    """One Postgres round trip per window. Fine: this is a batch audit."""
    out: list[Disagreement] = []
    with read_only() as uow:
        accounts = {
            a["id"]: (a["name"], a["type"])
            for a in uow.execute("SELECT id, name, type FROM accounts")
        }
        for row in gold_rows:
            online = uow.risk.spend_window(
                row["account_id"], row["window_start"], row["window_end"]
            )
            if (online["spend_minor"] == row["spend_minor"]
                    and online["txn_count"] == row["txn_count"]):
                continue
            name, type_ = accounts.get(row["account_id"], ("(unknown)", "?"))
            out.append(Disagreement(
                account_id=row["account_id"], account_name=name, account_type=type_,
                window_start=row["window_start"], window_end=row["window_end"],
                offline_spend=row["spend_minor"], online_spend=online["spend_minor"],
                offline_txns=row["txn_count"], online_txns=online["txn_count"],
            ))
    return out


def missing_accounts(gold_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Funding accounts with online spending and no offline rows at all.

    ``compare`` can only audit windows the Spark job produced, so an account it
    never emitted a row for registers zero disagreements -- the check would call
    a totally invisible account clean. This is the other half of the question.

    Restricted to assets and liabilities, which is where money is spent FROM.
    Every posting credits something, so without this filter the report lists
    Revenue:Income and Equity:Opening Balances as "missing" on every run, and a
    check that cries wolf every run is one nobody reads.
    """
    covered = {row["account_id"] for row in gold_rows}
    with read_only() as uow:
        active = uow.execute(
            """
            SELECT a.id, a.name, a.type,
                   COUNT(*)::int AS credit_entries,
                   SUM(e.amount_minor)::bigint AS spend_minor
              FROM accounts a
              JOIN entries e ON e.account_id = a.id AND e.direction = 'credit'
             WHERE a.type IN ('asset', 'liability')
             GROUP BY a.id, a.name, a.type
            """
        )
    return [dict(r) for r in active if r["id"] not in covered]


def report(
    total: int,
    disagreements: list[Disagreement],
    missing: list[dict[str, Any]] | None = None,
) -> str:
    lines = [f"compared {total} windows; {len(disagreements)} disagree "
             f"({len(disagreements) / total:.1%})" if total else "no windows to compare"]
    buckets: dict[tuple[str, str, str], list[Disagreement]] = {}
    for d in disagreements:
        buckets.setdefault((d.account_name, d.account_type, d.direction), []).append(d)
    for (name, type_, direction), items in sorted(buckets.items()):
        worst = max(items, key=lambda d: abs(d.offline_spend - d.online_spend))
        lines += [
            "",
            f"  {name} ({type_}) -- {direction}: {len(items)} windows",
            f"    worst: {worst.window_start} -> {worst.window_end}",
            f"      offline spend={worst.offline_spend:>9} txns={worst.offline_txns}",
            f"      online  spend={worst.online_spend:>9} txns={worst.online_txns}",
        ]
    for acct in missing or []:
        lines += [
            "",
            f"  {acct['name']} ({acct['type']}) -- NO offline rows at all",
            f"    online: {acct['credit_entries']} credit entries, "
            f"spend={acct['spend_minor']}",
        ]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="ledgerflow-spark-parity")
    parser.add_argument("--gold", default="./data/gold/account_features")
    parser.add_argument("--format", default=None, help="delta or parquet")
    args = parser.parse_args(argv)

    from . import session

    spark = session.build("ledgerflow-parity")
    fmt = args.format or session.table_format()
    rows = [r.asDict() for r in spark.read.format(fmt).load(args.gold).collect()]
    spark.stop()

    disagreements = compare(rows)
    missing = missing_accounts(rows)
    print(report(len(rows), disagreements, missing))
    # Nonzero on skew: this is a check, and a check that always passes is not one.
    return 1 if (disagreements or missing) else 0


if __name__ == "__main__":
    raise SystemExit(main())
