"""Postgres -> bronze Parquet/Delta, so the Spark jobs have something to read.

In production this is the Kafka sink writing continuously. For the demo it is a
snapshot export, which keeps the offline pipeline runnable without a broker.
"""

from __future__ import annotations

import json
import pathlib
from typing import Any

from ..adapters.db import read_only


def export_normalized(output_path: str) -> int:
    """Write every normalized transaction as newline-delimited JSON.

    JSON rather than Parquet on purpose: no pyarrow dependency in the API
    process, and Spark reads it with the declared schema either way.
    """
    with read_only() as uow:
        rows = uow.execute(
            """
            SELECT n.id, r.transaction_id, n.raw_transaction_id, n.tenant_id,
                   n.account_id, r.amount_minor, r.currency,
                   r.occurred_at, r.descriptor,
                   n.merchant_id, n.merchant_name, n.category,
                   n.confidence, n.normalizer_version
              FROM normalized_transactions n
              JOIN raw_transactions r ON r.id = n.raw_transaction_id
             ORDER BY r.occurred_at
            """
        )

    path = pathlib.Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as handle:
        for row in rows:
            record: dict[str, Any] = dict(row)
            record["occurred_at"] = record["occurred_at"].isoformat()
            record["confidence"] = float(record["confidence"]) if record["confidence"] else None
            handle.write(json.dumps(record, default=str) + "\n")
    return len(rows)
