"""Worker entry points."""

from __future__ import annotations

import logging
import time


def run_worker(name: str, *, once: bool = False) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s"
    )
    from . import outbox_relay
    from .normalizer import Normalizer
    from .risk import RiskWorker
    from .runner import run
    from .webhooks import WebhookFanout, dispatch_once

    if name == "relay":
        outbox_relay.run(once=once)
    elif name == "normalizer":
        run(Normalizer(), once=once)
    elif name == "risk":
        run(RiskWorker(), once=once)
    elif name == "webhooks":
        run(WebhookFanout(), once=once)
        dispatch_once()
    elif name == "all":
        # single-process pipeline, for the demo and the test suite. in
        # production each of these is its own deployment, scaled separately --
        # that independence is most of the reason the stream exists.
        drain_all(verbose=True)
    else:
        raise SystemExit(f"unknown worker {name!r}")


def drain_all(verbose: bool = False) -> dict[str, int]:
    """Run the whole pipeline to quiescence. Used by the demo and the tests."""
    from . import outbox_relay
    from .normalizer import Normalizer
    from .risk import RiskWorker
    from .runner import run
    from .webhooks import WebhookFanout, dispatch_once

    counts = {"relayed": 0, "normalized": 0, "scored": 0, "fanned_out": 0}

    # ordered by dependency: the relay feeds the normalizer, which feeds risk
    for _ in range(3):  # a couple of passes, since each stage feeds the next
        counts["relayed"] += outbox_relay.run(once=True)
        counts["fanned_out"] += run(WebhookFanout(), once=True)
        counts["normalized"] += run(Normalizer(), once=True)
        counts["scored"] += run(RiskWorker(), once=True)

    # keep the balance cache current, so reads stay bounded as history grows
    from ..adapters.db import unit_of_work

    with unit_of_work() as uow:
        for row in uow.execute("SELECT DISTINCT account_id FROM entries"):
            uow.accounts.write_snapshot(row["account_id"])

    counts.update(dispatch_once())
    if verbose:
        print(counts)
    return counts
