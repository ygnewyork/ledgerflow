"""Load test.

Drives the real HTTP API with real idempotency keys against a real database,
because the interesting number is end-to-end write latency under contention,
not how fast the domain objects can be constructed.

    python -m ledgerflow.bench --url http://localhost:8000 --key lf_test_... \\
        --requests 2000 --concurrency 16

The `--funding-accounts` flag exists to make a specific point. Every card
purchase credits a funding account, and any account with a balance floor takes
a row lock on the write path. Point every request at one funding account and
the ledger serializes on that row -- which is correct, and is the ceiling.
Shard across N accounts and throughput scales until something else binds. The
difference between those two runs is the most informative number this produces.
"""

from __future__ import annotations

import argparse
import json
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import httpx

DESCRIPTORS = [
    "SQ *TST* STARBUCKS 800-782-7282 CA",
    "WHOLEFDS MKT #10234 AUSTIN TX",
    "AMZN Mktp US*2K4LM9XY3",
    "UBER   *TRIP HELP.UBER.COM CA",
    "POS DEBIT CHIPOTLE 1234 03/14",
]


@dataclass
class Results:
    latencies_ms: list[float] = field(default_factory=list)
    statuses: dict[int, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, ms: float, status: int) -> None:
        with self.lock:
            self.latencies_ms.append(ms)
            self.statuses[status] = self.statuses.get(status, 0) + 1

    def fail(self, message: str) -> None:
        with self.lock:
            self.errors.append(message)

    def percentile(self, p: float) -> float:
        if not self.latencies_ms:
            return 0.0
        ordered = sorted(self.latencies_ms)
        # nearest-rank: no interpolation between samples that were never taken
        index = max(0, min(len(ordered) - 1, round(p / 100 * len(ordered)) - 1))
        return ordered[index]


def _worker(
    client: httpx.Client,
    results: Results,
    n: int,
    offset: int,
    funding: list[str],
    run_id: str,
) -> None:
    for i in range(n):
        index = offset + i
        body = {
            "kind": "card_purchase",
            "amount": 100 + (index % 9_900),
            "accounts": {
                "expense": "groceries",
                # round-robin across the funding accounts we were given
                "funding": funding[index % len(funding)],
            },
            "descriptor": DESCRIPTORS[index % len(DESCRIPTORS)],
        }
        started = time.perf_counter()
        try:
            response = client.post(
                "/v1/transactions",
                json=body,
                headers={"Idempotency-Key": f"{run_id}_{index}"},
            )
            elapsed = (time.perf_counter() - started) * 1000
            results.record(elapsed, response.status_code)
            if response.status_code >= 400:
                results.fail(f"{response.status_code}: {response.text[:160]}")
        except Exception as exc:  # noqa: BLE001
            results.record((time.perf_counter() - started) * 1000, 0)
            results.fail(f"{type(exc).__name__}: {exc}")


def run(
    *,
    url: str,
    key: str,
    requests: int,
    concurrency: int,
    funding: list[str],
    label: str = "",
) -> dict:
    results = Results()
    run_id = f"bench{int(time.time() * 1000)}"
    per_worker = requests // concurrency

    clients = [
        httpx.Client(
            base_url=url,
            headers={"Authorization": f"Bearer {key}"},
            timeout=30.0,
            # one connection per worker; pooling across threads would measure
            # the client's queueing rather than the server's
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
        )
        for _ in range(concurrency)
    ]

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for w in range(concurrency):
            pool.submit(
                _worker, clients[w], results, per_worker, w * per_worker, funding, run_id
            )
    elapsed = time.perf_counter() - started

    for client in clients:
        client.close()

    completed = len(results.latencies_ms)
    ok = results.statuses.get(201, 0)
    return {
        "label": label,
        "concurrency": concurrency,
        "funding_accounts": len(funding),
        "requests": completed,
        "succeeded": ok,
        "seconds": round(elapsed, 2),
        "throughput_per_sec": round(ok / elapsed) if elapsed else 0,
        "p50_ms": round(results.percentile(50), 1),
        "p95_ms": round(results.percentile(95), 1),
        "p99_ms": round(results.percentile(99), 1),
        "max_ms": round(max(results.latencies_ms), 1) if results.latencies_ms else 0,
        "statuses": results.statuses,
        "sample_errors": results.errors[:3],
    }


def ensure_funding_accounts(
    api_url: str, key: str, count: int, *, floor: bool = True
) -> list[str]:
    """Create and fund N accounts to shard writes across.

    ``floor=True`` gives each a minimum balance, so they take the same row lock
    the default checking account does. That keeps the sharded run comparable:
    the only variable is how many distinct rows are being locked, not whether
    locking happens at all. ``floor=False`` removes the lock entirely, which
    isolates its cost from the cost of contending for it.
    """
    client = httpx.Client(base_url=api_url, headers={"Authorization": f"Bearer {key}"}, timeout=60)
    suffix = "floor" if floor else "nofloor"
    external_ids: list[str] = []

    for i in range(count):
        external = f"bench_{suffix}_{i}"
        client.post("/v1/accounts", json={
            "name": f"Assets:Bench{suffix.title()}{i}", "type": "asset",
            "currency": "usd", "external_id": external,
            "minimum_balance": 0 if floor else None,
        }, headers={"Idempotency-Key": f"mkacct_{external}"})

        if client.get(f"/v1/accounts/{external}").status_code != 200:
            raise SystemExit(f"could not create or find {external}")

        # a floored account must hold money before anything can be credited
        # from it, or every request fails the overdraft check instead of
        # measuring the write path
        if floor:
            client.post("/v1/transactions", json={
                "kind": "deposit", "amount": 500_000_00,
                "accounts": {"destination": external, "income": "income"},
            }, headers={"Idempotency-Key": f"fund_{external}"})

        external_ids.append(external)

    client.close()
    return external_ids


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ledgerflow-bench")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--key", required=True)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--funding-accounts", type=int, default=1,
                        help="shard writes across N funding accounts (1 = maximum contention)")
    parser.add_argument("--sweep", action="store_true",
                        help="run a concurrency sweep and print a table")
    parser.add_argument("--no-floor", action="store_true",
                        help="use funding accounts with no balance floor (no row lock)")
    args = parser.parse_args(argv)

    if args.no_floor or args.funding_accounts > 1:
        funding = ensure_funding_accounts(
            args.url, args.key, max(1, args.funding_accounts), floor=not args.no_floor
        )
    else:
        funding = ["checking"]

    if not args.sweep:
        print(json.dumps(run(
            url=args.url, key=args.key, requests=args.requests,
            concurrency=args.concurrency, funding=funding,
        ), indent=2))
        return

    rows = []
    for concurrency in (1, 2, 4, 8, 16, 32):
        rows.append(run(
            url=args.url, key=args.key, requests=args.requests,
            concurrency=concurrency, funding=funding,
        ))
        time.sleep(0.5)

    print(f"\n{len(funding)} funding account(s), "
          f"{'no balance floor (no row lock)' if args.no_floor else 'each with a balance floor'}")
    print(f"{'conc':>5} {'txn/s':>8} {'p50':>8} {'p95':>8} {'p99':>8} {'errors':>7}")
    for r in rows:
        errors = r["requests"] - r["succeeded"]
        print(f"{r['concurrency']:>5} {r['throughput_per_sec']:>8} {r['p50_ms']:>8} "
              f"{r['p95_ms']:>8} {r['p99_ms']:>8} {errors:>7}")
        if r["sample_errors"]:
            print(f"      ! {r['sample_errors'][0][:100]}")


if __name__ == "__main__":
    main()
