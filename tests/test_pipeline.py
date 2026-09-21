"""Outbox, stream delivery, dedupe, dead letters, and replay."""

from __future__ import annotations

from ledgerflow import ids
from ledgerflow.adapters.db import read_only
from ledgerflow.stream import LEDGER_EVENTS, get_stream
from ledgerflow.workers import drain_all, outbox_relay
from ledgerflow.workers.outbox_relay import relay_once
from ledgerflow.workers.runner import Consumer, PoisonMessage, process_one, run


def _purchase(client, key: str | None = None, **overrides):
    body = {
        "kind": "card_purchase", "amount": 8437,
        "accounts": {"expense": "groceries", "funding": "checking"},
        "descriptor": "SQ *TST* STARBUCKS 800-782-7282 CA",
    }
    body.update(overrides)
    return client.post(
        "/v1/transactions", headers={"Idempotency-Key": key or ids.new_id("k")}, json=body
    )


def test_the_event_is_written_with_the_ledger_not_after_it(funded, client):
    """The outbox row must exist the moment the transaction does."""
    response = _purchase(client)
    txn_id = response.json()["id"]

    with read_only() as uow:
        events = uow.events.list(tenant_id=funded["tenant_id"], mode="test", limit=50)

    payloads = [e["payload"]["data"]["object"]["id"] for e in events]
    assert txn_id in payloads, "the ledger committed without its event"

    # and it is unpublished: writing the row is the API's job, publishing is the
    # relay's. the split is what makes the write path independent of the broker.
    unpublished = [e for e in events if e["published_at"] is None]
    assert unpublished, "events should be pending until the relay runs"


def test_relay_publishes_then_marks_sent(funded, client):
    txn_id = _purchase(client).json()["id"]

    # Drain, rather than publishing a single batch. relay_once() takes the
    # OLDEST 500 unpublished rows, so with any backlog ahead of it this
    # tenant's event simply is not in the first batch -- which is correct
    # relay behaviour and a broken assumption for a test to hold.
    published = outbox_relay.run(once=True)
    assert published >= 1

    # Scoped to this tenant's event, not the global outbox. outbox_lag() spans
    # every tenant, so asserting it reaches zero makes this test depend on no
    # other test having written anything -- which is a property a test suite
    # should never be asked to have.
    with read_only() as uow:
        mine = [
            e for e in uow.events.list(
                tenant_id=funded["tenant_id"], mode="test", limit=100)
            if e["payload"]["data"]["object"]["id"] == txn_id
        ]
    assert mine, "the transaction produced no event"
    assert all(e["published_at"] is not None for e in mine), "the relay left it unpublished"


def test_redelivery_is_absorbed_by_the_dedupe_claim(funded, client):
    """At-least-once delivery plus the claim equals effectively-once processing."""
    _purchase(client)
    drain_all()

    with read_only() as uow:
        before = uow.execute("SELECT count(*) n FROM normalized_transactions")[0]["n"]

    # rewind the normalizer and let every event arrive a second time
    stream = get_stream()
    stream.seek(topic=LEDGER_EVENTS, consumer_group="normalizer", offset=0)
    drain_all()

    with read_only() as uow:
        after = uow.execute("SELECT count(*) n FROM normalized_transactions")[0]["n"]

    assert after == before, "redelivery created duplicate normalized rows"


class _AlwaysPoison(Consumer):
    topic = LEDGER_EVENTS

    def __init__(self):
        # a unique group per instance: dead letters and offsets are keyed by
        # consumer group, so two tests sharing a name see each other's rows
        self.group = ids.new_id("grp_poison")

    def handle(self, uow, message):
        raise PoisonMessage("this payload will never be processable")


class _AlwaysTransient(Consumer):
    topic = LEDGER_EVENTS

    def __init__(self):
        self.group = ids.new_id("grp_transient")
        self.attempts = 0

    def handle(self, uow, message):
        self.attempts += 1
        raise TimeoutError("the database is having a moment")


def test_a_poison_message_is_dead_lettered_not_retried(funded, client):
    _purchase(client)
    relay_once()

    consumer = _AlwaysPoison()
    stream = get_stream()
    message = stream.poll(topic=LEDGER_EVENTS, consumer_group=consumer.group, limit=1)[0]

    assert process_one(consumer, message) is True, "the offset must still advance"

    with read_only() as uow:
        letters = uow.events.list_dead_letters(consumer_group=consumer.group)
    assert len(letters) == 1
    assert letters[0]["error_class"] == "PoisonMessage"
    # retrying a poison message is burning cycles before the inevitable
    assert letters[0]["attempts"] == 1


def test_a_transient_failure_is_retried_then_dead_lettered(funded, client):
    _purchase(client)
    relay_once()

    consumer = _AlwaysTransient()
    stream = get_stream()
    message = stream.poll(topic=LEDGER_EVENTS, consumer_group=consumer.group, limit=1)[0]

    process_one(consumer, message)

    assert consumer.attempts == 3, "should retry up to consumer_max_attempts"
    with read_only() as uow:
        letters = uow.events.list_dead_letters(consumer_group=consumer.group)
    assert letters[0]["error_class"] == "TimeoutError"


def test_a_poison_message_does_not_stall_the_topic(funded, client):
    """One bad message must not block every account behind it."""
    for _ in range(3):
        _purchase(client)
    relay_once()

    consumer = _AlwaysPoison()
    processed = run(consumer, once=True)

    assert processed >= 3, "the consumer stopped at the first failure"
    stream = get_stream()
    assert stream.offset(topic=LEDGER_EVENTS, consumer_group=consumer.group) > 0


def test_replay_clears_claims_so_history_reprocesses(funded, client):
    _purchase(client)
    drain_all()

    with read_only() as uow:
        claims_before = uow.execute(
            "SELECT count(*) n FROM processed_events WHERE consumer_group = 'normalizer'"
        )[0]["n"]
    assert claims_before > 0

    response = client.post("/v1/replays", json={
        "consumer_group": "normalizer", "topic": LEDGER_EVENTS,
        "from_offset": 0, "reason": "normalizer v4 evaluation",
    })
    assert response.status_code == 202

    with read_only() as uow:
        claims_after = uow.execute(
            "SELECT count(*) n FROM processed_events WHERE consumer_group = 'normalizer'"
        )[0]["n"]
    assert claims_after < claims_before, "replay must drop the dedupe claims it rewinds past"


def test_reconciliation_and_global_drift_stay_clean(funded, client):
    for _ in range(5):
        _purchase(client)
    drain_all()

    result = client.post("/v1/reconcile").json()
    assert result["ledger_drift"] == [], "money was created or destroyed"
    assert result["mismatches"] == []
    assert result["ok"] is True
