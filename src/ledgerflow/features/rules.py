"""Fraud rules.

Rules, not a model. A model needs labels, and labels need a fraud team; a rule
set can be read, argued with, and unit-tested today. The features are built so
a model can be trained on them later without changing the pipeline.

Almost nothing here blocks a posting. A false positive that declines someone's
groceries is a far worse outcome than a flag a human reviews, so the default is
``action="flag"``: the rule fires after the money moved and only annotates.

The exception earns its place. One rule carries ``action="block"``, evaluated
inside the write transaction by ``application.services._screen`` before any
entry exists, and it sits at a deliberately higher threshold than the advisory
rule that shadows it. Read ``.blocking`` as "this one is allowed to say no",
and note that the two callers below are not interchangeable: ``advisory()`` is
what the post-commit worker may record, because a blocking rule evaluated after
COMMIT would claim a refusal that never happened.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from .compute import Features


@dataclass(frozen=True, slots=True)
class Rule:
    name: str
    score: float
    description: str
    predicate: Callable[[Features], bool]
    #: "flag" runs after the money moved and can only annotate. "block" runs
    #: inside the write transaction and stops the posting. The distinction is
    #: the whole design: a blocking rule buys safety with latency on every
    #: write and with the cost of being wrong in public, so it has to earn it.
    action: str = "flag"

    @property
    def blocking(self) -> bool:
        return self.action == "block"

    def evaluate(self, features: Features) -> bool:
        try:
            return self.predicate(features)
        except ZeroDivisionError:
            return False


RULES: tuple[Rule, ...] = (
    Rule(
        "velocity_1h", 0.6,
        "spend in the last hour far above this account's own baseline",
        lambda f: f.avg_spend_1h_90d > 0 and f.spend_1h > 5 * f.avg_spend_1h_90d,
    ),
    Rule(
        "amount_zscore", 0.5,
        "a single amount far outside this account's 90-day distribution",
        # sample-size guard: with three transactions on file, everything looks
        # like an outlier, and a rule that fires on every new account is noise
        lambda f: f.stddev_amount_90d > 0 and f.amount_zscore > 4,
    ),
    Rule(
        "novel_merchant", 0.3,
        "a large charge at a merchant this account has never used",
        lambda f: f.merchant_frequency == 0 and f.amount_minor > 20_000,
    ),
    Rule(
        "card_testing", 0.8,
        "many tiny charges in a short window -- the signature of a stolen card "
        "being probed for validity",
        lambda f: f.txn_count_1h > 10 and f.max_amount_1h < 500,
    ),
    Rule(
        "card_testing_block", 0.95,
        "a card-testing burst that has gone past the point of doubt: the next "
        "attempt is declined rather than flagged",
        # Deliberately a higher bar than the advisory rule above. The flag
        # fires at 10 and a human looks; blocking waits until 12, because the
        # cost of a false positive here is someone's card declining at a till.
        lambda f: f.txn_count_1h > 12 and f.max_amount_1h < 500,
        action="block",
    ),
    Rule(
        "merchant_sprawl", 0.4,
        "an unusual number of distinct merchants in a week",
        lambda f: f.distinct_merchants_7d > 25,
    ),
)


def evaluate(features: Features) -> list[Rule]:
    """Every rule that fires, advisory and blocking alike."""
    return [rule for rule in RULES if rule.evaluate(features)]


def advisory(features: Features) -> list[Rule]:
    """What the async worker records. It cannot stop anything."""
    return [r for r in RULES if not r.blocking and r.evaluate(features)]


BLOCKING = tuple(r for r in RULES if r.blocking)
