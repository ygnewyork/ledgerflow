"""Fraud rules.

Rules, not a model. A model needs labels, and labels need a fraud team; a rule
set can be read, argued with, and unit-tested today. The features are built so
a model can be trained on them later without changing the pipeline.

Nothing here blocks a posting. Signals are emitted and surfaced. A false
positive that declines someone's groceries is a far worse outcome than a flag a
human reviews -- and the demo is more interesting when the signal stream runs
alongside the money stream instead of stopping it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .compute import Features


@dataclass(frozen=True, slots=True)
class Rule:
    name: str
    score: float
    description: str
    predicate: Callable[[Features], bool]

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
        "merchant_sprawl", 0.4,
        "an unusual number of distinct merchants in a week",
        lambda f: f.distinct_merchants_7d > 25,
    ),
)


def evaluate(features: Features) -> list[Rule]:
    return [rule for rule in RULES if rule.evaluate(features)]
