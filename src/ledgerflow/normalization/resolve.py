"""Merchant resolution.

Cleaning leaves a string like ``wholefds mkt austin``. Resolution decides which
merchant that is, and how sure we are.

Three stages, each cheap and inspectable:

1. **Candidates** -- trigram similarity against the alias dictionary, indexed
   with pg_trgm. Also run against the leading token prefixes, because a
   trailing city ('austin') drags the whole-string similarity down while the
   first one or two tokens are usually the merchant.
2. **Blend** -- string similarity, the alias's own weight, and a prior: how
   often this tenant already resolved something to that merchant. Frequency is
   real evidence; a customer who buys coffee daily is more likely at the coffee
   shop than at a similarly-spelled place they have never visited.
3. **Threshold** -- below ``MIN_CONFIDENCE`` we return the cleaned string and
   no merchant id.

That last one matters more than it looks. A wrong merchant silently poisons
every category total and every fraud feature keyed on merchant novelty. An
honest "I don't know" is recoverable; a confident wrong answer is not.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from ..adapters.db import UnitOfWork
from .clean import VERSION, clean

MIN_CONFIDENCE = 0.80

# How much the tenant's own history can lift a match. Capped deliberately:
# frequency should break ties, never overturn a poor string match.
_PRIOR_CEILING = 0.15


@dataclass(frozen=True, slots=True)
class Resolution:
    merchant_id: str | None
    merchant_name: str | None
    category: str | None
    confidence: float
    cleaned: str
    version: int = VERSION

    @property
    def resolved(self) -> bool:
        return self.merchant_id is not None


def _prior_boost(count: int) -> float:
    """Diminishing returns: the 50th visit is not 50x the evidence of the 1st."""
    if count <= 0:
        return 0.0
    return min(_PRIOR_CEILING, _PRIOR_CEILING * math.log1p(count) / math.log1p(20))


def resolve(uow: UnitOfWork, tenant_id: str, descriptor: str) -> Resolution:
    cleaned = clean(descriptor)
    if not cleaned:
        return Resolution(None, None, None, 0.0, cleaned)

    # the full string plus leading prefixes; a trailing city should not be able
    # to hide the merchant sitting at the front
    tokens = cleaned.split()
    probes = {cleaned}
    for n in (1, 2, 3):
        if len(tokens) > n:
            probes.add(" ".join(tokens[:n]))

    best: tuple[float, dict] | None = None
    for probe in probes:
        for row in uow.normalization.candidates(probe, limit=5):
            score = float(row["score"]) * float(row["weight"])
            # a probe that is a strict prefix of the full string is slightly
            # less evidence than the whole string matching
            if probe != cleaned:
                score *= 0.97
            if best is None or score > best[0]:
                best = (score, row)

    if best is None:
        return Resolution(None, None, None, 0.0, cleaned)

    score, row = best
    prior = uow.normalization.merchant_prior(tenant_id, row["merchant_id"])
    confidence = min(1.0, score + _prior_boost(prior))

    if confidence < MIN_CONFIDENCE:
        # keep the cleaned string: a human or a later normalizer version can
        # still use it, and the raw row is untouched either way
        return Resolution(None, cleaned, None, round(confidence, 4), cleaned)

    return Resolution(
        merchant_id=row["merchant_id"],
        merchant_name=row["display_name"],
        category=row["category"],
        confidence=round(confidence, 4),
        cleaned=cleaned,
    )
