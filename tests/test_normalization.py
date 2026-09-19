"""Descriptor cleaning and merchant resolution, against a golden set.

Precision over recall, deliberately: a wrong merchant silently poisons every
category total and every fraud feature keyed on merchant novelty, while an
honest "I don't know" is recoverable.
"""

from __future__ import annotations

import pytest

from ledgerflow.adapters.db import read_only
from ledgerflow.normalization.clean import clean
from ledgerflow.normalization.resolve import MIN_CONFIDENCE, resolve

# (raw descriptor, expected merchant display name or None)
GOLDEN = [
    ("SQ *TST* STARBUCKS 800-782-7282 CA", "Starbucks"),
    ("STARBUCKS #04212", "Starbucks"),
    ("STARBUCKS MOBILE 04212", "Starbucks"),
    ("WHOLEFDS MKT #10234 AUSTIN TX", "Whole Foods"),
    ("WHOLE FOODS MARKET #442", "Whole Foods"),
    ("H-E-B #123 AUSTIN TX", "H-E-B"),
    ("HEB GROCERY #5501", "H-E-B"),
    ("AMZN Mktp US*2K4LM9XY3", "Amazon"),
    ("AMAZON.COM*RT4YU8 AMZN.COM/BILL WA", "Amazon"),
    ("AMAZON PRIME*2L9KD MEMBERSHIP", "Amazon"),
    ("UBER   *TRIP HELP.UBER.COM CA", "Uber"),
    ("LYFT   *RIDE THU 3PM", "Lyft"),
    ("SHELL OIL 57445201907 HOUSTON TX", "Shell"),
    ("CHEVRON 00201234 AUSTIN TX", "Chevron"),
    ("NETFLIX.COM 866-579-7172 CA", "Netflix"),
    ("SPOTIFY USA 8887771111 NY", "Spotify"),
    ("POS DEBIT CHIPOTLE 1234 03/14", "Chipotle"),
    ("CVS/PHARMACY #08234", "CVS"),
    ("WALGREENS #4412 AUSTIN TX", "Walgreens"),
    ("WAL-MART SUPERCENTER #2201", "Walmart"),
    ("TARGET        T-2245", "Target"),
    ("TRADER JOE S #445 AUSTIN TX", "Trader Joe's"),
    ("MCDONALD'S F1234", "McDonald's"),
    ("COMCAST CABLE COMM 800-COMCAST", "Comcast"),
    ("MARRIOTT HOTELS 8882367687 MD", "Marriott"),
    # the tail every real ingest has -- must resolve to nothing, not to a guess
    ("ZZQQ UNKNOWN VENDOR 99", None),
    ("PY *LOCAL FARMERS MKT", None),
    ("ZELLE TO J SMITH 20260817", None),
]


@pytest.mark.parametrize("descriptor,expected", GOLDEN, ids=[g[0][:28] for g in GOLDEN])
def test_golden_set(tenant, descriptor, expected):
    with read_only() as uow:
        result = resolve(uow, tenant["tenant_id"], descriptor)
    assert result.merchant_name if expected else True
    if expected is None:
        assert not result.resolved, (
            f"{descriptor!r} resolved to {result.merchant_name!r} at "
            f"{result.confidence} -- a confident wrong answer is worse than none"
        )
    else:
        assert result.merchant_name == expected, f"{descriptor!r} -> {result.merchant_name!r}"
        assert result.confidence >= MIN_CONFIDENCE


def test_precision_and_recall_on_the_golden_set(tenant):
    """The number to watch when shipping a new normalizer version."""
    resolvable = [(d, e) for d, e in GOLDEN if e is not None]
    correct = wrong = missed = 0

    with read_only() as uow:
        for descriptor, expected in resolvable:
            result = resolve(uow, tenant["tenant_id"], descriptor)
            if not result.resolved:
                missed += 1
            elif result.merchant_name == expected:
                correct += 1
            else:
                wrong += 1

    precision = correct / (correct + wrong) if (correct + wrong) else 0.0
    recall = correct / len(resolvable)
    print(f"\nprecision {precision:.3f}  recall {recall:.3f}  "
          f"(correct {correct}, wrong {wrong}, missed {missed})")

    assert precision == 1.0, f"{wrong} descriptors resolved to the wrong merchant"
    assert recall >= 0.90


def test_cleaning_strips_noise_but_keeps_the_merchant():
    assert clean("SQ *TST* STARBUCKS 800-782-7282 CA") == "starbucks"
    assert clean("POS DEBIT CHIPOTLE 1234 03/14") == "chipotle"
    # a domain label is the merchant name; deleting the whole token loses it
    assert "netflix" in clean("NETFLIX.COM 866-579-7172 CA")
    # 'amzn mktp' is a merchant, not a processor prefix
    assert "amzn" in clean("AMZN Mktp US*2K4LM9XY3")


def test_a_trailing_state_code_goes_but_a_leading_one_stays():
    assert clean("STARBUCKS CA") == "starbucks"
    assert clean("CA PIZZA KITCHEN 220") == "ca pizza kitchen"


def test_unresolvable_descriptors_keep_their_cleaned_form(tenant):
    """Below threshold we still hand back something usable."""
    with read_only() as uow:
        result = resolve(uow, tenant["tenant_id"], "ZZQQ UNKNOWN VENDOR 99")
    assert not result.resolved
    assert result.cleaned
    assert result.merchant_id is None
