"""Descriptor cleaning.

Card descriptors are a dumping ground: a payment processor prefix, the
merchant, a store number, a phone number, a city, a state, sometimes a date,
all crammed into 22-40 characters by systems that predate everyone reading
this.

    SQ *TST* STARBUCKS 800-782-7282 CA   ->  starbucks
    STARBUCKS #04212                     ->  starbucks
    AMZN Mktp US*2K4LM9XY3                ->  amzn mktp us

The rules are deterministic and individually testable. No model: a model that
is right 95% of the time and inscrutable when wrong is worse here than rules
that are right 90% of the time and can be read.

``VERSION`` is stored on every normalized row. Bump it, replay the stream, and
diff the two versions on identical inputs before trusting the new logic.
"""

from __future__ import annotations

import re

VERSION = 3

# Processor and network prefixes. Ordered longest-first so 'SQ *TST*' is
# consumed before 'SQ *' can match half of it.
_PREFIXES = [
    "debit card purchase", "recurring payment authorised on", "purchase authorized on",
    "pos debit", "pos purchase", "checkcard", "visa purchase", "ach debit",
    "sq *tst*", "tst* ", "sq *", "sp ", "py *", "pp*", "paypal *", "pmnt*",
    "dd *", "gp *", "in *", "tst*", "wl *",
]
# NOT prefixes: 'amzn mktp', 'msft', 'dd doordash'. Those are merchant names --
# stripping them leaves the noise and throws away the only useful token.

_SUFFIX_NOISE = [
    "recurring", "purchase", "payment", "card purchase", "pending",
]

# Everything below is noise no merchant name ever needs.
_PATTERNS = [
    # phone numbers in every shape a descriptor uses
    re.compile(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    re.compile(r"\b8\d{2}[-.\s]?\d{3}[-.\s]?\d{4}\b"),
    # store / terminal numbers
    re.compile(r"#\s*\d+"),
    re.compile(r"\bstore\s*\d+\b"),
    re.compile(r"\bterm(?:inal)?\s*\d+\b"),
    # dates embedded mid-string
    re.compile(r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b"),
    # NOTE: domains are reduced, not deleted. 'netflix.com' must become
    # 'netflix', not vanish -- the domain label is usually the merchant name.
    None,
    # long alphanumeric order ids: 2K4LM9XY3, XJ8821LKD
    re.compile(r"\b(?=[a-z0-9]*\d)(?=[a-z0-9]*[a-z])[a-z0-9]{6,}\b"),
    # bare digit runs
    re.compile(r"\b\d{3,}\b"),
    # short letter-prefixed store codes: F1234, T2245, BK0098
    re.compile(r"\b[a-z]{1,2}\d{3,}\b"),
]

# Trailing state codes. Only stripped at the END, so 'CA Pizza Kitchen' keeps
# its CA and 'STARBUCKS CA' loses it.
_STATES = (
    "al ak az ar ca co ct de fl ga hi id il in ia ks ky la me md ma mi mn ms "
    "mo mt ne nv nh nj nm ny nc nd oh ok or pa ri sc sd tn tx ut vt va wa wv wi wy dc"
).split()

_DOMAIN = re.compile(r"(?:https?://)?(?:www\.)?\b([a-z0-9-]+)\.(?:com|net|org|co|io)\b")
_PUNCT = re.compile(r"[*#,;:_/\\|]+")
# hyphens become spaces so 'h-e-b' and 'wal-mart' reach their aliases.
# apostrophes are dropped outright, not spaced: "mcdonald's" must collapse to
# "mcdonalds" to reach its alias, and "mcdonald s" would not.
_APOSTROPHE = re.compile(r"['’]")
_NONWORD = re.compile(r"[^a-z0-9&\s]")
_SPACES = re.compile(r"\s+")


def clean(descriptor: str) -> str:
    """Reduce a raw descriptor to the merchant tokens, or '' if nothing survives."""
    text = descriptor.lower().strip()

    for prefix in _PREFIXES:
        if text.startswith(prefix):
            text = text[len(prefix):].strip()
            break

    # keep the domain label, drop the scheme and TLD: netflix.com -> netflix
    text = _DOMAIN.sub(r"\1 ", text)

    for pattern in _PATTERNS:
        if pattern is not None:
            text = pattern.sub(" ", text)

    text = _APOSTROPHE.sub("", text)
    text = _PUNCT.sub(" ", text)
    text = _NONWORD.sub(" ", text)
    text = _SPACES.sub(" ", text).strip()

    tokens = text.split()
    # trailing state code, then trailing noise words, repeatedly: a descriptor
    # can end '... purchase ca' and both should go
    changed = True
    while changed and tokens:
        changed = False
        if tokens[-1] in _STATES and len(tokens) > 1:
            tokens.pop()
            changed = True
        for noise in _SUFFIX_NOISE:
            parts = noise.split()
            if len(tokens) > len(parts) and tokens[-len(parts):] == parts:
                del tokens[-len(parts):]
                changed = True

    # A trailing single letter is the remains of a stripped store code
    # ("target t" from "TARGET T-2245"). Only drop it when the token before it
    # is longer -- otherwise "h e b" loses its last letter and stops matching.
    if len(tokens) >= 2 and len(tokens[-1]) == 1 and len(tokens[-2]) > 1:
        tokens.pop()

    return " ".join(tokens).strip()
