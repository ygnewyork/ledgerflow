"""Money as a value object.

Integer minor units and a currency. No floats, ever: ``0.1 + 0.2 != 0.3`` in
binary floating point, and a ledger that drifts by a cent per million
transactions is a ledger nobody trusts.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Final

# ISO 4217 exponents for the currencies we support. Expanding this list is a
# deliberate act, not a fallback, because getting the exponent wrong is a
# hundred-fold error.
EXPONENTS: Final[dict[str, int]] = {
    "usd": 2,
    "eur": 2,
    "gbp": 2,
    "jpy": 0,  # yen has no minor unit -- a useful test case
}


class MoneyError(ValueError):
    """Base for every way money can be malformed."""


class CurrencyMismatch(MoneyError):
    """Arithmetic across two currencies. There is no exchange rate here."""


class UnsupportedCurrency(MoneyError):
    pass


class PrecisionError(MoneyError):
    """More precision than the currency has.

    Deliberately an error rather than a rounding: if an upstream system sends
    ``84.375`` USD, that is a schema disagreement between two systems. Silently
    rounding makes the disagreement invisible until reconciliation, months
    later, at which point nobody knows which side was wrong.
    """


@dataclass(frozen=True, slots=True)
class Money:
    minor: int
    currency: str

    def __post_init__(self) -> None:
        # bool is a subclass of int; Money(True, "usd") should not be $0.01
        if type(self.minor) is not int:
            raise MoneyError(
                f"minor must be an int of minor units, got {type(self.minor).__name__}"
            )
        normalized = self.currency.lower()
        if normalized not in EXPONENTS:
            raise UnsupportedCurrency(f"unsupported currency {self.currency!r}")
        object.__setattr__(self, "currency", normalized)

    # -- construction ------------------------------------------------------

    @classmethod
    def parse(cls, amount: str | int | Decimal, currency: str) -> "Money":
        """Build from a human-facing decimal amount, e.g. ``"84.37"``."""
        code = currency.lower()
        if code not in EXPONENTS:
            raise UnsupportedCurrency(f"unsupported currency {currency!r}")
        exponent = EXPONENTS[code]

        if isinstance(amount, float):
            raise PrecisionError(
                "refusing to build Money from a float; pass a string or Decimal"
            )
        try:
            value = Decimal(amount)
        except (InvalidOperation, TypeError) as exc:
            raise MoneyError(f"cannot parse {amount!r} as a decimal amount") from exc

        shifted = value.scaleb(exponent)
        if shifted != shifted.to_integral_value():
            raise PrecisionError(
                f"{value} has more precision than {code} supports "
                f"({exponent} decimal places)"
            )
        return cls(int(shifted), code)

    @classmethod
    def zero(cls, currency: str) -> "Money":
        return cls(0, currency)

    # -- arithmetic --------------------------------------------------------

    def _same(self, other: "Money") -> None:
        if self.currency != other.currency:
            raise CurrencyMismatch(
                f"cannot combine {self.currency} and {other.currency}"
            )

    def __add__(self, other: "Money") -> "Money":
        self._same(other)
        return Money(self.minor + other.minor, self.currency)

    def __sub__(self, other: "Money") -> "Money":
        self._same(other)
        return Money(self.minor - other.minor, self.currency)

    def __neg__(self) -> "Money":
        return Money(-self.minor, self.currency)

    def __abs__(self) -> "Money":
        return Money(abs(self.minor), self.currency)

    def __lt__(self, other: "Money") -> bool:
        self._same(other)
        return self.minor < other.minor

    def __le__(self, other: "Money") -> bool:
        self._same(other)
        return self.minor <= other.minor

    def __gt__(self, other: "Money") -> bool:
        self._same(other)
        return self.minor > other.minor

    def __ge__(self, other: "Money") -> bool:
        self._same(other)
        return self.minor >= other.minor

    # -- predicates --------------------------------------------------------

    @property
    def is_zero(self) -> bool:
        return self.minor == 0

    @property
    def is_positive(self) -> bool:
        return self.minor > 0

    # -- presentation ------------------------------------------------------

    def to_decimal(self) -> Decimal:
        return Decimal(self.minor).scaleb(-EXPONENTS[self.currency])

    def __str__(self) -> str:
        return f"{self.to_decimal()} {self.currency.upper()}"

    def __repr__(self) -> str:
        return f"Money({self.minor}, {self.currency!r})"
