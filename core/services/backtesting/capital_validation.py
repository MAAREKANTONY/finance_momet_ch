from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any


CAPITAL_PER_TICKER_POSITIVE_MESSAGE = "Le capital par action doit être supérieur à zéro."
CAPITAL_TOTAL_AT_LEAST_PER_TICKER_MESSAGE = (
    "Le capital total doit être supérieur ou égal au capital par action, "
    "ou égal à zéro pour un capital global illimité."
)


@dataclass(frozen=True, slots=True)
class CapitalValidationError:
    field: str
    message: str
    code: str


def _to_decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def validate_backtest_capital(capital_total: Any, capital_per_ticker: Any) -> list[CapitalValidationError]:
    capital_total_dec = _to_decimal(capital_total)
    capital_per_ticker_dec = _to_decimal(capital_per_ticker)
    errors: list[CapitalValidationError] = []

    if capital_per_ticker_dec <= 0:
        errors.append(
            CapitalValidationError(
                field="capital_per_ticker",
                message=CAPITAL_PER_TICKER_POSITIVE_MESSAGE,
                code="capital_per_ticker_non_positive",
            )
        )
    if capital_total_dec > 0 and capital_per_ticker_dec > 0 and capital_total_dec < capital_per_ticker_dec:
        errors.append(
            CapitalValidationError(
                field="capital_total",
                message=CAPITAL_TOTAL_AT_LEAST_PER_TICKER_MESSAGE,
                code="capital_total_less_than_capital_per_ticker",
            )
        )
    return errors


def validate_backtest_capital_for_object(obj: Any) -> list[CapitalValidationError]:
    return validate_backtest_capital(
        getattr(obj, "capital_total", None),
        getattr(obj, "capital_per_ticker", None),
    )
