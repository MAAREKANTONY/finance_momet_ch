from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.models import Backtest, Scenario
from core.services.china_benchmark_registry import CSI300_MARKET_BENCHMARK


EFFECTIVE_CURRENCY_SETTINGS_KEY = "effective_currency"
CSI300_EFFECTIVE_CURRENCY = "CNY"


def effective_currency_for_universe_mode(universe_mode: Any) -> str:
    if str(universe_mode or "").strip() == Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC:
        return CSI300_EFFECTIVE_CURRENCY
    return ""


def effective_currency_for_new_result(universe_meta: Any) -> str:
    if not isinstance(universe_meta, dict):
        return ""
    return effective_currency_for_universe_mode(universe_meta.get("mode"))


def _persisted_effective_currency(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip().upper()
    return normalized if normalized == CSI300_EFFECTIVE_CURRENCY else ""


def effective_currency_for_backtest_display(backtest: Any) -> str:
    results = getattr(backtest, "results", None)
    if isinstance(results, dict) and results:
        meta = results.get("meta")
        if isinstance(meta, dict):
            persisted = _persisted_effective_currency(meta.get(EFFECTIVE_CURRENCY_SETTINGS_KEY))
            if persisted:
                return persisted
            universe = meta.get("universe")
            if isinstance(universe, dict):
                historical = effective_currency_for_universe_mode(universe.get("mode"))
                if historical:
                    return historical
        return ""

    if results not in (None, {}):
        return ""

    if getattr(backtest, "status", None) not in {
        Backtest.Status.PENDING,
        Backtest.Status.RUNNING,
    }:
        return ""

    settings = getattr(backtest, "settings", None)
    if isinstance(settings, dict):
        persisted = _persisted_effective_currency(settings.get(EFFECTIVE_CURRENCY_SETTINGS_KEY))
        if persisted:
            return persisted
    return ""


@dataclass(frozen=True)
class ResolvedCurrencyValidation:
    expected_currency: str
    symbol_count: int
    invalid_symbol_count: int
    missing_currency_count: int
    actual_currencies: tuple[str, ...]
    invalid_examples: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return self.invalid_symbol_count == 0

    def error_message(self) -> str:
        return (
            f"Devise CSI300 invalide pour {self.invalid_symbol_count} symbole(s) "
            f"sur {self.symbol_count} : devise attendue {self.expected_currency}, "
            f"devises trouvées {', '.join(self.actual_currencies)}. "
            f"Exemples : {', '.join(self.invalid_examples)}."
        )


class ResolvedCurrencyValidationError(ValueError):
    pass


@dataclass(frozen=True)
class BenchmarkCurrencyValidation:
    provider_symbol: str
    symbol_present: bool
    symbol_id: int | None
    expected_currency: str
    actual_currency: str

    @property
    def valid(self) -> bool:
        return self.symbol_present and self.actual_currency == self.expected_currency

    def error_message(self) -> str:
        return (
            f"Devise du benchmark {self.provider_symbol} invalide : "
            f"devise attendue {self.expected_currency}, "
            f"devise trouvée {self.actual_currency or 'absente'}."
        )


def resolved_currency_validation(universe_mode: Any, symbols: Any) -> ResolvedCurrencyValidation | None:
    expected_currency = effective_currency_for_universe_mode(universe_mode)
    if not expected_currency:
        return None

    symbol_list = list(symbols or [])
    invalid: list[tuple[Any, str]] = []
    for symbol in symbol_list:
        currency = str(getattr(symbol, "currency", "") or "").strip().upper()
        if currency != expected_currency:
            invalid.append((symbol, currency))

    actual_currencies = tuple(sorted({currency or "(absente)" for _symbol, currency in invalid}))
    examples = tuple(
        f"{getattr(symbol, 'ticker', '')}"
        f"{(':' + str(getattr(symbol, 'exchange', ''))) if getattr(symbol, 'exchange', '') else ''}="
        f"{currency or '(absente)'}"
        for symbol, currency in invalid[:10]
    )
    return ResolvedCurrencyValidation(
        expected_currency=expected_currency,
        symbol_count=len(symbol_list),
        invalid_symbol_count=len(invalid),
        missing_currency_count=sum(1 for _symbol, currency in invalid if not currency),
        actual_currencies=actual_currencies or (expected_currency,),
        invalid_examples=examples,
    )


def validate_resolved_universe_currency(resolved_universe: Any) -> ResolvedCurrencyValidation | None:
    if resolved_universe is None:
        return None
    validation = resolved_currency_validation(
        getattr(resolved_universe, "mode", ""),
        getattr(resolved_universe, "symbols", ()),
    )
    if validation is not None and not validation.valid:
        raise ResolvedCurrencyValidationError(validation.error_message())
    return validation


def csi300_market_benchmark_currency_validation(benchmark_symbol: Any) -> BenchmarkCurrencyValidation:
    provider_symbol = str(CSI300_MARKET_BENCHMARK.provider_symbol or "000300.SHG")
    symbol_present = benchmark_symbol is not None
    currency = (
        str(getattr(benchmark_symbol, "currency", "") or "").strip().upper()
        if symbol_present
        else ""
    )
    return BenchmarkCurrencyValidation(
        provider_symbol=provider_symbol,
        symbol_present=symbol_present,
        symbol_id=getattr(benchmark_symbol, "id", None) if symbol_present else None,
        expected_currency=CSI300_EFFECTIVE_CURRENCY,
        actual_currency=currency,
    )


def validate_csi300_market_benchmark_currency(benchmark_symbol: Any) -> BenchmarkCurrencyValidation:
    validation = csi300_market_benchmark_currency_validation(benchmark_symbol)
    if not validation.valid:
        raise ResolvedCurrencyValidationError(validation.error_message())
    return validation
