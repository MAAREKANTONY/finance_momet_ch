from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Iterable

from django.conf import settings
from django.db.models import Max, Min

from core.models import Backtest, DailyBar, Symbol


OHLC_READINESS_USER_MESSAGE = (
    "Les prix historiques nécessaires au backtest ne sont pas encore tous disponibles. "
    "Lancez la préparation OHLC dynamique dédiée avant de relancer. "
    "Le backtest n’est pas lancé pour éviter un résultat partiel."
)

OHLC_READINESS_TOO_MANY_MISSING_MESSAGE = (
    "Les prix historiques de trop nombreux symboles doivent être synchronisés avant ce backtest. "
    "Lancez une synchronisation des prix depuis l’administration ou réduisez la période. "
    "Le backtest n’est pas lancé pour éviter un résultat partiel."
)


class OHLCReadinessError(RuntimeError):
    def __init__(self, message: str = OHLC_READINESS_USER_MESSAGE, *, missing_tickers: list[str] | None = None):
        super().__init__(message)
        self.missing_tickers = missing_tickers or []


@dataclass
class OHLCReadinessResult:
    ready: bool
    checked_symbols: int
    missing_before: list[str] = field(default_factory=list)
    missing_after: list[str] = field(default_factory=list)
    did_fetch: bool = False
    fetched_bars: int = 0
    notes: list[str] = field(default_factory=list)


def _boundary_tolerance_days() -> int:
    return int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_BOUNDARY_TOLERANCE_DAYS", 3))


def _latest_acceptable_start_bar(start: date, end: date) -> date:
    # Coarse guard only: tolerate weekend/holiday boundaries without building a market calendar in Phase 6A.
    return min(end, start + timedelta(days=max(0, _boundary_tolerance_days())))


def _earliest_acceptable_end_bar(start: date, end: date) -> date:
    return max(start, end - timedelta(days=max(0, _boundary_tolerance_days())))


def _bars_cover_range(symbol_id: int, start: date, end: date) -> bool:
    qs = DailyBar.objects.filter(symbol_id=symbol_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(
        agg["mn"]
        and agg["mx"]
        and agg["mn"] <= _latest_acceptable_start_bar(start, end)
        and agg["mx"] >= _earliest_acceptable_end_bar(start, end)
    )


def _missing_symbols(symbols: Iterable[Symbol], start: date, end: date) -> list[Symbol]:
    return [symbol for symbol in symbols if not _bars_cover_range(symbol.id, start, end)]


def _raise_not_ready(missing: list[Symbol]) -> None:
    tickers = [symbol.ticker for symbol in missing]
    examples = ", ".join(tickers[:10])
    detail = f" Tickers incomplets: {examples}{'...' if len(tickers) > 10 else ''}."
    raise OHLCReadinessError(f"{OHLC_READINESS_USER_MESSAGE}{detail}", missing_tickers=tickers)


def _raise_guardrail_blocked(*, missing: list[Symbol], start_date: date, end_date: date) -> None:
    tickers = [symbol.ticker for symbol in missing]
    examples = ", ".join(tickers[:10])
    detail = (
        f" Symboles manquants: {len(tickers)}. "
        f"Période: {start_date.isoformat()} → {end_date.isoformat()}. "
        f"Exemples: {examples}{'...' if len(tickers) > 10 else ''}."
    )
    raise OHLCReadinessError(f"{OHLC_READINESS_TOO_MANY_MISSING_MESSAGE}{detail}", missing_tickers=tickers)


def ensure_ohlc_ready_for_backtest(
    *,
    backtest: Backtest,
    symbols,
    start_date: date,
    end_date: date,
    allow_fetch: bool = False,
) -> OHLCReadinessResult:
    """Ensure scoped DailyBar coverage before a dynamic backtest reaches the engine.

    Phase 6A is intentionally a guard, not a hidden preparation step. Missing OHLC data
    must be prepared by an explicit job before launching a dynamic S&P 500 backtest.
    """
    scoped_symbols = list(symbols)
    missing_before = _missing_symbols(scoped_symbols, start_date, end_date)
    if not missing_before:
        return OHLCReadinessResult(
            ready=True,
            checked_symbols=len(scoped_symbols),
            notes=["OHLC coverage already present for the dynamic universe scope."],
        )

    max_symbols = int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_AUTO_FETCH_MAX_SYMBOLS", 25))
    max_days = int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_AUTO_FETCH_MAX_DAYS", 730))
    requested_days = max(1, (end_date - start_date).days + 1)
    notes = [
        f"Missing DailyBar coverage for {len(missing_before)} dynamic universe symbols "
        f"(sample: {', '.join(symbol.ticker for symbol in missing_before[:10])}{'...' if len(missing_before) > 10 else ''})."
    ]
    if len(missing_before) > max_symbols or requested_days > max_days:
        notes.append(
            f"Scoped OHLC readiness blocked: {len(missing_before)} symbols over {requested_days} days exceeds guardrails."
        )
        _raise_guardrail_blocked(missing=missing_before, start_date=start_date, end_date=end_date)

    if allow_fetch:
        notes.append("Dynamic OHLC auto-fetch is disabled in Phase 6A; use the dedicated preparation job.")
    _raise_not_ready(missing_before)
