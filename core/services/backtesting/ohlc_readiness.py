from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Iterable, Mapping

from django.conf import settings
from django.db.models import Max, Min

from core.models import Backtest, DailyBar, Scenario, Symbol


OHLC_READINESS_USER_MESSAGE = (
    "Les prix historiques nécessaires au backtest ne sont pas encore tous disponibles. "
    "Ce backtest utilise un univers dynamique: cliquez sur « Préparer les données OHLC », "
    "puis relancez le backtest lorsque la préparation est terminée. "
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


@dataclass(frozen=True)
class OHLCRequiredRange:
    symbol_id: int
    ticker: str
    start: date
    end: date
    closed_membership: bool = False


def _boundary_tolerance_days() -> int:
    return int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_BOUNDARY_TOLERANCE_DAYS", 3))


def _closed_membership_end_tolerance_days() -> int:
    return int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_CLOSED_MEMBERSHIP_END_TOLERANCE_DAYS", 10))


def _latest_acceptable_start_bar(start: date, end: date) -> date:
    # Coarse guard only: tolerate weekend/holiday boundaries without building a market calendar in Phase 6A.
    return min(end, start + timedelta(days=max(0, _boundary_tolerance_days())))


def _earliest_acceptable_end_bar(start: date, end: date, *, closed_membership: bool = False) -> date:
    tolerance_days = (
        _closed_membership_end_tolerance_days()
        if closed_membership
        else _boundary_tolerance_days()
    )
    return max(start, end - timedelta(days=max(0, tolerance_days)))


def _bars_cover_range(symbol_id: int, start: date, end: date, *, closed_membership: bool = False) -> bool:
    qs = DailyBar.objects.filter(symbol_id=symbol_id, date__gte=start, date__lte=end)
    agg = qs.aggregate(mn=Min("date"), mx=Max("date"))
    return bool(
        agg["mn"]
        and agg["mx"]
        and agg["mn"] <= _latest_acceptable_start_bar(start, end)
        and agg["mx"] >= _earliest_acceptable_end_bar(start, end, closed_membership=closed_membership)
    )


def _missing_symbols(symbols: Iterable[Symbol], start: date, end: date) -> list[Symbol]:
    return [symbol for symbol in symbols if not _bars_cover_range(symbol.id, start, end)]


def _membership_intervals_by_symbol_id(membership_by_ticker) -> dict[int, list]:
    intervals_by_symbol_id: dict[int, list] = {}
    for intervals in (membership_by_ticker or {}).values():
        for interval in intervals:
            symbol_id = getattr(interval, "symbol_id", None)
            if symbol_id is None:
                continue
            intervals_by_symbol_id.setdefault(int(symbol_id), []).append(interval)
    return intervals_by_symbol_id


def get_required_ohlc_ranges_for_dynamic_universe(
    *,
    symbols: Iterable[Symbol],
    start_date: date,
    end_date: date,
    membership_by_ticker=None,
) -> dict[int, list[OHLCRequiredRange]]:
    """Return OHLC ranges required for each symbol in a dynamic universe.

    When memberships are unavailable, the function deliberately falls back to the
    historical global range. That is conservative: it may block, but it cannot
    allow a partial backtest silently.
    """
    scoped_symbols = list(symbols)
    if not membership_by_ticker:
        return {
            symbol.id: [OHLCRequiredRange(symbol_id=symbol.id, ticker=symbol.ticker, start=start_date, end=end_date)]
            for symbol in scoped_symbols
        }

    intervals_by_symbol_id = _membership_intervals_by_symbol_id(membership_by_ticker)
    ranges_by_symbol_id: dict[int, list[OHLCRequiredRange]] = {}
    for symbol in scoped_symbols:
        ranges: list[OHLCRequiredRange] = []
        for interval in intervals_by_symbol_id.get(symbol.id, []):
            effective_start = max(start_date, getattr(interval, "valid_from"))
            raw_valid_to = getattr(interval, "valid_to", None)
            effective_end = min(end_date, raw_valid_to or end_date)
            if effective_end < effective_start:
                continue
            ranges.append(
                OHLCRequiredRange(
                    symbol_id=symbol.id,
                    ticker=symbol.ticker,
                    start=effective_start,
                    end=effective_end,
                    closed_membership=raw_valid_to is not None,
                )
            )
        if not ranges:
            ranges.append(OHLCRequiredRange(symbol_id=symbol.id, ticker=symbol.ticker, start=start_date, end=end_date))
        ranges_by_symbol_id[symbol.id] = ranges
    return ranges_by_symbol_id


def get_missing_ohlc_symbols_for_dynamic_universe(
    *,
    symbols: Iterable[Symbol],
    start_date: date,
    end_date: date,
    membership_by_ticker=None,
) -> list[Symbol]:
    scoped_symbols = list(symbols)
    ranges_by_symbol_id = get_required_ohlc_ranges_for_dynamic_universe(
        symbols=scoped_symbols,
        start_date=start_date,
        end_date=end_date,
        membership_by_ticker=membership_by_ticker,
    )
    missing: list[Symbol] = []
    for symbol in scoped_symbols:
        required_ranges = ranges_by_symbol_id.get(symbol.id, [])
        if not required_ranges or any(
            not _bars_cover_range(
                symbol.id,
                required_range.start,
                required_range.end,
                closed_membership=required_range.closed_membership,
            )
            for required_range in required_ranges
        ):
            missing.append(symbol)
    return missing


def _max_required_days_for_symbols(missing: list[Symbol], ranges_by_symbol_id: Mapping[int, list[OHLCRequiredRange]]) -> int:
    max_days = 0
    for symbol in missing:
        for required_range in ranges_by_symbol_id.get(symbol.id, []):
            max_days = max(max_days, (required_range.end - required_range.start).days + 1)
    return max(1, max_days)


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
    block_on_missing: bool = True,
) -> OHLCReadinessResult:
    """Ensure scoped DailyBar coverage before a dynamic backtest reaches the engine.

    Phase 6A is intentionally a guard, not a hidden preparation step. Missing OHLC data
    must be prepared by an explicit job before launching a dynamic S&P 500 backtest.
    """
    scoped_symbols = list(symbols)
    notes: list[str] = []
    ranges_by_symbol_id = get_required_ohlc_ranges_for_dynamic_universe(
        symbols=scoped_symbols,
        start_date=start_date,
        end_date=end_date,
    )
    is_dynamic_sp500 = (
        getattr(getattr(backtest, "scenario", None), "universe_mode", Scenario.UniverseMode.STATIC_TICKERS)
        == Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
    )
    if is_dynamic_sp500:
        try:
            from core.services.universe_resolver import UniverseResolver

            resolved_universe = UniverseResolver().resolve(
                scenario=backtest.scenario,
                start_date=start_date,
                end_date=end_date,
            )
            ranges_by_symbol_id = get_required_ohlc_ranges_for_dynamic_universe(
                symbols=scoped_symbols,
                start_date=start_date,
                end_date=end_date,
                membership_by_ticker=resolved_universe.membership_by_ticker,
            )
            missing_before = get_missing_ohlc_symbols_for_dynamic_universe(
                symbols=scoped_symbols,
                start_date=start_date,
                end_date=end_date,
                membership_by_ticker=resolved_universe.membership_by_ticker,
            )
        except Exception as exc:
            notes.append(
                "Dynamic OHLC readiness could not resolve membership intervals; "
                f"falling back to global coverage check ({type(exc).__name__})."
            )
            missing_before = _missing_symbols(scoped_symbols, start_date, end_date)
    else:
        missing_before = _missing_symbols(scoped_symbols, start_date, end_date)

    if not missing_before:
        return OHLCReadinessResult(
            ready=True,
            checked_symbols=len(scoped_symbols),
            notes=[*notes, "OHLC coverage already present for the dynamic universe scope."],
        )

    max_symbols = int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_AUTO_FETCH_MAX_SYMBOLS", 25))
    max_days = int(getattr(settings, "DYNAMIC_UNIVERSE_OHLC_AUTO_FETCH_MAX_DAYS", 730))
    requested_days = _max_required_days_for_symbols(missing_before, ranges_by_symbol_id)
    notes.extend([
        f"Missing DailyBar coverage for {len(missing_before)} dynamic universe symbols "
        f"(sample: {', '.join(symbol.ticker for symbol in missing_before[:10])}{'...' if len(missing_before) > 10 else ''})."
    ])
    if len(missing_before) > max_symbols or requested_days > max_days:
        notes.append(
            f"Attention: beaucoup d'actions n'ont pas de prix disponibles ({len(missing_before)} symbols over {requested_days} days)."
        )

    if block_on_missing:
        if len(missing_before) > max_symbols or requested_days > max_days:
            _raise_guardrail_blocked(missing=missing_before, start_date=start_date, end_date=end_date)
        _raise_not_ready(missing_before)

    if allow_fetch:
        notes.append("Dynamic OHLC auto-fetch is disabled; use the dedicated preparation job.")
    missing_tickers = [symbol.ticker for symbol in missing_before]
    ready_count = len(scoped_symbols) - len(missing_before)
    notes.append(
        f"{ready_count} actions sur {len(scoped_symbols)} ont des prix disponibles. "
        f"{len(missing_before)} actions n'ont pas de prix et seront ignorées si vous lancez le backtest. "
        f"Exemples: {', '.join(missing_tickers[:10])}{'...' if len(missing_tickers) > 10 else ''}."
    )
    return OHLCReadinessResult(
        ready=False,
        checked_symbols=len(scoped_symbols),
        missing_before=missing_tickers,
        missing_after=missing_tickers,
        did_fetch=False,
        notes=notes,
    )
