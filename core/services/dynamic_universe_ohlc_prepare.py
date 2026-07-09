from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Callable

from django.db import transaction

from core.models import Backtest, DailyBar, Scenario, Symbol
from core.services.backtesting.ohlc_readiness import (
    OHLCRequiredRange,
    get_missing_ohlc_symbols_for_dynamic_universe,
    get_required_ohlc_ranges_for_dynamic_universe,
)
from core.services.provider_eodhd import (
    EODHDClient,
    EODHDError,
    UnsupportedEODHDSymbolError,
    sanitize_provider_error_message,
    to_eodhd_symbol,
)
from core.services.universe_resolver import (
    CSI300_UNIVERSE_CODE,
    SP500_UNIVERSE_CODE,
    UniverseResolver,
    is_historical_dynamic_universe_mode,
    universe_code_for_historical_dynamic_mode,
)


class DynamicUniverseOHLCPrepareError(RuntimeError):
    pass


@dataclass
class DynamicUniverseOHLCPrepareResult:
    checked_symbols: int
    ready_before: int
    missing_before: list[str] = field(default_factory=list)
    fetched_symbols: list[str] = field(default_factory=list)
    inserted_bars: int = 0
    updated_bars: int = 0
    unchanged_bars: int = 0
    no_data_symbols: list[str] = field(default_factory=list)
    provider_error_symbols: dict[str, str] = field(default_factory=dict)
    network_error_symbols: dict[str, str] = field(default_factory=dict)
    skipped_symbols: dict[str, str] = field(default_factory=dict)
    ready_after: int = 0
    missing_after: list[str] = field(default_factory=list)
    provider: str = "eodhd"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _parse_date_value(value: date | str | None, *, name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "")).date()
    except ValueError as exc:
        raise DynamicUniverseOHLCPrepareError(f"Invalid {name}: {value}") from exc


def _is_network_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "connection",
            "dns",
            "resolve",
            "resolved",
            "timed out",
            "timeout",
            "network",
            "temporary failure",
        )
    )


def _coverage_start(backtest: Backtest | None, start_date: date) -> date:
    warmup_days = int(getattr(backtest, "warmup_days", 0) or 0) if backtest else 0
    if warmup_days > 0:
        return start_date - timedelta(days=warmup_days)
    return start_date


def _resolve_scope(
    *,
    universe_code: str,
    start_date: date | str | None,
    end_date: date | str | None,
    backtest_id: int | None,
    scenario_id: int | None,
) -> tuple[Any, date, date, date]:
    backtest: Backtest | None = None
    scenario: Scenario | Any | None = None

    if backtest_id:
        backtest = Backtest.objects.select_related("scenario").get(id=backtest_id)
        scenario = backtest.scenario
    elif scenario_id:
        scenario = Scenario.objects.get(id=scenario_id)
    else:
        requested_universe = str(universe_code or "").upper()
        if requested_universe == CSI300_UNIVERSE_CODE:
            raise DynamicUniverseOHLCPrepareError(
                "Préparation OHLC automatique non disponible pour CSI300 V1. "
                "Importez/préparez les OHLC séparément."
            )
        if requested_universe != SP500_UNIVERSE_CODE:
            raise DynamicUniverseOHLCPrepareError(f"Unsupported dynamic universe: {universe_code}")
        scenario = SimpleNamespace(universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)

    mode = getattr(scenario, "universe_mode", Scenario.UniverseMode.STATIC_TICKERS)
    if mode != Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC:
        if is_historical_dynamic_universe_mode(mode):
            resolved_code = universe_code_for_historical_dynamic_mode(mode) or str(universe_code or "").upper()
            if resolved_code == CSI300_UNIVERSE_CODE:
                raise DynamicUniverseOHLCPrepareError(
                    "Préparation OHLC automatique non disponible pour CSI300 V1. "
                    "Importez/préparez les OHLC séparément."
                )
        raise DynamicUniverseOHLCPrepareError("OHLC preparation is only supported for SP500_HISTORICAL_DYNAMIC scenarios.")

    resolved_start = _parse_date_value(start_date, name="start_date") or getattr(backtest, "start_date", None)
    resolved_end = _parse_date_value(end_date, name="end_date") or getattr(backtest, "end_date", None)
    if not resolved_start or not resolved_end:
        raise DynamicUniverseOHLCPrepareError("start_date and end_date are required unless backtest_id supplies them.")
    if resolved_end < resolved_start:
        raise DynamicUniverseOHLCPrepareError("end_date must be greater than or equal to start_date.")
    return scenario, resolved_start, resolved_end, _coverage_start(backtest, resolved_start)


def _daily_bar_defaults(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": int(row["volume"]),
        "source": "eodhd",
    }


def _values_equal(current: Any, incoming: Any) -> bool:
    if isinstance(incoming, Decimal):
        return Decimal(str(current)) == incoming
    return current == incoming


def _upsert_daily_bars(symbol: Symbol, rows: list[dict[str, Any]]) -> tuple[int, int, int]:
    inserted = updated = unchanged = 0
    for row in rows:
        defaults = _daily_bar_defaults(row)
        bar, created = DailyBar.objects.get_or_create(
            symbol=symbol,
            date=row["date"],
            defaults=defaults,
        )
        if created:
            inserted += 1
            continue
        changed_fields = [
            field_name
            for field_name, incoming_value in defaults.items()
            if not _values_equal(getattr(bar, field_name), incoming_value)
        ]
        if not changed_fields:
            unchanged += 1
            continue
        for field_name in changed_fields:
            setattr(bar, field_name, defaults[field_name])
        bar.save(update_fields=[*changed_fields, "ingested_at"])
        updated += 1
    return inserted, updated, unchanged


def _fetch_window_for_symbol(symbol: Symbol, ranges_by_symbol_id: dict[int, list[OHLCRequiredRange]]) -> tuple[date, date]:
    required_ranges = ranges_by_symbol_id.get(symbol.id) or []
    if not required_ranges:
        raise DynamicUniverseOHLCPrepareError(f"No OHLC range resolved for symbol {symbol.ticker}.")
    return (
        min(required_range.start for required_range in required_ranges),
        max(required_range.end for required_range in required_ranges),
    )


def _normalize_ticker_set(tickers: Any | None) -> set[str]:
    if not tickers:
        return set()
    if isinstance(tickers, str):
        raw_values = [tickers]
    else:
        raw_values = tickers
    return {
        str(ticker).strip().upper()
        for ticker in raw_values
        if str(ticker).strip()
    }


def prepare_dynamic_universe_ohlc(
    *,
    universe_code: str = SP500_UNIVERSE_CODE,
    start_date: date | str | None = None,
    end_date: date | str | None = None,
    backtest_id: int | None = None,
    scenario_id: int | None = None,
    provider: str = "eodhd",
    force_refresh: bool = False,
    max_symbols: int | None = None,
    exclude_tickers: Any | None = None,
    job: Any | None = None,
    client: EODHDClient | None = None,
    progress_callback: Callable[[str], None] | None = None,
) -> DynamicUniverseOHLCPrepareResult:
    del job
    provider_key = str(provider or "").strip().lower()
    if provider_key != "eodhd":
        raise DynamicUniverseOHLCPrepareError(f"Unsupported OHLC provider for dynamic universe: {provider}")

    scenario, scoped_start, scoped_end, coverage_start = _resolve_scope(
        universe_code=universe_code,
        start_date=start_date,
        end_date=end_date,
        backtest_id=backtest_id,
        scenario_id=scenario_id,
    )
    resolved_universe = UniverseResolver().resolve(
        scenario=scenario,
        start_date=scoped_start,
        end_date=scoped_end,
        warmup_start_date=coverage_start,
    )
    symbols = list(resolved_universe.symbols)
    ranges_by_symbol_id = get_required_ohlc_ranges_for_dynamic_universe(
        symbols=symbols,
        start_date=coverage_start,
        end_date=scoped_end,
        membership_by_ticker=resolved_universe.membership_by_ticker,
    )
    missing_before_symbols = get_missing_ohlc_symbols_for_dynamic_universe(
        symbols=symbols,
        start_date=coverage_start,
        end_date=scoped_end,
        membership_by_ticker=resolved_universe.membership_by_ticker,
    )
    missing_before = [symbol.ticker for symbol in missing_before_symbols]
    target_symbols = symbols if force_refresh else missing_before_symbols
    excluded_tickers = _normalize_ticker_set(exclude_tickers)
    skipped_by_request = [
        symbol.ticker
        for symbol in target_symbols
        if symbol.ticker.strip().upper() in excluded_tickers
    ]
    if excluded_tickers:
        target_symbols = [
            symbol
            for symbol in target_symbols
            if symbol.ticker.strip().upper() not in excluded_tickers
        ]
    if max_symbols is not None:
        target_symbols = target_symbols[: max(0, int(max_symbols))]

    result = DynamicUniverseOHLCPrepareResult(
        checked_symbols=len(symbols),
        ready_before=len(symbols) - len(missing_before_symbols),
        missing_before=missing_before,
        ready_after=len(symbols) - len(missing_before_symbols),
        missing_after=missing_before,
        provider=provider_key,
    )
    for ticker in skipped_by_request:
        result.skipped_symbols[ticker] = "excluded_by_request"
    if not target_symbols:
        return result

    eodhd_client = client or EODHDClient()
    for index, symbol in enumerate(target_symbols, start=1):
        if progress_callback:
            progress_callback(f"{index}/{len(target_symbols)} {symbol.ticker}")
        try:
            provider_symbol = to_eodhd_symbol(symbol)
        except UnsupportedEODHDSymbolError as exc:
            result.skipped_symbols[symbol.ticker] = str(exc)
            continue

        try:
            fetch_start, fetch_end = _fetch_window_for_symbol(symbol, ranges_by_symbol_id)
            rows = eodhd_client.fetch_historical_ohlc(provider_symbol, fetch_start, fetch_end)
        except EODHDError as exc:
            message = sanitize_provider_error_message(exc)
            if _is_network_error(exc):
                result.network_error_symbols[symbol.ticker] = message
            else:
                result.provider_error_symbols[symbol.ticker] = message
            continue

        if not rows:
            result.no_data_symbols.append(symbol.ticker)
            continue

        with transaction.atomic():
            inserted, updated, unchanged = _upsert_daily_bars(symbol, rows)
        result.fetched_symbols.append(symbol.ticker)
        result.inserted_bars += inserted
        result.updated_bars += updated
        result.unchanged_bars += unchanged

    missing_after_symbols = get_missing_ohlc_symbols_for_dynamic_universe(
        symbols=symbols,
        start_date=coverage_start,
        end_date=scoped_end,
        membership_by_ticker=resolved_universe.membership_by_ticker,
    )
    result.missing_after = [symbol.ticker for symbol in missing_after_symbols]
    result.ready_after = len(symbols) - len(missing_after_symbols)
    return result
