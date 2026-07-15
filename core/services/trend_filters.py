from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any

from core.models import DailyBar, Symbol
from core.services.china_benchmark_registry import csi300_market_benchmark_ticker
from core.services.csi300_sector_gm import resolve_csi300_sector_benchmark
from core.services.global_momentum import (
    DEFAULT_GLOBAL_MOMENTUM_NEUTRAL_BAND,
    regime_for_value,
)


TREND_FILTER_OPERATOR_KEY = "trend_filter_operator"
TREND_FILTER_GM_CURRENT_KEY = "trend_filter_gm_current"
TREND_FILTER_GM_MARKET_KEY = "trend_filter_gm_market"
TREND_FILTER_GM_SECTOR_KEY = "trend_filter_gm_sector"

TREND_FILTER_CODES = {"IGNORE", "GM_POS", "GM_NEG", "GM_NEU", "GM_POS_OR_NEU", "GM_NEG_OR_NEU"}
TREND_FILTER_LABELS = {
    TREND_FILTER_GM_CURRENT_KEY: "GM current",
    TREND_FILTER_GM_MARKET_KEY: "GM_market",
    TREND_FILTER_GM_SECTOR_KEY: "GM_sector",
}

_US_EXCHANGES = {
    "AMEX",
    "ARCA",
    "BATS",
    "NASDAQ",
    "NYSE",
    "NYSE ARCA",
}
_US_COUNTRIES = {"US", "USA", "UNITED STATES", "UNITED STATES OF AMERICA"}
_CHINA_A_SHARE_EXCHANGES = {"SHG", "SHE"}
_CSI300_UNIVERSE_CODE = "CSI300"
_MARKET_BENCHMARK_OVERRIDES: dict[str, str] = {
    "SPY": "SPY",
}
_SECTOR_ETF_BY_NORMALIZED_SECTOR = {
    "BASIC MATERIALS": "XLB",
    "COMMUNICATION SERVICES": "XLC",
    "CONSUMER CYCLICAL": "XLY",
    "CONSUMER DEFENSIVE": "XLP",
    "CONSUMER DISCRETIONARY": "XLY",
    "CONSUMER STAPLES": "XLP",
    "ENERGY": "XLE",
    "FINANCIAL SERVICES": "XLF",
    "FINANCIALS": "XLF",
    "HEALTH CARE": "XLV",
    "HEALTHCARE": "XLV",
    "INDUSTRIALS": "XLI",
    "MATERIALS": "XLB",
    "REAL ESTATE": "XLRE",
    "TECHNOLOGY": "XLK",
    "UTILITIES": "XLU",
}


def _to_dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def normalize_trend_filter_code(value: Any) -> str:
    code = str(value or "IGNORE").strip().upper()
    return code if code in TREND_FILTER_CODES else "IGNORE"


def normalize_trend_filter_operator(value: Any) -> str:
    operator = str(value or "AND").strip().upper()
    return operator if operator in {"AND", "OR"} else "AND"


def normalize_trend_filter_settings(settings: Any) -> dict[str, str]:
    settings = settings if isinstance(settings, dict) else {}
    return {
        TREND_FILTER_OPERATOR_KEY: normalize_trend_filter_operator(settings.get(TREND_FILTER_OPERATOR_KEY)),
        TREND_FILTER_GM_CURRENT_KEY: normalize_trend_filter_code(settings.get(TREND_FILTER_GM_CURRENT_KEY)),
        TREND_FILTER_GM_MARKET_KEY: normalize_trend_filter_code(settings.get(TREND_FILTER_GM_MARKET_KEY)),
        TREND_FILTER_GM_SECTOR_KEY: normalize_trend_filter_code(settings.get(TREND_FILTER_GM_SECTOR_KEY)),
    }


def active_trend_filter_keys(settings: Any) -> list[str]:
    normalized = normalize_trend_filter_settings(settings)
    return [
        key
        for key in (TREND_FILTER_GM_CURRENT_KEY, TREND_FILTER_GM_MARKET_KEY, TREND_FILTER_GM_SECTOR_KEY)
        if normalized.get(key) != "IGNORE"
    ]


def has_active_trend_filters(settings: Any) -> bool:
    return bool(active_trend_filter_keys(settings))


def _normalize_sector_name(value: Any) -> str:
    text = str(value or "").strip().upper()
    for char in ("-", "_", "/", "&"):
        text = text.replace(char, " ")
    return " ".join(text.split())


def _is_china_a_share_symbol(symbol: Symbol | None) -> bool:
    exchange = str(getattr(symbol, "exchange", "") or "").strip().upper()
    return exchange in _CHINA_A_SHARE_EXCHANGES


def market_benchmark_ticker_for_symbol(symbol: Symbol | None, *, universe_code: str | None = None) -> str | None:
    if symbol is None:
        return None
    ticker = str(getattr(symbol, "ticker", "") or "").strip().upper()
    if _is_china_a_share_symbol(symbol):
        if str(universe_code or "").strip().upper() == _CSI300_UNIVERSE_CODE:
            return csi300_market_benchmark_ticker()
        return None
    if ticker in _MARKET_BENCHMARK_OVERRIDES:
        return _MARKET_BENCHMARK_OVERRIDES[ticker]
    country = str(getattr(symbol, "country", "") or "").strip().upper()
    exchange = str(getattr(symbol, "exchange", "") or "").strip().upper()
    if country in _US_COUNTRIES or exchange in _US_EXCHANGES:
        return "SPY"
    return None


def sector_benchmark_ticker_for_symbol(symbol: Symbol | None, *, universe_code: str | None = None) -> str | None:
    if symbol is None:
        return None
    ticker = str(getattr(symbol, "ticker", "") or "").strip().upper()
    if _is_china_a_share_symbol(symbol):
        if str(universe_code or "").strip().upper() == _CSI300_UNIVERSE_CODE:
            resolution = resolve_csi300_sector_benchmark(symbol)
            return resolution.benchmark_ticker or None
        return None
    if ticker in _SECTOR_ETF_BY_NORMALIZED_SECTOR.values():
        return ticker
    normalized_sector = _normalize_sector_name(getattr(symbol, "sector", ""))
    return _SECTOR_ETF_BY_NORMALIZED_SECTOR.get(normalized_sector)


def collect_distinct_benchmark_tickers(
    symbols: list[Symbol],
    settings: Any,
    *,
    universe_code: str | None = None,
) -> set[str]:
    normalized = normalize_trend_filter_settings(settings)
    out: set[str] = set()
    if normalized[TREND_FILTER_GM_MARKET_KEY] != "IGNORE":
        for symbol in symbols:
            ticker = market_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
            if ticker:
                out.add(ticker)
    if normalized[TREND_FILTER_GM_SECTOR_KEY] != "IGNORE":
        for symbol in symbols:
            ticker = sector_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
            if ticker:
                out.add(ticker)
    return out


def preload_benchmark_price_cache(
    *,
    symbols: list[Symbol],
    scenario,
    start_date: date,
    end_date: date,
) -> dict[str, dict[str, list[Any]]]:
    if not symbols or not start_date or not end_date:
        return {}

    a = _to_dec(getattr(scenario, "a", None))
    b = _to_dec(getattr(scenario, "b", None))
    c = _to_dec(getattr(scenario, "c", None))
    d = _to_dec(getattr(scenario, "d", None))
    if None in (a, b, c, d):
        return {}
    denom = a + b + c + d
    if denom == 0:
        return {}

    symbol_ids = [symbol.id for symbol in symbols if getattr(symbol, "id", None)]
    ticker_by_symbol_id = {symbol.id: symbol.ticker for symbol in symbols if getattr(symbol, "id", None)}
    if not symbol_ids:
        return {}

    rows = (
        DailyBar.objects.filter(
            symbol_id__in=symbol_ids,
            date__gte=start_date,
            date__lte=end_date,
        )
        .order_by("symbol_id", "date")
        .values("symbol_id", "date", "open", "high", "low", "close")
    )

    cache: dict[str, dict[str, list[Any]]] = defaultdict(lambda: {"dates": [], "values": []})
    for row in rows:
        ticker = ticker_by_symbol_id.get(row["symbol_id"])
        if not ticker:
            continue
        open_v = _to_dec(row.get("open"))
        high_v = _to_dec(row.get("high"))
        low_v = _to_dec(row.get("low"))
        close_v = _to_dec(row.get("close"))
        if None in (open_v, high_v, low_v, close_v):
            continue
        p_value = ((a * close_v) + (b * high_v) + (c * low_v) + (d * open_v)) / denom
        cache[ticker]["dates"].append(row["date"])
        cache[ticker]["values"].append(p_value)
    return dict(cache)


def trend_return_from_cache(cache_entry: dict[str, list[Any]] | None, *, as_of: date, nglobal: int) -> Decimal | None:
    if not cache_entry or not as_of:
        return None
    nglobal = int(nglobal or 0)
    if nglobal <= 0:
        return None
    dates = cache_entry.get("dates") or []
    values = cache_entry.get("values") or []
    idx = bisect_right(dates, as_of) - 1
    if idx < 0 or idx < nglobal or idx >= len(values):
        return None
    cur = _to_dec(values[idx])
    base = _to_dec(values[idx - nglobal])
    if cur is None or base in (None, Decimal("0")):
        return None
    try:
        return (cur / base) - Decimal("1")
    except Exception:
        return None


def trend_regime_from_cache(
    cache_entry: dict[str, list[Any]] | None,
    *,
    as_of: date,
    nglobal: int,
    neutral_band: Decimal | None = None,
) -> str | None:
    return regime_for_value(
        trend_return_from_cache(cache_entry, as_of=as_of, nglobal=nglobal),
        neutral_band=neutral_band or DEFAULT_GLOBAL_MOMENTUM_NEUTRAL_BAND,
    )


def trend_filter_matches(*, actual_regime: str | None, expected_filter: Any) -> bool:
    expected = normalize_trend_filter_code(expected_filter)
    if expected == "IGNORE":
        return True
    actual = str(actual_regime or "").strip().upper()
    if expected == "GM_POS_OR_NEU":
        return actual in {"GM_POS", "GM_NEU"}
    if expected == "GM_NEG_OR_NEU":
        return actual in {"GM_NEG", "GM_NEU"}
    return actual == expected


def gm_condition_matches(
    *,
    actual_value: Decimal | None,
    mode: Any,
    threshold: Decimal | None = None,
    explicit_threshold: bool = False,
    neutral_band: Decimal | None = None,
) -> bool:
    """Evaluate one GM market condition against a numeric momentum value.

    Legacy GM conditions are regime-based and use the historical neutral band.
    Explicit-threshold conditions compare directly against the provided threshold.
    """
    if actual_value is None:
        return False
    mode_text = str(mode or "IGNORE").strip().upper()
    if mode_text.startswith("GM_"):
        mode_text = mode_text[3:]
    if mode_text in {"POSITIVE", "POSITIF"}:
        mode_text = "POS"
    elif mode_text in {"NEGATIVE", "NEGATIF"}:
        mode_text = "NEG"
    elif mode_text in {"NEUTRAL", "NEUTRE"}:
        mode_text = "NEU"
    if mode_text == "IGNORE":
        return True

    band = _to_dec(neutral_band) or DEFAULT_GLOBAL_MOMENTUM_NEUTRAL_BAND
    if explicit_threshold:
        threshold_value = _to_dec(threshold)
        if threshold_value is None:
            return False
        if mode_text == "POS":
            return actual_value > threshold_value
        if mode_text == "NEG":
            return actual_value < threshold_value
        if mode_text == "NEU":
            return abs(actual_value) <= abs(threshold_value)
        return False

    if mode_text == "POS":
        return actual_value > band
    if mode_text == "NEG":
        return actual_value < -band
    if mode_text == "NEU":
        return abs(actual_value) <= band
    if mode_text == "POS_OR_NEU":
        return actual_value >= -band
    if mode_text == "NEG_OR_NEU":
        return actual_value <= band
    return False


def evaluate_trend_filters_for_symbol(
    *,
    symbol: Symbol | None,
    settings: Any,
    as_of: date,
    nglobal: int,
    gm_current_regime: str | None,
    benchmark_cache_by_ticker: dict[str, dict[str, list[Any]]] | None,
    suppress_gm_current: bool = False,
    universe_code: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_trend_filter_settings(settings)
    if suppress_gm_current:
        normalized[TREND_FILTER_GM_CURRENT_KEY] = "IGNORE"
    operator = normalized[TREND_FILTER_OPERATOR_KEY]
    benchmark_cache_by_ticker = benchmark_cache_by_ticker or {}
    filters: dict[str, dict[str, Any]] = {}

    def _append_result(
        key: str,
        *,
        expected: str,
        actual: str | None,
        source_ticker: str | None = None,
        reason: str | None = None,
    ) -> None:
        status = "ignored"
        passed = None
        if expected != "IGNORE":
            if actual is None:
                status = "missing"
                passed = False
            else:
                passed = trend_filter_matches(actual_regime=actual, expected_filter=expected)
                status = "passed" if passed else "failed"
        filters[key] = {
            "label": TREND_FILTER_LABELS[key],
            "filter_code": expected,
            "actual_regime": actual,
            "status": status,
            "passed": passed,
            "benchmark_ticker": source_ticker,
            "reason": reason,
        }

    _append_result(
        TREND_FILTER_GM_CURRENT_KEY,
        expected=normalized[TREND_FILTER_GM_CURRENT_KEY],
        actual=gm_current_regime,
    )

    market_benchmark_ticker = market_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
    market_reason = None
    market_regime = None
    if normalized[TREND_FILTER_GM_MARKET_KEY] != "IGNORE":
        if market_benchmark_ticker is None:
            market_reason = "missing benchmark mapping"
        else:
            market_regime = trend_regime_from_cache(
                benchmark_cache_by_ticker.get(market_benchmark_ticker),
                as_of=as_of,
                nglobal=nglobal,
            )
            if market_regime is None:
                market_reason = "missing benchmark data or insufficient lookback"
    _append_result(
        TREND_FILTER_GM_MARKET_KEY,
        expected=normalized[TREND_FILTER_GM_MARKET_KEY],
        actual=market_regime,
        source_ticker=market_benchmark_ticker,
        reason=market_reason,
    )

    sector_benchmark_ticker = sector_benchmark_ticker_for_symbol(symbol, universe_code=universe_code)
    sector_reason = None
    sector_regime = None
    if normalized[TREND_FILTER_GM_SECTOR_KEY] != "IGNORE":
        if sector_benchmark_ticker is None:
            sector_reason = "missing sector mapping"
        else:
            sector_regime = trend_regime_from_cache(
                benchmark_cache_by_ticker.get(sector_benchmark_ticker),
                as_of=as_of,
                nglobal=nglobal,
            )
            if sector_regime is None:
                sector_reason = "missing benchmark data or insufficient lookback"
    _append_result(
        TREND_FILTER_GM_SECTOR_KEY,
        expected=normalized[TREND_FILTER_GM_SECTOR_KEY],
        actual=sector_regime,
        source_ticker=sector_benchmark_ticker,
        reason=sector_reason,
    )

    active = [payload for payload in filters.values() if payload["filter_code"] != "IGNORE"]
    if not active:
        passed = True
    elif operator == "AND":
        passed = all(payload["passed"] is True for payload in active)
    else:
        passed = any(payload["passed"] is True for payload in active)

    return {
        "operator": operator,
        "passed": passed,
        "has_active": bool(active),
        "filters": filters,
    }


def summarize_benchmark_usage(
    *,
    symbols: list[Symbol],
    settings: Any,
    max_sector_curves: int = 6,
    universe_code: str | None = None,
) -> dict[str, Any]:
    normalized = normalize_trend_filter_settings(settings)
    market_tickers: list[str] = []
    sector_tickers: list[str] = []
    if normalized[TREND_FILTER_GM_MARKET_KEY] != "IGNORE":
        market_tickers = sorted({
            ticker
            for ticker in (market_benchmark_ticker_for_symbol(symbol, universe_code=universe_code) for symbol in symbols)
            if ticker
        })
    if normalized[TREND_FILTER_GM_SECTOR_KEY] != "IGNORE":
        sector_tickers = sorted({
            ticker
            for ticker in (sector_benchmark_ticker_for_symbol(symbol, universe_code=universe_code) for symbol in symbols)
            if ticker
        })
    sector_warning = None
    if len(sector_tickers) > max_sector_curves:
        sector_warning = (
            f"Too many distinct sector ETF curves ({len(sector_tickers)}). "
            f"Display is capped to {max_sector_curves}."
        )
    return {
        "market_benchmarks": market_tickers,
        "sector_benchmarks": sector_tickers[:max_sector_curves],
        "sector_benchmark_total": len(sector_tickers),
        "sector_warning": sector_warning,
    }
