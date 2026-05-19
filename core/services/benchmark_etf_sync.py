from __future__ import annotations

import logging

from core.models import Scenario, Symbol
from core.services.symbol_enrichment import enrich_symbols_metadata
from core.services.trend_filters import market_benchmark_ticker_for_symbol, sector_benchmark_ticker_for_symbol


logger = logging.getLogger(__name__)


_BENCHMARK_DEFAULTS_BY_TICKER: dict[str, dict[str, str | bool]] = {
    "SPY": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "SPDR S&P 500 ETF Trust", "active": True},
    "XLB": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Materials Select Sector SPDR Fund", "active": True},
    "XLC": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Communication Services Select Sector SPDR Fund", "active": True},
    "XLE": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Energy Select Sector SPDR Fund", "active": True},
    "XLF": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Financial Select Sector SPDR Fund", "active": True},
    "XLI": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Industrial Select Sector SPDR Fund", "active": True},
    "XLK": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Technology Select Sector SPDR Fund", "active": True},
    "XLP": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Consumer Staples Select Sector SPDR Fund", "active": True},
    "XLRE": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Real Estate Select Sector SPDR Fund", "active": True},
    "XLU": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Utilities Select Sector SPDR Fund", "active": True},
    "XLV": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Health Care Select Sector SPDR Fund", "active": True},
    "XLY": {"exchange": "NYSE ARCA", "country": "US", "instrument_type": "ETF", "name": "Consumer Discretionary Select Sector SPDR Fund", "active": True},
}


def required_benchmark_tickers_for_symbols(symbols) -> set[str]:
    out: set[str] = set()
    for symbol in list(symbols):
        market_ticker = market_benchmark_ticker_for_symbol(symbol)
        if market_ticker:
            out.add(str(market_ticker).upper())
        sector_ticker = sector_benchmark_ticker_for_symbol(symbol)
        if sector_ticker:
            out.add(str(sector_ticker).upper())
    return out


def format_benchmark_sync_summary(totals: dict, *, label: str = "summary") -> str:
    benchmark_tickers = list(totals.get("benchmark_tickers") or [])
    ohlc = totals.get("ohlc") or {}
    summary = (
        f"{label} "
        f"source_symbols={totals.get('source_symbols', 0)} "
        f"benchmarks={len(benchmark_tickers)} "
        f"created={totals.get('created', 0)} "
        f"existing={totals.get('existing', 0)} "
        f"dry_run={int(bool(totals.get('dry_run')))} "
        f"skip_enrichment={int(bool(totals.get('skip_enrichment')))} "
        f"skip_ohlc={int(bool(totals.get('skip_ohlc')))}"
    )
    if ohlc:
        summary += f" ohlc_symbols={ohlc.get('symbols', 0)} ohlc_bars={ohlc.get('bars', 0)}"
    return summary


def _benchmark_defaults(ticker: str) -> dict[str, str | bool]:
    payload = dict(_BENCHMARK_DEFAULTS_BY_TICKER.get(str(ticker or "").upper(), {}))
    payload.setdefault("active", True)
    return payload


def _ensure_benchmark_symbols(*, benchmark_tickers: list[str], dry_run: bool) -> tuple[list[Symbol], dict]:
    details = []
    benchmark_symbols: list[Symbol] = []
    created = existing = 0

    for ticker in benchmark_tickers:
        symbol = Symbol.objects.filter(ticker=ticker).order_by("-active", "exchange", "id").first()
        if symbol is not None:
            status = "existing"
            if not symbol.active:
                status = "reactivated"
                if not dry_run:
                    symbol.active = True
                    symbol.save(update_fields=["active"])
            existing += 1
            benchmark_symbols.append(symbol)
            details.append({"ticker": ticker, "status": status, "symbol_id": symbol.id})
            continue

        defaults = _benchmark_defaults(ticker)
        detail = {"ticker": ticker, "status": "created" if not dry_run else "dry_run_create", "symbol_id": None}
        if dry_run:
            benchmark_symbols.append(Symbol(ticker=ticker, **defaults))
            details.append(detail)
            created += 1
            continue

        symbol = Symbol.objects.create(ticker=ticker, **defaults)
        benchmark_symbols.append(symbol)
        detail["symbol_id"] = symbol.id
        details.append(detail)
        created += 1

    return benchmark_symbols, {"created": created, "existing": existing, "details": details}


def sync_benchmark_etfs_for_symbols(
    symbols,
    *,
    dry_run: bool = False,
    skip_ohlc: bool = False,
    skip_enrichment: bool = False,
) -> dict:
    source_symbols = list(symbols)
    benchmark_tickers = sorted(required_benchmark_tickers_for_symbols(source_symbols))
    benchmark_symbols, ensure_stats = _ensure_benchmark_symbols(benchmark_tickers=benchmark_tickers, dry_run=dry_run)

    enrichment_stats = None
    if benchmark_symbols and not skip_enrichment:
        enrichment_stats = enrich_symbols_metadata(benchmark_symbols, only_missing=True, dry_run=dry_run)

    ohlc_stats = None
    if benchmark_symbols and not skip_ohlc and not dry_run:
        from core.tasks import _fetch_daily_bars_for_symbols, desired_outputsize_years

        years = Scenario.objects.filter(active=True).order_by("-history_years").values_list("history_years", flat=True).first() or 2
        outputsize = desired_outputsize_years(int(years))
        persisted_symbols = [symbol for symbol in benchmark_symbols if getattr(symbol, "id", None)]
        ohlc_stats = _fetch_daily_bars_for_symbols(symbol_qs=persisted_symbols, outputsize=outputsize)

    totals = {
        "source_symbols": len(source_symbols),
        "benchmark_tickers": benchmark_tickers,
        "created": ensure_stats["created"],
        "existing": ensure_stats["existing"],
        "dry_run": bool(dry_run),
        "skip_ohlc": bool(skip_ohlc),
        "skip_enrichment": bool(skip_enrichment),
        "enrichment": enrichment_stats,
        "ohlc": ohlc_stats,
        "per_symbol": ensure_stats["details"],
    }
    logger.info("[benchmark-etf-sync] %s", format_benchmark_sync_summary(totals))
    return totals
