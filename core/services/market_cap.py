from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal

from core.models import HistoricalMarketCap, Symbol


def get_market_cap_at_or_before(symbol: Symbol, as_of: date, provider: str = "eodhd") -> Decimal | None:
    # Warning: this helper performs a DB lookup and must not be called from
    # simulation hot loops. Future engine integration should preload series once
    # per run and resolve dates from in-memory caches.
    if not symbol or not as_of:
        return None
    row = (
        HistoricalMarketCap.objects
        .filter(symbol=symbol, provider=provider, date__lte=as_of)
        .order_by("-date")
        .only("market_cap")
        .first()
    )
    return row.market_cap if row is not None else None


def preload_market_cap_series(
    symbols: list[Symbol],
    start_date: date,
    end_date: date,
    provider: str = "eodhd",
) -> dict[int, list[tuple[date, Decimal]]]:
    if not symbols or not start_date or not end_date:
        return {}
    symbol_ids = [s.id for s in symbols if getattr(s, "id", None)]
    if not symbol_ids:
        return {}

    rows = (
        HistoricalMarketCap.objects
        .filter(
            symbol_id__in=symbol_ids,
            provider=provider,
            date__gte=start_date,
            date__lte=end_date,
        )
        .order_by("symbol_id", "date")
        .values_list("symbol_id", "date", "market_cap")
    )
    series: dict[int, list[tuple[date, Decimal]]] = defaultdict(list)
    for symbol_id, row_date, market_cap in rows:
        series[symbol_id].append((row_date, market_cap))
    return dict(series)
