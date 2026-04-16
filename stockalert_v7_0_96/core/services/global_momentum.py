from __future__ import annotations

from collections import defaultdict
from datetime import date
from decimal import Decimal
from typing import Any, Callable

GLOBAL_MOMENTUM_POS = "GM_POS"
GLOBAL_MOMENTUM_NEG = "GM_NEG"
GLOBAL_MOMENTUM_NEU = "GM_NEU"
GLOBAL_MOMENTUM_CODES = (GLOBAL_MOMENTUM_POS, GLOBAL_MOMENTUM_NEG, GLOBAL_MOMENTUM_NEU)
DEFAULT_GLOBAL_MOMENTUM_NEUTRAL_BAND = Decimal("0.001")  # 0.1%


def _to_dec(v: Any) -> Decimal | None:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


def regime_for_value(value: Decimal | None, neutral_band: Decimal | None = None) -> str | None:
    if value is None:
        return None
    try:
        band = _to_dec(neutral_band)
    except Exception:
        band = None
    if band is None:
        band = DEFAULT_GLOBAL_MOMENTUM_NEUTRAL_BAND
    if value > band:
        return GLOBAL_MOMENTUM_POS
    if value < (-band):
        return GLOBAL_MOMENTUM_NEG
    return GLOBAL_MOMENTUM_NEU


def compute_global_momentum_values_by_date(
    metrics_by_ticker: dict[Any, dict[date, Any]],
    *,
    nglobal: int,
    p_getter: Callable[[Any], Any] | None = None,
) -> dict[date, Decimal | None]:
    """Average per-date Nglobal return across all available tickers.

    Exact formula per ticker i on date t:
        P_i(t) / P_i(t-Nglobal) - 1

    where t-Nglobal means the Nglobal-th prior *available trading observation* for
    that ticker in the provided series.
    """
    nglobal = int(nglobal or 0)
    if nglobal <= 0:
        return {}
    if p_getter is None:
        p_getter = lambda x: x

    acc: dict[date, list[Decimal]] = defaultdict(list)
    for _ticker, series in (metrics_by_ticker or {}).items():
        ordered = sorted((series or {}).items(), key=lambda kv: kv[0])
        if len(ordered) <= nglobal:
            continue
        p_values: list[Decimal | None] = [_to_dec(p_getter(v)) for _, v in ordered]
        for idx in range(nglobal, len(ordered)):
            cur = p_values[idx]
            base = p_values[idx - nglobal]
            if cur is None or base in (None, Decimal("0")):
                continue
            try:
                ret = (cur / base) - Decimal("1")
            except Exception:
                continue
            d = ordered[idx][0]
            acc[d].append(ret)

    out: dict[date, Decimal | None] = {}
    for d, vals in acc.items():
        if vals:
            out[d] = sum(vals) / Decimal(len(vals))
        else:
            out[d] = None
    return out


def build_global_momentum_regime_by_date(
    metrics_by_ticker: dict[Any, dict[date, Any]],
    *,
    nglobal: int,
    neutral_band: Decimal | None = None,
    p_getter: Callable[[Any], Any] | None = None,
) -> dict[date, str]:
    values = compute_global_momentum_values_by_date(metrics_by_ticker, nglobal=nglobal, p_getter=p_getter)
    out: dict[date, str] = {}
    for d, v in values.items():
        regime = regime_for_value(v, neutral_band=neutral_band)
        if regime:
            out[d] = regime
    return out
