from __future__ import annotations

from datetime import date
from typing import Any

from tools.csi300_policy import (
    CSI300_SUPPORTED_HISTORY_START_ISO,
    is_csi300_universe,
)


def _persisted_supported_history_start(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip()
    try:
        date.fromisoformat(normalized)
    except ValueError:
        return ""
    return normalized


def supported_history_start_for_universe_meta(universe_meta: Any) -> str:
    if not isinstance(universe_meta, dict) or not is_csi300_universe(
        universe_code=universe_meta.get("universe_code") or universe_meta.get("code"),
        universe_mode=universe_meta.get("mode"),
    ):
        return ""
    return (
        _persisted_supported_history_start(universe_meta.get("supported_history_start"))
        or CSI300_SUPPORTED_HISTORY_START_ISO
    )


def supported_history_start_for_backtest_display(backtest: Any) -> str:
    results = getattr(backtest, "results", None)
    if isinstance(results, dict) and results:
        meta = results.get("meta")
        universe_meta = meta.get("universe") if isinstance(meta, dict) else None
        return supported_history_start_for_universe_meta(universe_meta)
    if results not in (None, {}):
        return ""

    scenario = getattr(backtest, "scenario", None)
    if is_csi300_universe(universe_mode=getattr(scenario, "universe_mode", "")):
        return CSI300_SUPPORTED_HISTORY_START_ISO
    return ""


def supported_history_start_label(value: Any) -> str:
    normalized = _persisted_supported_history_start(value)
    if normalized == CSI300_SUPPORTED_HISTORY_START_ISO:
        return "3 janvier 2023"
    return normalized
