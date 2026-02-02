"""Optional Parquet storage for backtest daily series.

This module is intentionally isolated so it can be enabled/disabled without
impacting existing features.

Activation
----------
- ENABLE_PARQUET_STORAGE=1  -> writes Parquet files
- ENABLE_PARQUET_STORAGE!=1 -> does nothing

Storage
-------
Default base directory: /data (override with BACKTEST_DATA_DIR)
Layout:
    /data/backtests/<backtest_id>/<scenario>/<ticker>.parquet

Notes
-----
- This is *additive*: existing JSON persistence remains unchanged.
- Failures in Parquet writing MUST NOT fail the backtest (no regression).
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Any, Iterable

logger = logging.getLogger(__name__)


def parquet_storage_enabled() -> bool:
    return os.environ.get("ENABLE_PARQUET_STORAGE", "0").strip() == "1"


def _safe_segment(value: str) -> str:
    """Make a string safe to use as a folder segment."""
    value = (value or "").strip()
    if not value:
        return "unknown"
    # Keep it readable and filesystem-safe
    value = value.lower()
    value = re.sub(r"[^a-z0-9_-]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "unknown"


def _iter_daily_rows_for_ticker(results_tickers_entry: dict[str, Any]) -> Iterable[dict[str, Any]]:
    """Yield flattened daily rows for all lines of a ticker."""
    lines = results_tickers_entry.get("lines") or []
    for line in lines:
        li = line.get("line_index")
        buy = line.get("buy")
        sell = line.get("sell")
        daily = line.get("daily") or []
        for row in daily:
            if not isinstance(row, dict):
                continue
            out = dict(row)  # shallow copy
            out["line_index"] = li
            out["buy"] = buy
            out["sell"] = sell
            yield out


def write_backtest_parquet_files(backtest: Any, results: dict[str, Any]) -> dict[str, Any]:
    """Write Parquet files for a backtest result.

    Returns a small report dict for logs/diagnostics.

    MUST NEVER raise.
    """
    report: dict[str, Any] = {
        "enabled": False,
        "base_dir": None,
        "written": 0,
        "skipped": 0,
        "errors": 0,
        "error_samples": [],
    }

    if not parquet_storage_enabled():
        return report

    report["enabled"] = True

    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception as e:  # pragma: no cover
        # Optional dependency; do not fail backtest.
        msg = f"pyarrow not available: {e}"
        logger.warning(msg)
        report["errors"] += 1
        report["error_samples"].append(msg)
        return report

    base_dir = os.environ.get("BACKTEST_DATA_DIR", "/data").strip() or "/data"
    report["base_dir"] = base_dir

    scenario_name = getattr(getattr(backtest, "scenario", None), "name", "")
    scenario_segment = _safe_segment(scenario_name) if scenario_name else str(getattr(backtest, "scenario_id", "scenario"))

    root = Path(base_dir) / "backtests" / str(getattr(backtest, "id", "")) / scenario_segment

    try:
        root.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        msg = f"Cannot create parquet dir {root}: {e}"
        logger.warning(msg)
        report["errors"] += 1
        report["error_samples"].append(msg)
        return report

    tickers_block = (results or {}).get("tickers") or {}
    if not isinstance(tickers_block, dict):
        msg = "results['tickers'] is not a dict; skip parquet write"
        logger.warning(msg)
        report["errors"] += 1
        report["error_samples"].append(msg)
        return report

    for ticker, tentry in tickers_block.items():
        try:
            ticker_str = str(ticker).strip()
            if not ticker_str:
                report["skipped"] += 1
                continue

            rows = list(_iter_daily_rows_for_ticker(tentry if isinstance(tentry, dict) else {}))
            if not rows:
                report["skipped"] += 1
                continue

            # Build Arrow table directly (avoid pandas dependency)
            table = pa.Table.from_pylist(rows)

            fp = root / f"{_safe_segment(ticker_str)}.parquet"
            pq.write_table(table, fp, compression="snappy")
            report["written"] += 1
        except Exception as e:
            report["errors"] += 1
            if len(report["error_samples"]) < 5:
                report["error_samples"].append(f"{ticker}: {e}")
            logger.exception("Parquet write failed for %s", ticker)

    return report
