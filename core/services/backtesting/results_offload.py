"""Backtest results offload to avoid PostgreSQL JSONB hard limit.

Why
---
PostgreSQL enforces a hard limit (~256MB) on the total size of JSONB object
elements. With large universes (e.g., S&P 500) and long histories, storing the
full per-ticker daily series inside Backtest.results can exceed that limit and
crash the update.

Design goals
------------
- **No regression**: small/normal backtests keep the legacy behaviour (daily
  series embedded in JSON).
- **Additive**: when the payload becomes too large, we move only the heavy
  `daily` arrays to files on disk and keep a lightweight pointer in JSON.
- **No extra deps**: uses only stdlib (json + gzip).

Storage layout
--------------
Base dir: BACKTEST_DATA_DIR (default: /data)

    /data/backtests/<backtest_id>/<scenario_segment>/daily/<ticker>_L<line>.json.gz
"""

from __future__ import annotations

import gzip
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Tuple

logger = logging.getLogger(__name__)


DEFAULT_MAX_DB_PAYLOAD_MB = 200  # safety margin below PG JSONB ~256MB limit


def _safe_segment(value: str) -> str:
    value = (value or "").strip()
    if not value:
        return "unknown"
    value = value.lower()
    value = re.sub(r"[^a-z0-9_-]+", "-", value)
    value = re.sub(r"-+", "-", value).strip("-")
    return value or "unknown"


def estimate_json_bytes(payload: Any) -> int:
    """Return an approximate UTF-8 encoded size of a JSON payload."""
    try:
        return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))
    except Exception:
        # Fallback: be conservative.
        try:
            return len(json.dumps(payload).encode("utf-8"))
        except Exception:
            return 0


def _paths_for(backtest: Any, scenario_segment: str, ticker: str, line_index: int) -> Tuple[Path, str]:
    base_dir = os.environ.get("BACKTEST_DATA_DIR", "/data").strip() or "/data"
    root = Path(base_dir) / "backtests" / str(getattr(backtest, "id", "")) / scenario_segment / "daily"
    root.mkdir(parents=True, exist_ok=True)
    fname = f"{_safe_segment(ticker)}_L{int(line_index)}.json.gz"
    fp = root / fname
    # Store path as absolute to avoid ambiguity across deployments
    return fp, str(fp)


def offload_daily_series_if_needed(backtest: Any, results: Dict[str, Any], max_mb: int | None = None) -> Dict[str, Any]:
    """Offload heavy per-ticker daily arrays to disk when DB payload would be too large.

    - Mutates and returns `results`.
    - If payload size is under threshold, does nothing.

    The offload replaces `line['daily']` with:
        - daily_path
        - daily_rows
        - daily_offloaded = True
    """

    if not isinstance(results, dict):
        return results

    threshold_mb = max_mb or int(os.environ.get("BACKTEST_RESULTS_MAX_DB_MB", DEFAULT_MAX_DB_PAYLOAD_MB))
    threshold_bytes = threshold_mb * 1024 * 1024

    payload_bytes = estimate_json_bytes(results)
    results.setdefault("meta", {})
    results["meta"]["results_bytes_estimate"] = payload_bytes

    if payload_bytes and payload_bytes <= threshold_bytes:
        results["meta"]["daily_offload"] = {
            "enabled": False,
            "reason": "below_threshold",
            "threshold_mb": threshold_mb,
        }
        return results

    tickers_block = results.get("tickers") or {}
    if not isinstance(tickers_block, dict) or not tickers_block:
        results["meta"]["daily_offload"] = {
            "enabled": False,
            "reason": "no_tickers_block",
            "threshold_mb": threshold_mb,
        }
        return results

    scenario_name = getattr(getattr(backtest, "scenario", None), "name", "")
    scenario_segment = _safe_segment(scenario_name) if scenario_name else str(getattr(backtest, "scenario_id", "scenario"))

    written = 0
    errors = 0
    total_rows = 0

    for ticker, tentry in tickers_block.items():
        if not isinstance(tentry, dict):
            continue
        lines = tentry.get("lines") or []
        if not isinstance(lines, list):
            continue

        for line in lines:
            if not isinstance(line, dict):
                continue
            daily = line.get("daily")
            if not daily:
                continue
            try:
                li = int(line.get("line_index") or 1)
                fp, fp_str = _paths_for(backtest, scenario_segment, str(ticker), li)
                # Write gzip JSON
                with gzip.open(fp, "wt", encoding="utf-8") as f:
                    json.dump(daily, f, ensure_ascii=False)

                total_rows += len(daily) if isinstance(daily, list) else 0
                written += 1

                # Replace heavy payload with pointers
                line.pop("daily", None)
                line["daily_offloaded"] = True
                line["daily_backend"] = "json.gz"
                line["daily_path"] = fp_str
                line["daily_rows"] = len(daily) if isinstance(daily, list) else None
            except Exception as e:
                errors += 1
                if errors <= 5:
                    logger.exception("Daily offload failed for %s", ticker)
                # If offload fails, keep legacy behaviour for that line to avoid losing data
                continue

    results["meta"]["daily_offload"] = {
        "enabled": True,
        "backend": "json.gz",
        "threshold_mb": threshold_mb,
        "written_files": written,
        "errors": errors,
        "total_rows": total_rows,
    }

    # Re-estimate size after offload (diagnostic)
    results["meta"]["results_bytes_estimate_after"] = estimate_json_bytes(results)

    return results


def load_daily_from_line(line: Dict[str, Any]) -> list[dict[str, Any]]:
    """Return daily series either from embedded JSON or from an offloaded file."""
    daily = line.get("daily")
    if isinstance(daily, list) and daily:
        return daily
    if line.get("daily_offloaded") and line.get("daily_backend") == "json.gz" and line.get("daily_path"):
        fp = Path(str(line.get("daily_path")))
        if fp.exists():
            try:
                with gzip.open(fp, "rt", encoding="utf-8") as f:
                    data = json.load(f)
                return data if isinstance(data, list) else []
            except Exception:
                logger.exception("Failed to read offloaded daily file %s", fp)
                return []
    return []
