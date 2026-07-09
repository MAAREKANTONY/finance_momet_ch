#!/usr/bin/env python3
"""Convert unliftedq CSI300 constituent history to StockAlert CSV format.

This tool is intentionally local/offline with respect to StockAlert: it does not
read or write the Django database and it does not call market-data providers.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import urllib.request
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

STOCKALERT_COLUMNS = [
    "universe_code",
    "symbol",
    "exchange",
    "mic",
    "name",
    "start_date",
    "end_date",
    "weight",
    "provider_symbol",
    "source",
    "country",
    "currency",
    "sector",
    "industry",
]

DEFAULT_HISTORY_URL = "https://raw.githubusercontent.com/unliftedq/index-constitution/main/history/csi300.csv"
DEFAULT_LATEST_URL = "https://raw.githubusercontent.com/unliftedq/index-constitution/main/latest/csi300.csv"
DEFAULT_EVENT_URL = "https://raw.githubusercontent.com/unliftedq/index-constitution/main/event/cn.csv"
DEFAULT_OUTPUT = "data/generated/csi300_stockalert_memberships.csv"
DEFAULT_SOURCE = "unliftedq_index_constitution"

SHANGHAI_SUFFIXES = {"SS", "SH", "SHG", "SSE", "XSHG"}
SHENZHEN_SUFFIXES = {"SZ", "SHE", "SZSE", "XSHE"}
SHANGHAI_PREFIXES = ("SH", "SSE", "XSHG")
SHENZHEN_PREFIXES = ("SZ", "SZSE", "XSHE")


@dataclass(frozen=True)
class CanonicalSymbol:
    symbol: str
    exchange: str
    mic: str
    provider_symbol: str
    warnings: tuple[str, ...] = ()


@dataclass
class ConversionReport:
    rows_read: int = 0
    memberships_written: int = 0
    distinct_tickers: int = 0
    min_start_date: str = ""
    max_start_date: str = ""
    max_end_date: str = ""
    latest_active_members: int = 0
    exchange_distribution: dict[str, int] = field(default_factory=dict)
    unmappable_tickers: list[str] = field(default_factory=list)
    duplicate_exact_rows: list[dict[str, str]] = field(default_factory=list)
    overlapping_periods: list[dict[str, str]] = field(default_factory=list)
    invalid_dates: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    active_counts: dict[str, int] = field(default_factory=dict)
    yfiua_comparison: dict[str, object] = field(default_factory=dict)
    source_version: str = ""
    event_rows_read: int = 0
    latest_rows_read: int = 0

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def is_url(value: str | Path) -> bool:
    text = str(value)
    return text.startswith("https://") or text.startswith("http://")


def read_text(path_or_url: str | Path) -> str:
    if is_url(path_or_url):
        with urllib.request.urlopen(str(path_or_url), timeout=30) as response:
            return response.read().decode("utf-8-sig")
    return Path(path_or_url).read_text(encoding="utf-8-sig")


def read_csv_rows(path_or_url: str | Path) -> list[dict[str, str]]:
    text = read_text(path_or_url)
    reader = csv.DictReader(io.StringIO(text))
    return [{str(k or "").strip(): str(v or "").strip() for k, v in row.items()} for row in reader]


def canonicalize_cn_symbol(raw_symbol: str, *, strict: bool = False) -> CanonicalSymbol:
    original = str(raw_symbol or "").strip()
    if not original:
        raise ValueError("empty symbol")
    token = original.upper().replace(" ", "")
    warnings: list[str] = []
    exchange = ""
    symbol = ""

    if "." in token:
        left, suffix = token.rsplit(".", 1)
        symbol = left
        exchange = _exchange_from_code(suffix)
    else:
        for prefix in sorted((*SHANGHAI_PREFIXES, *SHENZHEN_PREFIXES), key=len, reverse=True):
            if token.startswith(prefix) and token[len(prefix):].isdigit():
                symbol = token[len(prefix):]
                exchange = _exchange_from_code(prefix)
                break
        if not symbol:
            symbol = token

    if not symbol.isdigit():
        raise ValueError(f"unsupported China ticker format: {original}")
    symbol = symbol.zfill(6)

    if not exchange:
        if symbol.startswith("6"):
            exchange = "SHG"
            warnings.append(f"inferred SHG from ticker prefix for {original}")
        elif symbol.startswith(("0", "3")):
            exchange = "SHE"
            warnings.append(f"inferred SHE from ticker prefix for {original}")
        else:
            raise ValueError(f"cannot infer exchange for China ticker: {original}")

    if strict and warnings:
        raise ValueError("; ".join(warnings))

    mic = "XSHG" if exchange == "SHG" else "XSHE"
    provider_symbol = f"{symbol}.{exchange}"
    return CanonicalSymbol(symbol=symbol, exchange=exchange, mic=mic, provider_symbol=provider_symbol, warnings=tuple(warnings))


def _exchange_from_code(code: str) -> str:
    normalized = str(code or "").strip().upper()
    if normalized in SHANGHAI_SUFFIXES:
        return "SHG"
    if normalized in SHENZHEN_SUFFIXES:
        return "SHE"
    return ""


def parse_iso_date(value: str, *, label: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{label} must be YYYY-MM-DD: {text}") from exc


def source_label(source_version: str = "") -> str:
    version = str(source_version or "").strip()
    return f"{DEFAULT_SOURCE}:{version}" if version else DEFAULT_SOURCE


def convert_history_rows(
    rows: Iterable[dict[str, str]],
    *,
    source_version: str = "",
    strict: bool = False,
    control_dates: Iterable[str] = (),
    event_rows: Iterable[dict[str, str]] = (),
    latest_rows: Iterable[dict[str, str]] = (),
) -> tuple[list[dict[str, str]], ConversionReport]:
    report = ConversionReport(source_version=source_version)
    output_rows: list[dict[str, str]] = []
    seen_exact: dict[tuple[str, str, str, str], int] = {}
    periods: dict[tuple[str, str], list[tuple[date, date | None, dict[str, str]]]] = defaultdict(list)
    source = source_label(source_version)

    for row_index, row in enumerate(rows, start=2):
        report.rows_read += 1
        raw_symbol = first_value(row, "symbol", "Symbol")
        name = first_value(row, "name", "Name")
        start_raw = first_value(row, "opt-in", "start_date", "valid_from")
        end_raw = first_value(row, "opt-out", "end_date", "valid_to")
        try:
            canonical = canonicalize_cn_symbol(raw_symbol, strict=strict)
            start_date = parse_iso_date(start_raw, label=f"row {row_index} opt-in")
            end_date = parse_iso_date(end_raw, label=f"row {row_index} opt-out")
            if start_date is None:
                raise ValueError(f"row {row_index} opt-in is required")
            if end_date is not None and end_date < start_date:
                raise ValueError(f"row {row_index} opt-out before opt-in")
        except ValueError as exc:
            message = f"row {row_index} {raw_symbol}: {exc}"
            report.errors.append(message) if strict else report.warnings.append(message)
            if "date" in str(exc) or "opt-in" in str(exc) or "opt-out" in str(exc):
                report.invalid_dates.append(message)
            if not raw_symbol:
                report.unmappable_tickers.append(f"row {row_index}")
            else:
                report.unmappable_tickers.append(raw_symbol)
            continue

        for warning in canonical.warnings:
            report.warnings.append(f"row {row_index}: {warning}")

        out = {
            "universe_code": "CSI300",
            "symbol": canonical.symbol,
            "exchange": canonical.exchange,
            "mic": canonical.mic,
            "name": name,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat() if end_date else "",
            "weight": "",
            "provider_symbol": canonical.provider_symbol,
            "source": source,
            "country": "CN",
            "currency": "CNY",
            "sector": "",
            "industry": "",
        }
        exact_key = (canonical.symbol, canonical.exchange, start_date.isoformat(), out["end_date"])
        if exact_key in seen_exact:
            report.duplicate_exact_rows.append({"symbol": canonical.symbol, "exchange": canonical.exchange, "first_row": str(seen_exact[exact_key]), "duplicate_row": str(row_index)})
        else:
            seen_exact[exact_key] = row_index
        periods[(canonical.symbol, canonical.exchange)].append((start_date, end_date, {"symbol": canonical.symbol, "exchange": canonical.exchange, "row": str(row_index)}))
        output_rows.append(out)

    report.memberships_written = len(output_rows)
    report.distinct_tickers = len({(row["symbol"], row["exchange"]) for row in output_rows})
    report.exchange_distribution = dict(Counter(row["exchange"] for row in output_rows))
    report.event_rows_read = sum(1 for _ in event_rows)
    report.latest_rows_read = sum(1 for _ in latest_rows)

    start_dates = [row["start_date"] for row in output_rows]
    end_dates = [row["end_date"] for row in output_rows if row["end_date"]]
    report.min_start_date = min(start_dates) if start_dates else ""
    report.max_start_date = max(start_dates) if start_dates else ""
    report.max_end_date = max(end_dates) if end_dates else ""
    report.latest_active_members = sum(1 for row in output_rows if not row["end_date"])

    for key, items in periods.items():
        sorted_items = sorted(items, key=lambda item: item[0])
        previous_start: date | None = None
        previous_end: date | None = None
        previous_meta: dict[str, str] | None = None
        for start, end, meta in sorted_items:
            if previous_start is not None and previous_meta is not None:
                if previous_end is None or start <= previous_end:
                    report.overlapping_periods.append({
                        "symbol": key[0],
                        "exchange": key[1],
                        "previous_row": previous_meta["row"],
                        "row": meta["row"],
                        "previous_start": previous_start.isoformat(),
                        "previous_end": previous_end.isoformat() if previous_end else "",
                        "start": start.isoformat(),
                        "end": end.isoformat() if end else "",
                    })
            previous_start, previous_end, previous_meta = start, end, meta

    if report.duplicate_exact_rows:
        report.warnings.append(f"duplicate exact rows detected: {len(report.duplicate_exact_rows)}")
    if report.overlapping_periods:
        report.warnings.append(f"overlapping periods detected: {len(report.overlapping_periods)}")

    if strict:
        if report.duplicate_exact_rows:
            report.errors.append("strict mode: duplicate exact rows detected")
        if report.overlapping_periods:
            report.errors.append("strict mode: overlapping periods detected")
        if report.latest_active_members < 280 or report.latest_active_members > 320:
            report.errors.append(f"strict mode: latest active member count is not close to 300 ({report.latest_active_members})")
    elif report.latest_active_members < 280 or report.latest_active_members > 320:
        report.warnings.append(f"latest active member count is not close to 300 ({report.latest_active_members})")

    dates_to_count = set(control_dates or [])
    if report.min_start_date:
        dates_to_count.add(report.min_start_date)
    if report.max_start_date:
        dates_to_count.add(report.max_start_date)
    today = date.today().isoformat()
    dates_to_count.add(today)
    for year in sorted({row["start_date"][:4] for row in output_rows if row["start_date"]}):
        dates_to_count.add(f"{year}-12-31")
    report.active_counts = {
        value: active_count_on(output_rows, value)
        for value in sorted(dates_to_count)
        if _looks_like_date(value)
    }
    return output_rows, report


def first_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _looks_like_date(value: str) -> bool:
    try:
        date.fromisoformat(str(value))
    except ValueError:
        return False
    return True


def active_count_on(rows: Iterable[dict[str, str]], as_of: str) -> int:
    check_date = date.fromisoformat(as_of)
    active = 0
    for row in rows:
        start = date.fromisoformat(row["start_date"])
        end = parse_iso_date(row.get("end_date", ""), label="end_date")
        if start <= check_date and (end is None or check_date <= end):
            active += 1
    return active


def compare_yfiua_latest(stockalert_rows: Iterable[dict[str, str]], yfiua_rows: Iterable[dict[str, str]], *, strict: bool = False) -> dict[str, object]:
    stockalert_active = {
        f"{row['symbol']}.{row['exchange']}"
        for row in stockalert_rows
        if not row.get("end_date")
    }
    yfiua_symbols: set[str] = set()
    yfiua_warnings: list[str] = []
    for index, row in enumerate(yfiua_rows, start=2):
        raw_symbol = first_value(row, "Symbol", "symbol", "ticker")
        try:
            canonical = canonicalize_cn_symbol(raw_symbol, strict=strict)
        except ValueError as exc:
            yfiua_warnings.append(f"row {index} {raw_symbol}: {exc}")
            continue
        yfiua_symbols.add(f"{canonical.symbol}.{canonical.exchange}")
    return {
        "overlap_count": len(stockalert_active & yfiua_symbols),
        "only_in_unliftedq": sorted(stockalert_active - yfiua_symbols),
        "only_in_yfiua": sorted(yfiua_symbols - stockalert_active),
        "unliftedq_active_count": len(stockalert_active),
        "yfiua_count": len(yfiua_symbols),
        "warnings": yfiua_warnings,
    }


def write_stockalert_csv(rows: Iterable[dict[str, str]], output_path: str | Path) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=STOCKALERT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in STOCKALERT_COLUMNS})


def load_unliftedq_inputs(input_dir: str | Path | None, *, download: bool) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], dict[str, str]]:
    if input_dir:
        root = Path(input_dir)
        paths = {
            "history": root / "history" / "csi300.csv",
            "latest": root / "latest" / "csi300.csv",
            "event": root / "event" / "cn.csv",
        }
    elif download:
        paths = {
            "history": DEFAULT_HISTORY_URL,
            "latest": DEFAULT_LATEST_URL,
            "event": DEFAULT_EVENT_URL,
        }
    else:
        raise ValueError("Provide --input-dir or pass --download to read public GitHub raw URLs.")

    history = read_csv_rows(paths["history"])
    latest = read_csv_rows(paths["latest"]) if _source_exists(paths["latest"]) else []
    events = read_csv_rows(paths["event"]) if _source_exists(paths["event"]) else []
    return history, latest, events, {key: str(value) for key, value in paths.items()}


def _source_exists(path_or_url: str | Path) -> bool:
    if is_url(path_or_url):
        try:
            with urllib.request.urlopen(str(path_or_url), timeout=30) as response:
                return response.status < 400
        except Exception:
            return False
    return Path(path_or_url).exists()


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert unliftedq CSI300 history to StockAlert historical universe CSV.")
    parser.add_argument("--input-dir", help="Local clone/download root containing history/csi300.csv, latest/csi300.csv, event/cn.csv.")
    parser.add_argument("--download", action="store_true", help="Read the public unliftedq raw GitHub CSV URLs instead of --input-dir.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help=f"Output StockAlert CSV path. Default: {DEFAULT_OUTPUT}")
    parser.add_argument("--source-version", default="", help="Optional unliftedq tag/commit stored in the source column suffix.")
    parser.add_argument("--compare-yfiua-latest", default="", help="Optional yfiua latest CSV path or URL for overlap comparison.")
    parser.add_argument("--as-of", action="append", default=[], help="Additional YYYY-MM-DD date to count active members. Can be repeated.")
    parser.add_argument("--strict", action="store_true", help="Fail on unmappable/inferred tickers, overlaps, duplicates, or latest active count far from 300.")
    parser.add_argument("--report", default="", help="Optional JSON report output path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    try:
        history_rows, latest_rows, event_rows, sources = load_unliftedq_inputs(args.input_dir, download=args.download)
        stockalert_rows, report = convert_history_rows(
            history_rows,
            source_version=args.source_version,
            strict=args.strict,
            control_dates=args.as_of,
            latest_rows=latest_rows,
            event_rows=event_rows,
        )
        if args.compare_yfiua_latest:
            yfiua_rows = read_csv_rows(args.compare_yfiua_latest)
            report.yfiua_comparison = compare_yfiua_latest(stockalert_rows, yfiua_rows, strict=args.strict)
        report_dict = report.to_dict()
        report_dict["sources"] = sources
        write_stockalert_csv(stockalert_rows, args.output)
        if args.report:
            report_path = Path(args.report)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report_dict, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
        print_summary(report, output=args.output, report_path=args.report)
        return 0 if report.ok else 1
    except Exception as exc:
        parser.exit(2, f"error: {exc}\n")


def print_summary(report: ConversionReport, *, output: str, report_path: str = "") -> None:
    print("CSI300 StockAlert CSV conversion summary")
    print(f"output={output}")
    if report_path:
        print(f"report={report_path}")
    print(f"rows_read={report.rows_read}")
    print(f"memberships_written={report.memberships_written}")
    print(f"distinct_tickers={report.distinct_tickers}")
    print(f"min_start_date={report.min_start_date}")
    print(f"max_start_date={report.max_start_date}")
    print(f"max_end_date={report.max_end_date}")
    print(f"latest_active_members={report.latest_active_members}")
    print(f"exchange_distribution={report.exchange_distribution}")
    print(f"unmappable_tickers={len(report.unmappable_tickers)}")
    print(f"duplicate_exact_rows={len(report.duplicate_exact_rows)}")
    print(f"overlapping_periods={len(report.overlapping_periods)}")
    if report.yfiua_comparison:
        print(f"yfiua_overlap_count={report.yfiua_comparison.get('overlap_count')}")
        print(f"yfiua_only_in_unliftedq={len(report.yfiua_comparison.get('only_in_unliftedq', []))}")
        print(f"yfiua_only_in_yfiua={len(report.yfiua_comparison.get('only_in_yfiua', []))}")
    for warning in report.warnings[:20]:
        print(f"warning: {warning}")
    for error in report.errors[:20]:
        print(f"error: {error}")


if __name__ == "__main__":
    raise SystemExit(main())
