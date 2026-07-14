#!/usr/bin/env python3
"""Convert the pinned unliftedq CSI300 history to StockAlert CSV format.

The upstream ``opt-in`` date is inclusive and ``opt-out`` is exclusive.
StockAlert stores inclusive ``start_date`` and ``end_date`` values, therefore
``end_date = opt-out - 1 day``.  The tool never reads or writes the Django
database and never calls a financial-data provider.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import tempfile
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable

try:
    from .csi300_policy import CSI300_SUPPORTED_HISTORY_START, CSI300_SUPPORTED_HISTORY_START_ISO
except ImportError:  # Direct execution: python tools/convert_unliftedq_csi300_to_stockalert_csv.py
    from csi300_policy import CSI300_SUPPORTED_HISTORY_START, CSI300_SUPPORTED_HISTORY_START_ISO

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

SOURCE_REPOSITORY = "https://github.com/unliftedq/index-constitution"
SOURCE_TAG = "v0.6.2"
SOURCE_COMMIT = "16d9d69fc0bf7f0f5e9aace868e16e26f2ecb5c2"
SOURCE_LICENSE = "MIT"
SOURCE_ATTRIBUTION = (
    "CSI300 membership source: unliftedq/index-constitution (MIT). "
    "Commercial redistribution rights for the underlying CSI data require separate validation."
)
SOURCE_PATHS = {
    "history": "history/csi300.csv",
    "latest": "latest/csi300.csv",
    "event": "event/cn.csv",
}
PINNED_SOURCE_URLS = {
    key: f"https://raw.githubusercontent.com/unliftedq/index-constitution/{SOURCE_COMMIT}/{path}"
    for key, path in SOURCE_PATHS.items()
}
EXPECTED_SOURCE_SHA256 = {
    "history": "6a6bca260f4752cbe555337369915794c752ecc0f70ee9b0d1bac6f83e7df1b8",
    "latest": "5f2e086ab3a0db35f807af34c38571d555aabc69612fa11c28d7c47498224aaf",
    "event": "060c54ee81403369a8522fc573de9243212975bcf58ea1be3aa3ecff6f4cd174",
}
EXPECTED_SOURCE_COLUMNS = {
    "history": {"symbol", "name", "opt-in", "opt-out"},
    "latest": {"symbol", "name", "opt-in"},
    "event": {"event_date", "event_type", "old_symbol", "new_symbol"},
}

DEFAULT_HISTORY_URL = PINNED_SOURCE_URLS["history"]
DEFAULT_LATEST_URL = PINNED_SOURCE_URLS["latest"]
DEFAULT_EVENT_URL = PINNED_SOURCE_URLS["event"]
DEFAULT_OUTPUT = "data/generated/csi300_stockalert_memberships.csv"
DEFAULT_SOURCE = "unliftedq_index_constitution"
MANDATORY_CONTROL_DATES = {
    CSI300_SUPPORTED_HISTORY_START_ISO,
    "2026-06-11",
    "2026-06-12",
    "2026-06-13",
}

SHANGHAI_SUFFIXES = {"SS", "SH", "SHG", "SSE", "XSHG"}
SHENZHEN_SUFFIXES = {"SZ", "SHE", "SZSE", "XSHE"}
SHANGHAI_PREFIXES = ("SH", "SSE", "XSHG")
SHENZHEN_PREFIXES = ("SZ", "SZSE", "XSHE")


class SourceValidationError(ValueError):
    """A pinned source or business validation failed (CLI exit code 1)."""


@dataclass(frozen=True)
class CanonicalSymbol:
    symbol: str
    exchange: str
    mic: str
    provider_symbol: str
    warnings: tuple[str, ...] = ()


@dataclass
class ConversionReport:
    source_repository: str = SOURCE_REPOSITORY
    source_tag: str = SOURCE_TAG
    source_commit: str = SOURCE_COMMIT
    source_license: str = SOURCE_LICENSE
    attribution_required: bool = True
    attribution: str = SOURCE_ATTRIBUTION
    pinned_urls: dict[str, str] = field(default_factory=lambda: dict(PINNED_SOURCE_URLS))
    checksums_expected: dict[str, str] = field(default_factory=lambda: dict(EXPECTED_SOURCE_SHA256))
    checksums_received: dict[str, str] = field(default_factory=dict)
    raw_checksums_received: dict[str, str] = field(default_factory=dict)
    requested: dict[str, str] = field(default_factory=dict)
    downloaded: dict[str, bool] = field(default_factory=dict)
    rows_read: int = 0
    supported_history_start: str = CSI300_SUPPORTED_HISTORY_START_ISO
    memberships_source_total: int = 0
    memberships_published: int = 0
    distinct_tickers_published: int = 0
    min_published_start_date: str = ""
    max_published_end_date: str = ""
    outside_supported_history_count: int = 0
    outside_supported_history: list[dict[str, str]] = field(default_factory=list)
    clipped_to_supported_start_count: int = 0
    clipped_to_supported_start: list[dict[str, str]] = field(default_factory=list)
    memberships_produced: int = 0
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
    rows_without_opt_in: list[dict[str, str]] = field(default_factory=list)
    repaired_rows: list[dict[str, str]] = field(default_factory=list)
    unconvertible_rows: list[dict[str, str]] = field(default_factory=list)
    ignored_rows: list[dict[str, str]] = field(default_factory=list)
    suspicious_company_names: list[dict[str, str]] = field(default_factory=list)
    warnings: list[dict[str, object]] = field(default_factory=list)
    errors: list[dict[str, object]] = field(default_factory=list)
    active_counts: dict[str, int] = field(default_factory=dict)
    control_date_checks: dict[str, dict[str, object]] = field(default_factory=dict)
    latest_history_validation: dict[str, object] = field(default_factory=dict)
    yfiua_comparison: dict[str, object] = field(default_factory=dict)
    source_version: str = SOURCE_COMMIT
    event_rows_read: int = 0
    latest_rows_read: int = 0
    status: str = "pending"

    @property
    def ok(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _add_warning(report: ConversionReport, code: str, message: str, **details: object) -> None:
    report.warnings.append({"code": code, "message": message, **details})


def _add_error(report: ConversionReport, code: str, message: str, **details: object) -> None:
    report.errors.append({"code": code, "message": message, **details})


def is_url(value: str | Path) -> bool:
    text = str(value)
    return text.startswith("https://") or text.startswith("http://")


def read_source_bytes(path_or_url: str | Path) -> bytes:
    if is_url(path_or_url):
        with urllib.request.urlopen(str(path_or_url), timeout=30) as response:
            return response.read()
    return Path(path_or_url).read_bytes()


def _canonical_source_bytes(raw: bytes) -> bytes:
    """Normalize only the optional UTF-8 BOM before the configured checksum."""
    return raw.decode("utf-8-sig").encode("utf-8")


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def read_text(path_or_url: str | Path) -> str:
    return read_source_bytes(path_or_url).decode("utf-8-sig")


def _csv_rows_from_bytes(raw: bytes) -> tuple[list[dict[str, str]], list[str]]:
    text = raw.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = [str(value or "").strip() for value in (reader.fieldnames or [])]
    rows = [
        {str(key or "").strip(): str(value or "").strip() for key, value in row.items()}
        for row in reader
    ]
    return rows, fieldnames


def read_csv_rows(path_or_url: str | Path) -> list[dict[str, str]]:
    rows, _fieldnames = _csv_rows_from_bytes(read_source_bytes(path_or_url))
    return rows


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
    return CanonicalSymbol(
        symbol=symbol,
        exchange=exchange,
        mic=mic,
        provider_symbol=f"{symbol}.{exchange}",
        warnings=tuple(warnings),
    )


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


def validate_source_version(source_version: str) -> str:
    requested = str(source_version or "").strip()
    if requested not in {SOURCE_COMMIT, SOURCE_TAG}:
        raise SourceValidationError(
            f"--source-version must be {SOURCE_COMMIT} or {SOURCE_TAG}; received {requested or '<empty>'}."
        )
    return SOURCE_COMMIT


def source_label(source_version: str = SOURCE_COMMIT) -> str:
    return f"{DEFAULT_SOURCE}:{validate_source_version(source_version)}"


def first_value(row: dict[str, str], *keys: str) -> str:
    for key in keys:
        value = str(row.get(key, "") or "").strip()
        if value:
            return value
    return ""


def _entry_date_indexes(
    latest_rows: Iterable[dict[str, str]],
    event_rows: Iterable[dict[str, str]],
) -> tuple[dict[str, set[date]], dict[str, set[date]]]:
    latest_dates: dict[str, set[date]] = defaultdict(set)
    event_dates: dict[str, set[date]] = defaultdict(set)
    for row in latest_rows:
        raw_symbol = first_value(row, "symbol", "Symbol")
        raw_date = first_value(row, "opt-in", "start_date", "valid_from")
        try:
            canonical = canonicalize_cn_symbol(raw_symbol)
            value = parse_iso_date(raw_date, label="latest opt-in")
        except ValueError:
            continue
        if value:
            latest_dates[canonical.provider_symbol].add(value)
    for row in event_rows:
        raw_symbol = first_value(row, "new_symbol")
        raw_date = first_value(row, "event_date")
        if not raw_symbol:
            continue
        try:
            canonical = canonicalize_cn_symbol(raw_symbol)
            value = parse_iso_date(raw_date, label="event date")
        except ValueError:
            continue
        if value:
            event_dates[canonical.provider_symbol].add(value)
    return latest_dates, event_dates


def _repair_missing_start(
    canonical: CanonicalSymbol,
    source_opt_out: date | None,
    latest_dates: dict[str, set[date]],
    event_dates: dict[str, set[date]],
) -> tuple[date | None, str]:
    key = canonical.provider_symbol
    # A latest row can identify only the start of the same still-open interval.
    if source_opt_out is None and len(latest_dates.get(key, set())) == 1:
        return next(iter(latest_dates[key])), "latest/csi300.csv opt-in"
    # A new_symbol event deterministically starts the membership under that symbol.
    candidates = {
        value
        for value in event_dates.get(key, set())
        if source_opt_out is None or value < source_opt_out
    }
    if len(candidates) == 1:
        return next(iter(candidates)), "event/cn.csv new_symbol event_date"
    return None, ""


def convert_history_rows(
    rows: Iterable[dict[str, str]],
    *,
    source_version: str = SOURCE_COMMIT,
    strict: bool = False,
    control_dates: Iterable[str] = (),
    event_rows: Iterable[dict[str, str]] = (),
    latest_rows: Iterable[dict[str, str]] = (),
    report: ConversionReport | None = None,
) -> tuple[list[dict[str, str]], ConversionReport]:
    report = report or ConversionReport()
    report.source_version = validate_source_version(source_version)
    output_rows: list[dict[str, str]] = []
    history_rows = list(rows)
    latest_rows = list(latest_rows)
    event_rows = list(event_rows)
    seen_exact: dict[tuple[str, str, str, str], int] = {}
    periods: dict[tuple[str, str], list[tuple[date, date | None, dict[str, str]]]] = defaultdict(list)
    source_dates: set[date] = set()
    source = f"{DEFAULT_SOURCE}:{SOURCE_COMMIT}"
    latest_dates, event_dates = _entry_date_indexes(latest_rows, event_rows)
    report.event_rows_read = len(event_rows)
    report.latest_rows_read = len(latest_rows)
    report.memberships_source_total = len(history_rows)

    for row_index, row in enumerate(history_rows, start=2):
        report.rows_read += 1
        raw_symbol = first_value(row, "symbol", "Symbol")
        name = first_value(row, "name", "Name")
        start_raw = first_value(row, "opt-in", "start_date", "valid_from")
        end_raw = first_value(row, "opt-out", "end_date", "valid_to")
        try:
            canonical = canonicalize_cn_symbol(raw_symbol, strict=strict)
            source_opt_out = parse_iso_date(end_raw, label=f"row {row_index} opt-out")
            start_date = parse_iso_date(start_raw, label=f"row {row_index} opt-in")
        except ValueError as exc:
            message = f"row {row_index} {raw_symbol}: {exc}"
            _add_error(report, "invalid_source_row", message, row=row_index, symbol=raw_symbol)
            if "date" in str(exc) or "opt-in" in str(exc) or "opt-out" in str(exc):
                report.invalid_dates.append(message)
            report.unmappable_tickers.append(raw_symbol or f"row {row_index}")
            continue

        end_date = source_opt_out - timedelta(days=1) if source_opt_out else None
        if start_date is not None and end_date is not None and end_date < start_date:
            message = (
                f"row {row_index} {raw_symbol}: inclusive end_date {end_date.isoformat()} "
                f"is before start_date {start_date.isoformat()}"
            )
            report.invalid_dates.append(message)
            _add_error(report, "invalid_membership_interval", message, row=row_index, symbol=raw_symbol)
            continue

        if start_date is None:
            missing = {
                "row": str(row_index),
                "symbol": raw_symbol,
                "provider_symbol": canonical.provider_symbol,
                "name": name,
                "opt_out": end_raw,
            }
            report.rows_without_opt_in.append(missing)
            if end_date is not None and end_date < CSI300_SUPPORTED_HISTORY_START:
                outside = {
                    "row": str(row_index),
                    "symbol": raw_symbol,
                    "provider_symbol": canonical.provider_symbol,
                    "name": name,
                    "source_opt_in": start_raw,
                    "source_opt_out": end_raw,
                    "inclusive_end_date": end_date.isoformat(),
                    "reason": "interval ends before CSI300_SUPPORTED_HISTORY_START",
                }
                report.outside_supported_history.append(outside)
                continue
            start_date, provenance = _repair_missing_start(
                canonical,
                source_opt_out,
                latest_dates,
                event_dates,
            )
            if start_date is not None:
                repaired = {**missing, "start_date": start_date.isoformat(), "provenance": provenance}
                report.repaired_rows.append(repaired)
                _add_warning(
                    report,
                    "missing_opt_in_repaired",
                    f"{raw_symbol} opt-in repaired from {provenance}.",
                    **repaired,
                )
            else:
                report.unconvertible_rows.append(missing)
                _add_error(
                    report,
                    "missing_opt_in",
                    f"row {row_index} {raw_symbol}: opt-in is missing and no deterministic pinned-source entry date exists.",
                    **missing,
                )
                continue

        if end_date is not None and end_date < CSI300_SUPPORTED_HISTORY_START:
            report.outside_supported_history.append({
                "row": str(row_index),
                "symbol": raw_symbol,
                "provider_symbol": canonical.provider_symbol,
                "name": name,
                "source_opt_in": start_raw,
                "source_opt_out": end_raw,
                "inclusive_end_date": end_date.isoformat(),
                "reason": "interval ends before CSI300_SUPPORTED_HISTORY_START",
            })
            continue

        source_start_date = start_date
        if start_date < CSI300_SUPPORTED_HISTORY_START:
            start_date = CSI300_SUPPORTED_HISTORY_START
            report.clipped_to_supported_start.append({
                "row": str(row_index),
                "symbol": raw_symbol,
                "provider_symbol": canonical.provider_symbol,
                "name": name,
                "source_start_date": source_start_date.isoformat(),
                "published_start_date": start_date.isoformat(),
                "end_date": end_date.isoformat() if end_date else "",
            })

        if source_opt_out:
            source_dates.add(source_opt_out)
        source_dates.add(start_date)

        for warning in canonical.warnings:
            _add_warning(report, "inferred_exchange", warning, row=row_index, symbol=raw_symbol)
        if name.isdigit():
            suspicious = {
                "symbol": canonical.provider_symbol,
                "source_value": name,
                "reason": "company name is purely numeric",
                "row": str(row_index),
            }
            report.suspicious_company_names.append(suspicious)
            _add_warning(
                report,
                "suspicious_company_name",
                f"{canonical.provider_symbol} has a purely numeric source company name: {name}.",
                **suspicious,
            )

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
        exact_key = (canonical.symbol, canonical.exchange, out["start_date"], out["end_date"])
        if exact_key in seen_exact:
            report.duplicate_exact_rows.append({
                "symbol": canonical.symbol,
                "exchange": canonical.exchange,
                "first_row": str(seen_exact[exact_key]),
                "duplicate_row": str(row_index),
            })
        else:
            seen_exact[exact_key] = row_index
        periods[(canonical.symbol, canonical.exchange)].append(
            (start_date, end_date, {"symbol": canonical.symbol, "exchange": canonical.exchange, "row": str(row_index)})
        )
        output_rows.append(out)

    report.memberships_produced = len(output_rows)
    report.memberships_published = len(output_rows)
    report.distinct_tickers = len({(row["symbol"], row["exchange"]) for row in output_rows})
    report.distinct_tickers_published = report.distinct_tickers
    report.outside_supported_history_count = len(report.outside_supported_history)
    report.clipped_to_supported_start_count = len(report.clipped_to_supported_start)
    report.exchange_distribution = dict(Counter(row["exchange"] for row in output_rows))
    starts = [row["start_date"] for row in output_rows]
    ends = [row["end_date"] for row in output_rows if row["end_date"]]
    report.min_start_date = min(starts) if starts else ""
    report.max_start_date = max(starts) if starts else ""
    report.max_end_date = max(ends) if ends else ""
    report.min_published_start_date = report.min_start_date
    report.max_published_end_date = report.max_end_date
    report.latest_active_members = sum(1 for row in output_rows if not row["end_date"])

    unsupported_rows = [
        row for row in output_rows
        if row["start_date"] < CSI300_SUPPORTED_HISTORY_START_ISO
        or (row.get("end_date") and row["end_date"] < CSI300_SUPPORTED_HISTORY_START_ISO)
    ]
    if unsupported_rows:
        _add_error(
            report,
            "published_outside_supported_history",
            "Published memberships must not precede CSI300_SUPPORTED_HISTORY_START.",
            count=len(unsupported_rows),
        )

    for key, items in periods.items():
        previous_start: date | None = None
        previous_end: date | None = None
        previous_meta: dict[str, str] | None = None
        for start, end, meta in sorted(items, key=lambda item: item[0]):
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
        _add_error(
            report,
            "duplicate_exact_rows",
            f"duplicate exact rows detected: {len(report.duplicate_exact_rows)}",
            count=len(report.duplicate_exact_rows),
        )
    if report.overlapping_periods:
        _add_error(
            report,
            "overlapping_periods",
            f"overlapping periods detected: {len(report.overlapping_periods)}",
            count=len(report.overlapping_periods),
        )

    _validate_latest_against_history(output_rows, latest_rows, report, strict=strict)
    _build_control_date_checks(output_rows, report, control_dates, source_dates)
    report.status = "valid" if report.ok else "failed"
    return output_rows, report


def _validate_latest_against_history(
    output_rows: list[dict[str, str]],
    latest_rows: list[dict[str, str]],
    report: ConversionReport,
    *,
    strict: bool,
) -> None:
    if not latest_rows:
        if strict:
            _add_error(report, "missing_latest_source", "latest/csi300.csv is empty.")
        return
    latest_symbols: list[str] = []
    latest_errors: list[str] = []
    for index, row in enumerate(latest_rows, start=2):
        raw_symbol = first_value(row, "symbol", "Symbol")
        try:
            canonical = canonicalize_cn_symbol(raw_symbol, strict=strict)
            start = parse_iso_date(first_value(row, "opt-in", "start_date", "valid_from"), label=f"latest row {index} opt-in")
            if start is None:
                raise ValueError("opt-in is required")
        except ValueError as exc:
            latest_errors.append(f"row {index} {raw_symbol}: {exc}")
            continue
        latest_symbols.append(canonical.provider_symbol)

    latest_set = set(latest_symbols)
    active_set = {
        row["provider_symbol"]
        for row in output_rows
        if not row.get("end_date")
    }
    missing = sorted(latest_set - active_set)
    unexpected = sorted(active_set - latest_set)
    duplicate_count = len(latest_symbols) - len(latest_set)
    matches = (
        len(latest_rows) == 300
        and len(latest_set) == 300
        and not latest_errors
        and not missing
        and not unexpected
    )
    report.latest_history_validation = {
        "latest_row_count": len(latest_rows),
        "latest_distinct_count": len(latest_set),
        "history_active_count": len(active_set),
        "duplicate_latest_symbols": duplicate_count,
        "missing_from_history_active": missing,
        "unexpected_history_active": unexpected,
        "errors": latest_errors,
        "matches": matches,
    }
    if len(latest_rows) != 300 or len(latest_set) != 300:
        _add_error(
            report,
            "latest_member_count",
            f"latest/csi300.csv must contain exactly 300 distinct members; rows={len(latest_rows)} distinct={len(latest_set)}.",
        )
    if latest_errors:
        _add_error(report, "invalid_latest_rows", "latest/csi300.csv contains invalid rows.", details=latest_errors)
    if missing or unexpected:
        _add_error(
            report,
            "latest_history_mismatch",
            "latest constituents do not match the active historical memberships.",
            missing_from_history_active=missing,
            unexpected_history_active=unexpected,
        )


def _build_control_date_checks(
    rows: list[dict[str, str]],
    report: ConversionReport,
    requested_dates: Iterable[str],
    source_dates: set[date],
) -> None:
    dates = set(MANDATORY_CONTROL_DATES)
    for raw in requested_dates:
        if _looks_like_date(raw):
            dates.add(str(raw))
        else:
            _add_error(report, "invalid_control_date", f"control date must be YYYY-MM-DD: {raw}")
    if report.max_start_date:
        rebalance = date.fromisoformat(report.max_start_date)
        dates.add((rebalance - timedelta(days=1)).isoformat())
        dates.add(rebalance.isoformat())
        dates.add((rebalance + timedelta(days=1)).isoformat())
    if source_dates:
        dates.add(max(source_dates).isoformat())

    checks: dict[str, dict[str, object]] = {}
    for value in sorted(dates):
        day = date.fromisoformat(value)
        entrants = sorted({row["provider_symbol"] for row in rows if row["start_date"] == value})
        sortants = sorted({
            row["provider_symbol"]
            for row in rows
            if row.get("end_date") and date.fromisoformat(row["end_date"]) + timedelta(days=1) == day
        })
        count = active_count_on(rows, value)
        warnings = []
        if count < 280 or count > 320:
            warnings.append("active_count_outside_expected_recent_range")
        checks[value] = {
            "active_count": count,
            "entrants": entrants,
            "sortants": sortants,
            "warnings": warnings,
        }
    report.control_date_checks = checks
    report.active_counts = {value: details["active_count"] for value, details in checks.items()}


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


def compare_yfiua_latest(
    stockalert_rows: Iterable[dict[str, str]],
    yfiua_rows: Iterable[dict[str, str]],
    *,
    strict: bool = False,
) -> dict[str, object]:
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
        yfiua_symbols.add(canonical.provider_symbol)
    return {
        "overlap_count": len(stockalert_active & yfiua_symbols),
        "only_in_unliftedq": sorted(stockalert_active - yfiua_symbols),
        "only_in_yfiua": sorted(yfiua_symbols - stockalert_active),
        "unliftedq_active_count": len(stockalert_active),
        "yfiua_count": len(yfiua_symbols),
        "warnings": yfiua_warnings,
    }


def _render_stockalert_csv(rows: Iterable[dict[str, str]]) -> bytes:
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(handle, fieldnames=STOCKALERT_COLUMNS, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({column: row.get(column, "") for column in STOCKALERT_COLUMNS})
    return handle.getvalue().encode("utf-8")


def _stage_payload(path: Path, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp_path = Path(temp_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise
    return temp_path


def _restore_payload(path: Path, payload: bytes) -> None:
    restore_path = _stage_payload(path, payload)
    try:
        os.replace(restore_path, path)
    finally:
        restore_path.unlink(missing_ok=True)


def _publish_payloads(payloads: list[tuple[Path, bytes]]) -> None:
    staged: list[tuple[Path, Path, bytes | None]] = []
    replaced: list[tuple[Path, bytes | None]] = []
    try:
        for path, payload in payloads:
            previous = path.read_bytes() if path.exists() else None
            staged.append((path, _stage_payload(path, payload), previous))
        for path, temp_path, previous in staged:
            os.replace(temp_path, path)
            replaced.append((path, previous))
        for directory in {path.parent for path, _payload in payloads}:
            try:
                descriptor = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
            except OSError:
                pass
    except Exception:
        for path, previous in reversed(replaced):
            if previous is None:
                path.unlink(missing_ok=True)
            else:
                _restore_payload(path, previous)
        raise
    finally:
        for _path, temp_path, _previous in staged:
            temp_path.unlink(missing_ok=True)


def write_stockalert_csv(rows: Iterable[dict[str, str]], output_path: str | Path) -> None:
    _publish_payloads([(Path(output_path), _render_stockalert_csv(rows))])


def _render_report(report: ConversionReport) -> bytes:
    return (json.dumps(report.to_dict(), indent=2, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")


def write_report(report: ConversionReport, report_path: str | Path) -> None:
    _publish_payloads([(Path(report_path), _render_report(report))])


def publish_success(
    rows: Iterable[dict[str, str]],
    output_path: str | Path,
    report: ConversionReport,
    report_path: str | Path | None,
) -> None:
    payloads = [(Path(output_path), _render_stockalert_csv(rows))]
    if report_path:
        payloads.append((Path(report_path), _render_report(report)))
    _publish_payloads(payloads)


def load_unliftedq_inputs(
    input_dir: str | Path | None,
    *,
    download: bool,
    report: ConversionReport | None = None,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], dict[str, str]]:
    report = report or ConversionReport()
    if input_dir:
        root = Path(input_dir)
        sources: dict[str, str | Path] = {key: root / relative for key, relative in SOURCE_PATHS.items()}
    elif download:
        sources = dict(PINNED_SOURCE_URLS)
    else:
        raise SourceValidationError("Provide --input-dir or pass --download.")

    raw_payloads: dict[str, bytes] = {}
    fieldnames: dict[str, list[str]] = {}
    parsed: dict[str, list[dict[str, str]]] = {}
    report.requested = {key: str(value) for key, value in sources.items()}
    report.downloaded = {key: is_url(value) for key, value in sources.items()}
    try:
        for key, source in sources.items():
            raw_payloads[key] = read_source_bytes(source)
    except (OSError, UnicodeError, urllib.error.URLError) as exc:
        _add_error(report, "source_read_error", f"Unable to read pinned source: {exc}")
        raise SourceValidationError(str(exc)) from exc

    report.raw_checksums_received = {key: _sha256(payload) for key, payload in raw_payloads.items()}
    try:
        canonical_payloads = {key: _canonical_source_bytes(payload) for key, payload in raw_payloads.items()}
    except UnicodeError as exc:
        _add_error(report, "source_encoding_error", f"Pinned source is not valid UTF-8: {exc}")
        raise SourceValidationError(str(exc)) from exc
    report.checksums_received = {key: _sha256(payload) for key, payload in canonical_payloads.items()}
    mismatches = {
        key: {"expected": EXPECTED_SOURCE_SHA256[key], "received": report.checksums_received.get(key, "")}
        for key in SOURCE_PATHS
        if report.checksums_received.get(key) != EXPECTED_SOURCE_SHA256[key]
    }
    if mismatches:
        _add_error(report, "checksum_mismatch", "Pinned source checksum mismatch.", files=mismatches)
        raise SourceValidationError("Pinned source checksum mismatch.")

    for key, payload in raw_payloads.items():
        parsed[key], fieldnames[key] = _csv_rows_from_bytes(payload)
        missing_columns = sorted(EXPECTED_SOURCE_COLUMNS[key] - set(fieldnames[key]))
        if missing_columns:
            _add_error(
                report,
                "source_columns_missing",
                f"{SOURCE_PATHS[key]} is missing required columns: {', '.join(missing_columns)}",
                source=key,
                missing_columns=missing_columns,
            )
    if report.errors:
        raise SourceValidationError("Pinned source columns are invalid.")
    return parsed["history"], parsed["latest"], parsed["event"], report.requested


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Convert pinned unliftedq CSI300 history to StockAlert historical universe CSV.")
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--input-dir", help="Offline root containing history/csi300.csv, latest/csi300.csv, event/cn.csv.")
    source_group.add_argument("--download", action="store_true", help=f"Download the three public CSVs pinned to {SOURCE_COMMIT}.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help=f"Output StockAlert CSV path. Default: {DEFAULT_OUTPUT}")
    parser.add_argument(
        "--source-version",
        default=SOURCE_COMMIT,
        help=f"Must identify the configured source ({SOURCE_TAG} or {SOURCE_COMMIT}). URLs always use the full commit.",
    )
    parser.add_argument("--compare-yfiua-latest", default="", help="Optional yfiua latest CSV path or URL for non-authoritative overlap comparison.")
    parser.add_argument("--as-of", action="append", default=[], help="Additional YYYY-MM-DD date to count active members. Can be repeated.")
    parser.add_argument("--strict", action="store_true", help="Fail on inferred tickers and all blocking membership validations.")
    parser.add_argument("--report", default="", help="Optional JSON report output path; failure reports never mark the CSV valid.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    report = ConversionReport()
    try:
        validate_source_version(args.source_version)
        history_rows, latest_rows, event_rows, _sources = load_unliftedq_inputs(
            args.input_dir,
            download=args.download,
            report=report,
        )
        stockalert_rows, report = convert_history_rows(
            history_rows,
            source_version=SOURCE_COMMIT,
            strict=args.strict,
            control_dates=args.as_of,
            latest_rows=latest_rows,
            event_rows=event_rows,
            report=report,
        )
        if args.compare_yfiua_latest:
            yfiua_rows = read_csv_rows(args.compare_yfiua_latest)
            report.yfiua_comparison = compare_yfiua_latest(stockalert_rows, yfiua_rows, strict=args.strict)
        report.status = "valid" if report.ok else "failed"
        if not report.ok:
            if args.report:
                write_report(report, args.report)
            print_summary(report, output=args.output, report_path=args.report)
            return 1
        report.memberships_written = len(stockalert_rows)
        publish_success(stockalert_rows, args.output, report, args.report or None)
        print_summary(report, output=args.output, report_path=args.report)
        return 0
    except SourceValidationError as exc:
        if not report.errors:
            _add_error(report, "source_validation_error", str(exc))
        report.status = "failed"
        if args.report:
            write_report(report, args.report)
        print_summary(report, output=args.output, report_path=args.report)
        return 1
    except Exception as exc:
        parser.exit(2, f"error: {exc}\n")


def print_summary(report: ConversionReport, *, output: str, report_path: str = "") -> None:
    print("CSI300 StockAlert CSV conversion summary")
    print(f"status={report.status}")
    print(f"output={output}")
    if report_path:
        print(f"report={report_path}")
    print(f"source_commit={report.source_commit}")
    print(f"rows_read={report.rows_read}")
    print(f"supported_history_start={report.supported_history_start}")
    print(f"outside_supported_history_count={report.outside_supported_history_count}")
    print(f"clipped_to_supported_start_count={report.clipped_to_supported_start_count}")
    print(f"memberships_produced={report.memberships_produced}")
    print(f"memberships_written={report.memberships_written}")
    print(f"distinct_tickers={report.distinct_tickers}")
    print(f"latest_active_members={report.latest_active_members}")
    print(f"rows_without_opt_in={len(report.rows_without_opt_in)}")
    print(f"repaired_rows={len(report.repaired_rows)}")
    print(f"unconvertible_rows={len(report.unconvertible_rows)}")
    print(f"suspicious_company_names={len(report.suspicious_company_names)}")
    print(f"duplicate_exact_rows={len(report.duplicate_exact_rows)}")
    print(f"overlapping_periods={len(report.overlapping_periods)}")
    if report.yfiua_comparison:
        print(f"yfiua_overlap_count={report.yfiua_comparison.get('overlap_count')}")
    for warning in report.warnings[:20]:
        print(f"warning[{warning.get('code')}]: {warning.get('message')}")
    for error in report.errors[:20]:
        print(f"error[{error.get('code')}]: {error.get('message')}")


if __name__ == "__main__":
    raise SystemExit(main())
