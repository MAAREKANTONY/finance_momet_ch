from __future__ import annotations

import csv
import tempfile
from pathlib import Path

from django.test import SimpleTestCase

from tools.convert_unliftedq_csi300_to_stockalert_csv import (
    STOCKALERT_COLUMNS,
    canonicalize_cn_symbol,
    compare_yfiua_latest,
    convert_history_rows,
    write_stockalert_csv,
)


class CSI300UnliftedqConverterTests(SimpleTestCase):
    def test_ticker_shanghai_yahoo_suffix_maps_to_stockalert_exchange(self):
        symbol = canonicalize_cn_symbol("600519.SS")

        self.assertEqual(symbol.symbol, "600519")
        self.assertEqual(symbol.exchange, "SHG")
        self.assertEqual(symbol.mic, "XSHG")
        self.assertEqual(symbol.provider_symbol, "600519.SHG")

    def test_ticker_shenzhen_yahoo_suffix_preserves_leading_zeroes(self):
        symbol = canonicalize_cn_symbol("000001.SZ")

        self.assertEqual(symbol.symbol, "000001")
        self.assertEqual(symbol.exchange, "SHE")
        self.assertEqual(symbol.mic, "XSHE")
        self.assertEqual(symbol.provider_symbol, "000001.SHE")

    def test_ticker_chinext_maps_to_shenzhen(self):
        symbol = canonicalize_cn_symbol("300750.SZ")

        self.assertEqual(symbol.symbol, "300750")
        self.assertEqual(symbol.exchange, "SHE")
        self.assertEqual(symbol.mic, "XSHE")

    def test_convert_history_preserves_start_and_blank_end_dates(self):
        rows = [
            {"symbol": "SH600519", "name": "Kweichow Moutai", "opt-in": "2020-01-01", "opt-out": ""},
            {"symbol": "SZ000001", "name": "Ping An Bank", "opt-in": "2020-01-01", "opt-out": "2023-06-30"},
        ]

        output, report = convert_history_rows(rows, source_version="abc123")

        self.assertTrue(report.ok)
        self.assertEqual(output[0]["start_date"], "2020-01-01")
        self.assertEqual(output[0]["end_date"], "")
        self.assertEqual(output[0]["source"], "unliftedq_index_constitution:abc123")
        self.assertEqual(output[1]["end_date"], "2023-06-30")

    def test_duplicate_exact_row_is_detected(self):
        rows = [
            {"symbol": "SH600519", "name": "Moutai", "opt-in": "2020-01-01", "opt-out": ""},
            {"symbol": "SH600519", "name": "Moutai", "opt-in": "2020-01-01", "opt-out": ""},
        ]

        _output, report = convert_history_rows(rows)

        self.assertEqual(len(report.duplicate_exact_rows), 1)
        self.assertIn("duplicate exact rows detected", " ".join(report.warnings))

    def test_overlapping_period_is_detected(self):
        rows = [
            {"symbol": "SH600519", "name": "Moutai", "opt-in": "2020-01-01", "opt-out": "2020-12-31"},
            {"symbol": "SH600519", "name": "Moutai", "opt-in": "2020-06-01", "opt-out": ""},
        ]

        _output, report = convert_history_rows(rows)

        self.assertEqual(len(report.overlapping_periods), 1)
        self.assertEqual(report.overlapping_periods[0]["symbol"], "600519")

    def test_missing_exchange_suffix_warns_by_default_and_errors_in_strict(self):
        symbol = canonicalize_cn_symbol("600519")
        self.assertEqual(symbol.exchange, "SHG")
        self.assertTrue(symbol.warnings)

        with self.assertRaisesRegex(ValueError, "inferred SHG"):
            canonicalize_cn_symbol("600519", strict=True)

    def test_output_csv_contains_stockalert_columns(self):
        rows = [{"symbol": "SZ000001", "name": "Ping An Bank", "opt-in": "2020-01-01", "opt-out": ""}]
        output, _report = convert_history_rows(rows)

        with tempfile.TemporaryDirectory() as tmp:
            output_path = Path(tmp) / "csi300.csv"
            write_stockalert_csv(output, output_path)
            with output_path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(reader.fieldnames, STOCKALERT_COLUMNS)
                row = next(reader)

        self.assertEqual(row["universe_code"], "CSI300")
        self.assertEqual(row["symbol"], "000001")
        self.assertEqual(row["exchange"], "SHE")

    def test_yfiua_comparison_accepts_yahoo_format(self):
        stockalert_rows, _report = convert_history_rows([
            {"symbol": "SH600519", "name": "Moutai", "opt-in": "2020-01-01", "opt-out": ""},
            {"symbol": "SZ000001", "name": "Ping An Bank", "opt-in": "2020-01-01", "opt-out": ""},
        ])
        yfiua_rows = [
            {"Symbol": "600519.SS", "Name": "Moutai"},
            {"Symbol": "000001.SZ", "Name": "Ping An Bank"},
            {"Symbol": "300750.SZ", "Name": "CATL"},
        ]

        comparison = compare_yfiua_latest(stockalert_rows, yfiua_rows)

        self.assertEqual(comparison["overlap_count"], 2)
        self.assertEqual(comparison["yfiua_count"], 3)
        self.assertEqual(comparison["only_in_yfiua"], ["300750.SHE"])
