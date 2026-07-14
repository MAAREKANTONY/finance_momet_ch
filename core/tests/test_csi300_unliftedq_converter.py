from __future__ import annotations

import csv
import hashlib
import io
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

from django.test import SimpleTestCase

from tools import convert_unliftedq_csi300_to_stockalert_csv as converter
from tools.convert_unliftedq_csi300_to_stockalert_csv import (
    EXPECTED_SOURCE_SHA256,
    PINNED_SOURCE_URLS,
    SOURCE_COMMIT,
    SOURCE_LICENSE,
    SOURCE_REPOSITORY,
    SOURCE_TAG,
    STOCKALERT_COLUMNS,
    ConversionReport,
    active_count_on,
    canonicalize_cn_symbol,
    compare_yfiua_latest,
    convert_history_rows,
    load_unliftedq_inputs,
    main,
    publish_success,
    write_stockalert_csv,
)


class CSI300UnliftedqConverterTests(SimpleTestCase):
    @staticmethod
    def _active_history(count: int = 300) -> list[dict[str, str]]:
        return [
            {
                "symbol": f"SH{600000 + index:06d}",
                "name": f"公司{index}",
                "opt-in": "2020-01-02",
                "opt-out": "",
            }
            for index in range(count)
        ]

    @staticmethod
    def _latest_from_history(rows: list[dict[str, str]]) -> list[dict[str, str]]:
        return [
            {"symbol": row["symbol"], "name": row["name"], "opt-in": row["opt-in"]}
            for row in rows
            if not row.get("opt-out")
        ]

    @staticmethod
    def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]], *, bom: bool = False) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8-sig" if bom else "utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _write_source_tree(
        self,
        root: Path,
        *,
        history: list[dict[str, str]] | None = None,
        latest: list[dict[str, str]] | None = None,
        events: list[dict[str, str]] | None = None,
        history_bom: bool = True,
    ) -> dict[str, str]:
        history = history if history is not None else self._active_history()
        latest = latest if latest is not None else self._latest_from_history(history)
        events = events or []
        self._write_csv(root / "history" / "csi300.csv", ["symbol", "name", "opt-in", "opt-out"], history, bom=history_bom)
        self._write_csv(root / "latest" / "csi300.csv", ["symbol", "name", "opt-in"], latest)
        self._write_csv(
            root / "event" / "cn.csv",
            ["event_date", "event_type", "old_symbol", "new_symbol", "old_name", "new_name", "source_url", "notes"],
            events,
        )
        checksums = {}
        for key, relative in converter.SOURCE_PATHS.items():
            raw = (root / relative).read_bytes()
            checksums[key] = hashlib.sha256(raw.decode("utf-8-sig").encode("utf-8")).hexdigest()
        return checksums

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

    def test_opt_out_is_converted_to_previous_inclusive_day(self):
        rows = [
            {"symbol": "SH600519", "name": "贵州茅台", "opt-in": "2020-01-01", "opt-out": ""},
            {"symbol": "SZ000001", "name": "平安银行", "opt-in": "2020-01-01", "opt-out": "2023-06-30"},
        ]

        output, report = convert_history_rows(rows)

        self.assertTrue(report.ok)
        self.assertEqual(output[0]["end_date"], "")
        self.assertEqual(output[0]["source"], f"unliftedq_index_constitution:{SOURCE_COMMIT}")
        self.assertEqual(output[1]["end_date"], "2023-06-29")

    def test_outgoing_is_inactive_and_incoming_active_on_rebalance_day(self):
        output, _report = convert_history_rows([
            {"symbol": "SH600000", "name": "sortant", "opt-in": "2020-01-01", "opt-out": "2026-06-12"},
            {"symbol": "SZ300000", "name": "entrant", "opt-in": "2026-06-12", "opt-out": ""},
        ])

        self.assertEqual(active_count_on(output, "2026-06-11"), 1)
        self.assertEqual(active_count_on(output, "2026-06-12"), 1)
        self.assertEqual(active_count_on(output, "2026-06-13"), 1)
        active_on_rebalance = {
            row["provider_symbol"]
            for row in output
            if row["start_date"] <= "2026-06-12" and (not row["end_date"] or row["end_date"] >= "2026-06-12")
        }
        self.assertEqual(active_on_rebalance, {"300000.SHE"})

    def test_rebalance_removes_the_319_member_overlap(self):
        permanent = self._active_history(281)
        outgoing = [
            {"symbol": f"SH{600281 + index:06d}", "name": f"sortant{index}", "opt-in": "2020-01-02", "opt-out": "2026-06-12"}
            for index in range(19)
        ]
        incoming = [
            {"symbol": f"SZ{300000 + index:06d}", "name": f"entrant{index}", "opt-in": "2026-06-12", "opt-out": ""}
            for index in range(19)
        ]

        output, report = convert_history_rows(permanent + outgoing + incoming)

        self.assertEqual(active_count_on(output, "2026-06-11"), 300)
        self.assertEqual(active_count_on(output, "2026-06-12"), 300)
        self.assertEqual(active_count_on(output, "2026-06-13"), 300)
        check = report.control_date_checks["2026-06-12"]
        self.assertEqual(check["active_count"], 300)
        self.assertEqual(len(check["entrants"]), 19)
        self.assertEqual(len(check["sortants"]), 19)

    def test_latest_has_exactly_300_members_and_matches_active_history(self):
        history = self._active_history()
        latest = self._latest_from_history(history)

        _output, report = convert_history_rows(history, latest_rows=latest, strict=True)

        self.assertTrue(report.ok)
        self.assertEqual(report.latest_active_members, 300)
        self.assertTrue(report.latest_history_validation["matches"])
        self.assertEqual(report.latest_history_validation["latest_row_count"], 300)

    def test_missing_opt_in_is_repaired_only_from_matching_open_latest_interval(self):
        history = self._active_history()
        history[0]["opt-in"] = ""
        latest = self._latest_from_history(self._active_history())

        output, report = convert_history_rows(history, latest_rows=latest, strict=True)

        self.assertTrue(report.ok)
        self.assertEqual(output[0]["start_date"], "2020-01-02")
        self.assertEqual(len(report.rows_without_opt_in), 1)
        self.assertEqual(len(report.repaired_rows), 1)
        self.assertEqual(report.repaired_rows[0]["provenance"], "latest/csi300.csv opt-in")
        self.assertIn("missing_opt_in_repaired", {warning["code"] for warning in report.warnings})

    def test_four_real_missing_opt_in_rows_are_explicit_blocking_errors(self):
        rows = [
            {"symbol": "SH600312", "name": "平高电气", "opt-in": "", "opt-out": "2012-01-01"},
            {"symbol": "SH600501", "name": "航天晨光", "opt-in": "", "opt-out": "2008-06-14"},
            {"symbol": "SH600549", "name": "厦门钨业", "opt-in": "", "opt-out": "2019-06-17"},
            {"symbol": "SH600786", "name": "东方锅炉", "opt-in": "", "opt-out": "2008-06-14"},
        ]
        latest = [{"symbol": "SH600549", "name": "厦门钨业", "opt-in": "2026-06-12"}]

        output, report = convert_history_rows(rows, latest_rows=latest)

        self.assertEqual(output, [])
        self.assertFalse(report.ok)
        self.assertEqual(len(report.rows_without_opt_in), 4)
        self.assertEqual(len(report.unconvertible_rows), 4)
        self.assertEqual(report.repaired_rows, [])
        self.assertEqual(report.ignored_rows, [])
        self.assertEqual(
            {entry["provider_symbol"] for entry in report.unconvertible_rows},
            {"600312.SHG", "600501.SHG", "600549.SHG", "600786.SHG"},
        )
        self.assertEqual(sum(error["code"] == "missing_opt_in" for error in report.errors), 4)

    def test_numeric_company_name_is_preserved_and_warned(self):
        output, report = convert_history_rows([
            {"symbol": "SH601006", "name": "000780", "opt-in": "2006-08-12", "opt-out": ""},
        ])

        self.assertTrue(report.ok)
        self.assertEqual(output[0]["name"], "000780")
        self.assertEqual(report.suspicious_company_names, [{
            "symbol": "601006.SHG",
            "source_value": "000780",
            "reason": "company name is purely numeric",
            "row": "2",
        }])
        self.assertIn("suspicious_company_name", {warning["code"] for warning in report.warnings})

    def test_duplicate_and_overlapping_intervals_are_blocking(self):
        rows = [
            {"symbol": "SH600519", "name": "茅台", "opt-in": "2020-01-01", "opt-out": "2021-01-01"},
            {"symbol": "SH600519", "name": "茅台", "opt-in": "2020-01-01", "opt-out": "2021-01-01"},
            {"symbol": "SH600519", "name": "茅台", "opt-in": "2020-06-01", "opt-out": ""},
        ]

        _output, report = convert_history_rows(rows)

        self.assertFalse(report.ok)
        self.assertEqual(len(report.duplicate_exact_rows), 1)
        self.assertTrue(report.overlapping_periods)
        self.assertIn("duplicate_exact_rows", {error["code"] for error in report.errors})
        self.assertIn("overlapping_periods", {error["code"] for error in report.errors})

    def test_exclusive_opt_out_that_precedes_start_is_blocking(self):
        output, report = convert_history_rows([
            {"symbol": "SH600519", "name": "茅台", "opt-in": "2020-01-01", "opt-out": "2020-01-01"},
        ])

        self.assertEqual(output, [])
        self.assertFalse(report.ok)
        self.assertIn("invalid_membership_interval", {error["code"] for error in report.errors})

    def test_checksum_validation_accepts_optional_history_bom(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checksums = self._write_source_tree(root, history_bom=True)
            report = ConversionReport()
            with patch.dict(converter.EXPECTED_SOURCE_SHA256, checksums, clear=True):
                history, latest, events, _sources = load_unliftedq_inputs(root, download=False, report=report)

        self.assertEqual(len(history), 300)
        self.assertEqual(len(latest), 300)
        self.assertEqual(events, [])
        self.assertEqual(report.checksums_received, checksums)
        self.assertNotEqual(report.raw_checksums_received["history"], checksums["history"])

    def test_download_uses_only_full_sha_urls_and_checksums(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checksums = self._write_source_tree(root)
            payloads = {
                PINNED_SOURCE_URLS[key]: (root / relative).read_bytes()
                for key, relative in converter.SOURCE_PATHS.items()
            }

            def fake_urlopen(url, timeout):
                self.assertEqual(timeout, 30)
                return io.BytesIO(payloads[url])

            report = ConversionReport()
            with patch.dict(converter.EXPECTED_SOURCE_SHA256, checksums, clear=True), patch(
                "tools.convert_unliftedq_csi300_to_stockalert_csv.urllib.request.urlopen",
                side_effect=fake_urlopen,
            ) as urlopen_mock:
                load_unliftedq_inputs(None, download=True, report=report)

        self.assertTrue(all(SOURCE_COMMIT in url for url in PINNED_SOURCE_URLS.values()))
        self.assertEqual({call.args[0] for call in urlopen_mock.call_args_list}, set(PINNED_SOURCE_URLS.values()))
        self.assertEqual(report.downloaded, {"history": True, "latest": True, "event": True})

    def test_checksum_mismatch_is_exit_one_and_never_publishes_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checksums = self._write_source_tree(root)
            checksums["history"] = "0" * 64
            output = root / "out.csv"
            report_path = root / "report.json"
            with patch.dict(converter.EXPECTED_SOURCE_SHA256, checksums, clear=True):
                exit_code = main(["--input-dir", str(root), "--output", str(output), "--report", str(report_path), "--strict"])

            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(output.exists())
        self.assertEqual(report["status"], "failed")
        self.assertIn("checksum_mismatch", {error["code"] for error in report["errors"]})

    def test_offline_mode_never_opens_network_and_output_is_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checksums = self._write_source_tree(root)
            first = root / "first.csv"
            second = root / "second.csv"
            first_report = root / "first.json"
            second_report = root / "second.json"
            with patch.dict(converter.EXPECTED_SOURCE_SHA256, checksums, clear=True), patch(
                "tools.convert_unliftedq_csi300_to_stockalert_csv.urllib.request.urlopen",
                side_effect=AssertionError("offline mode attempted network access"),
            ):
                first_exit = main(["--input-dir", str(root), "--output", str(first), "--report", str(first_report), "--strict"])
                second_exit = main(["--input-dir", str(root), "--output", str(second), "--report", str(second_report), "--strict"])

            first_payload = first.read_bytes()
            second_payload = second.read_bytes()

        self.assertEqual((first_exit, second_exit), (0, 0))
        self.assertEqual(first_payload, second_payload)

    def test_invalid_conversion_preserves_old_final_and_cleans_temporary_files(self):
        history = self._active_history()
        history.insert(0, {
            "symbol": "SH600000",
            "name": "ancien intervalle sans entrée",
            "opt-in": "",
            "opt-out": "2019-01-01",
        })
        latest = self._latest_from_history(self._active_history())
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            checksums = self._write_source_tree(root, history=history, latest=latest)
            output = root / "out.csv"
            output.write_text("OLD CSV\n", encoding="utf-8")
            with patch.dict(converter.EXPECTED_SOURCE_SHA256, checksums, clear=True):
                exit_code = main(["--input-dir", str(root), "--output", str(output), "--strict"])
            preserved = output.read_text(encoding="utf-8")
            temporary_files = list(root.glob(".*.tmp"))

        self.assertEqual(exit_code, 1)
        self.assertEqual(preserved, "OLD CSV\n")
        self.assertEqual(temporary_files, [])

    def test_atomic_multi_file_publication_rolls_back_on_second_replace_failure(self):
        rows, report = convert_history_rows([
            {"symbol": "SH600519", "name": "贵州茅台", "opt-in": "2020-01-01", "opt-out": ""},
        ])
        report.status = "valid"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "out.csv"
            report_path = root / "report.json"
            output.write_bytes(b"OLD CSV")
            report_path.write_bytes(b"OLD REPORT")
            real_replace = converter.os.replace
            calls = 0

            def fail_second_replace(source, destination):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated report publication failure")
                return real_replace(source, destination)

            with patch("tools.convert_unliftedq_csi300_to_stockalert_csv.os.replace", side_effect=fail_second_replace):
                with self.assertRaisesRegex(OSError, "simulated"):
                    publish_success(rows, output, report, report_path)

            self.assertEqual(output.read_bytes(), b"OLD CSV")
            self.assertEqual(report_path.read_bytes(), b"OLD REPORT")
            self.assertEqual(list(root.glob(".*.tmp")), [])

    def test_successful_csv_write_uses_atomic_replace_and_expected_columns(self):
        rows, _report = convert_history_rows([
            {"symbol": "SZ000001", "name": "平安银行", "opt-in": "2020-01-01", "opt-out": ""},
        ])
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "csi300.csv"
            real_replace = converter.os.replace
            with patch("tools.convert_unliftedq_csi300_to_stockalert_csv.os.replace", wraps=real_replace) as replace_mock:
                write_stockalert_csv(rows, output)
            with output.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                row = next(reader)
                fieldnames = reader.fieldnames

        replace_mock.assert_called_once()
        self.assertEqual(fieldnames, STOCKALERT_COLUMNS)
        self.assertEqual(row["symbol"], "000001")
        self.assertEqual(row["exchange"], "SHE")

    def test_report_contains_pin_checksums_attribution_and_validation_details(self):
        history = self._active_history()
        latest = self._latest_from_history(history)
        _output, report = convert_history_rows(history, latest_rows=latest, control_dates=["2026-06-12"])
        payload = report.to_dict()

        self.assertEqual(payload["source_repository"], SOURCE_REPOSITORY)
        self.assertEqual(payload["source_tag"], SOURCE_TAG)
        self.assertEqual(payload["source_commit"], SOURCE_COMMIT)
        self.assertEqual(payload["source_license"], SOURCE_LICENSE)
        self.assertTrue(payload["attribution_required"])
        self.assertIn("Commercial redistribution", payload["attribution"])
        self.assertEqual(payload["pinned_urls"], PINNED_SOURCE_URLS)
        self.assertEqual(payload["checksums_expected"], EXPECTED_SOURCE_SHA256)
        self.assertIn("2026-06-12", payload["control_date_checks"])
        self.assertTrue(payload["latest_history_validation"]["matches"])

    def test_invalid_source_version_is_blocking_and_does_not_publish(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            output = root / "out.csv"
            report_path = root / "report.json"
            exit_code = main([
                "--input-dir", str(root),
                "--source-version", "main",
                "--output", str(output),
                "--report", str(report_path),
            ])
            payload = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 1)
        self.assertFalse(output.exists())
        self.assertEqual(payload["status"], "failed")
        self.assertIn("source_validation_error", {error["code"] for error in payload["errors"]})

    def test_yfiua_comparison_accepts_yahoo_format(self):
        stockalert_rows, _report = convert_history_rows([
            {"symbol": "SH600519", "name": "茅台", "opt-in": "2020-01-01", "opt-out": ""},
            {"symbol": "SZ000001", "name": "平安银行", "opt-in": "2020-01-01", "opt-out": ""},
        ])
        comparison = compare_yfiua_latest(stockalert_rows, [
            {"Symbol": "600519.SS", "Name": "Moutai"},
            {"Symbol": "000001.SZ", "Name": "Ping An Bank"},
            {"Symbol": "300750.SZ", "Name": "CATL"},
        ])

        self.assertEqual(comparison["overlap_count"], 2)
        self.assertEqual(comparison["only_in_yfiua"], ["300750.SHE"])
