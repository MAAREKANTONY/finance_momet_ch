from __future__ import annotations

import inspect
import json
import tempfile
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase, override_settings
from django.urls import reverse

from core.models import (
    DailyBar,
    ProcessingJob,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.china_benchmark_registry import expected_primary_benchmarks
from core.services.csi300_csv_generation import (
    CSI300CSVGenerationError,
    csi300_generation_artifact,
    generate_csi300_historical_csv,
    latest_valid_csi300_generation,
)
from core.services.csi300_daily_refresh import CSI300DailyRefreshError, refresh_csi300_daily_data
from core.services.csi300_operations_status import build_csi300_operations_status
from core.services.provider_eodhd import EODHDError
from core.tasks import (
    _without_csi300_eodhd_scope,
    daily_system_refresh_job_task,
    generate_csi300_historical_csv_job_task,
    refresh_csi300_daily_data_job_task,
)
from tools import convert_unliftedq_csi300_to_stockalert_csv as converter
from tools.csi300_policy import CSI300_SUPPORTED_HISTORY_START_ISO


def _ohlc_row(value_date: date) -> dict:
    return {
        "date": value_date,
        "open": Decimal("10"),
        "high": Decimal("11"),
        "low": Decimal("9"),
        "close": Decimal("10.5"),
        "volume": 1000,
    }


def _valid_converter_runner(arguments_seen: list[list[str]], *, warning: bool = True):
    def run(arguments: list[str]) -> int:
        arguments_seen.append(list(arguments))
        output = Path(arguments[arguments.index("--output") + 1])
        report_path = Path(arguments[arguments.index("--report") + 1])
        output.write_text("universe_code,symbol\nCSI300,600000\n", encoding="utf-8")
        report = {
            "status": "valid",
            "source_repository": converter.SOURCE_REPOSITORY,
            "source_tag": converter.SOURCE_TAG,
            "source_commit": converter.SOURCE_COMMIT,
            "supported_history_start": CSI300_SUPPORTED_HISTORY_START_ISO,
            "checksums_expected": converter.EXPECTED_SOURCE_SHA256,
            "checksums_received": converter.EXPECTED_SOURCE_SHA256,
            "errors": [],
            "warnings": ([{"code": "suspicious_company_name", "message": "601006 / 000780"}] if warning else []),
            "duplicate_exact_rows": [],
            "overlapping_periods": [],
            "memberships_produced": 390,
            "memberships_written": 390,
            "distinct_tickers": 385,
            "min_start_date": "2023-01-03",
            "max_end_date": "2026-06-30",
            "outside_supported_history_count": 835,
            "clipped_to_supported_start_count": 300,
            "active_counts": {
                "2023-01-03": 300,
                "2026-06-11": 300,
                "2026-06-12": 300,
                "2026-06-13": 300,
            },
            "control_date_checks": {
                "2026-06-12": {"active_count": 300, "entrants": [f"IN{i}" for i in range(19)], "sortants": [f"OUT{i}" for i in range(19)]},
            },
        }
        report_path.write_text(json.dumps(report), encoding="utf-8")
        return 0

    return run


class CSI300GenerationServiceTests(TestCase):
    def test_calls_only_pinned_converter_and_publishes_valid_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            seen: list[list[str]] = []
            with override_settings(CSI300_GENERATION_ROOT=temporary):
                result = generate_csi300_historical_csv(
                    job_id=41,
                    converter_runner=_valid_converter_runner(seen),
                )
                latest = latest_valid_csi300_generation()

        self.assertEqual(len(seen), 1)
        command = seen[0]
        self.assertIn("--download", command)
        self.assertEqual(command[command.index("--source-version") + 1], converter.SOURCE_COMMIT)
        self.assertIn(CSI300_SUPPORTED_HISTORY_START_ISO, command)
        self.assertEqual(result.memberships, 390)
        self.assertEqual(result.distinct_tickers, 385)
        self.assertEqual(result.status, "DONE_WITH_WARNING")
        self.assertEqual(latest["csv_sha256"], result.csv_sha256)
        self.assertEqual(latest["source_tag"], "v0.6.2")

    def test_invalid_checksum_never_replaces_last_valid_generation(self):
        with tempfile.TemporaryDirectory() as temporary, override_settings(CSI300_GENERATION_ROOT=temporary):
            first = generate_csi300_historical_csv(
                job_id=1,
                converter_runner=_valid_converter_runner([]),
            )

            def invalid(arguments):
                code = _valid_converter_runner([])(arguments)
                report_path = Path(arguments[arguments.index("--report") + 1])
                payload = json.loads(report_path.read_text(encoding="utf-8"))
                payload["checksums_received"] = {"history": "bad"}
                report_path.write_text(json.dumps(payload), encoding="utf-8")
                return code

            with self.assertRaises(CSI300CSVGenerationError):
                generate_csi300_historical_csv(job_id=2, converter_runner=invalid)
            latest = latest_valid_csi300_generation()

        self.assertEqual(latest["csv_sha256"], first.csv_sha256)

    def test_network_error_fails_without_publishing_csv(self):
        with tempfile.TemporaryDirectory() as temporary, override_settings(CSI300_GENERATION_ROOT=temporary):
            with self.assertRaisesRegex(CSI300CSVGenerationError, "network unavailable"):
                generate_csi300_historical_csv(
                    job_id=3,
                    converter_runner=Mock(side_effect=OSError("network unavailable")),
                )
            self.assertIsNone(latest_valid_csi300_generation())
            self.assertIsNone(csi300_generation_artifact("csv"))

    @patch("core.tasks._acquire_csi300_operation_lock", return_value=(True, None))
    @patch("core.tasks.generate_csi300_historical_csv")
    def test_job_exposes_french_non_import_message_and_structured_payload(self, generate_mock, _lock_mock):
        generate_mock.return_value = SimpleNamespace(
            status="DONE",
            csv_path="/data/exports/csi300/history/out.csv",
            as_dict=lambda: {"memberships": 390, "warnings": []},
        )
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.GENERATE_CSI300_CSV,
            status=ProcessingJob.Status.PENDING,
        )

        generate_csi300_historical_csv_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("Aucune donnée n’a été importée", job.message)
        self.assertIn('"memberships": 390', job.message)
        self.assertEqual(job.output_name, "csi300_stockalert_memberships.csv")

    def test_generation_code_has_no_membership_import_path(self):
        source = inspect.getsource(generate_csi300_historical_csv_job_task)
        self.assertNotIn("import_universe_memberships", source)
        self.assertNotIn("UniverseMembership", source)

    @patch("core.tasks._acquire_csi300_operation_lock", return_value=(True, None))
    @patch("core.tasks.generate_csi300_historical_csv", side_effect=CSI300CSVGenerationError("checksum mismatch"))
    def test_generation_failure_is_tracked_and_keeps_no_artifact(self, _generate_mock, _lock_mock):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.GENERATE_CSI300_CSV,
            status=ProcessingJob.Status.PENDING,
        )

        with self.assertRaises(CSI300CSVGenerationError):
            generate_csi300_historical_csv_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("dernier CSV valide reste inchangé", job.message)
        self.assertEqual(job.output_file, "")


class CSI300OperationsFixtureMixin:
    def create_universe(self, *, symbol_count: int = 2, snapshot_days: int = 1):
        universe = UniverseDefinition.objects.create(code="CSI300", name="CSI 300", active=True, source="fixture")
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="fixture",
            source_name="fixture",
            source_reference="fixture",
            period_start=date(2023, 1, 3),
            period_end=date(2023, 1, 3 + snapshot_days - 1),
            expected_member_count=symbol_count,
            imported_member_count=symbol_count,
            mapped_member_count=symbol_count,
            status=UniverseCoverageStatus.VALIDATED,
        )
        symbols = []
        for index in range(symbol_count):
            ticker = f"6000{index:02d}"
            symbol = Symbol.objects.create(
                ticker=ticker,
                exchange="SHG",
                name=f"Local {index}",
                name_en=f"English {index}",
                sector="Financials",
            )
            symbols.append(symbol)
            UniverseMembership.objects.create(
                universe=universe,
                symbol=symbol,
                ticker=ticker,
                exchange="SHG",
                provider_symbol=f"{ticker}.SHG",
                valid_from=date(2023, 1, 3),
                source="fixture",
            )
        for offset in range(snapshot_days):
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=date(2023, 1, 3 + offset),
                expected_member_count=symbol_count,
                actual_member_count=symbol_count,
                mapped_member_count=symbol_count,
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
        return universe, batch, symbols


class CSI300DailyRefreshTests(CSI300OperationsFixtureMixin, TestCase):
    def test_refresh_uses_current_actions_and_exact_supported_benchmarks_without_fallback(self):
        _universe, _batch, symbols = self.create_universe(symbol_count=2)
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row(date(2023, 1, 4))]
        memberships_before = list(UniverseMembership.objects.values_list("id", "provider_symbol"))
        snapshots_before = list(UniverseCoverageSnapshot.objects.values_list("id", "coverage_date", "import_batch_id"))

        report = refresh_csi300_daily_data(refresh_date=date(2023, 1, 4), client=client)

        requested = [call.args[0] for call in client.fetch_historical_ohlc.call_args_list]
        expected_benchmarks = [item.provider_symbol for item in expected_primary_benchmarks()]
        self.assertEqual(report.actions_expected, 2)
        self.assertEqual(report.benchmarks_expected, 10)
        self.assertEqual(report.benchmark_tickers, expected_benchmarks)
        self.assertEqual(set(requested), {"600000.SHG", "600001.SHG", *expected_benchmarks})
        self.assertNotIn("510300.SHG", requested)
        self.assertNotIn("SPY", requested)
        self.assertFalse(any(value.startswith("XL") for value in requested))
        self.assertEqual(list(UniverseMembership.objects.values_list("id", "provider_symbol")), memberships_before)
        self.assertEqual(
            list(UniverseCoverageSnapshot.objects.values_list("id", "coverage_date", "import_batch_id")),
            snapshots_before,
        )
        self.assertTrue(all(DailyBar.objects.filter(symbol=symbol).exists() for symbol in symbols))

    def test_partial_provider_error_is_warning_and_other_symbols_are_persisted(self):
        _universe, _batch, symbols = self.create_universe(symbol_count=2)
        client = Mock()

        def fetch(provider_symbol, _start, end):
            if provider_symbol == "600000.SHG":
                raise EODHDError("HTTP 404")
            return [_ohlc_row(end)]

        client.fetch_historical_ohlc.side_effect = fetch
        report = refresh_csi300_daily_data(refresh_date=date(2023, 1, 4), client=client)

        self.assertEqual(report.status, "READY_WITH_WARNINGS")
        self.assertEqual(report.actions_errors, 1)
        self.assertFalse(DailyBar.objects.filter(symbol=symbols[0]).exists())
        self.assertTrue(DailyBar.objects.filter(symbol=symbols[1]).exists())

    def test_refresh_is_idempotent(self):
        self.create_universe(symbol_count=1)
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row(date(2023, 1, 4))]

        refresh_csi300_daily_data(refresh_date=date(2023, 1, 4), client=client)
        first_count = DailyBar.objects.count()
        second = refresh_csi300_daily_data(refresh_date=date(2023, 1, 4), client=client)

        self.assertEqual(DailyBar.objects.count(), first_count)
        self.assertEqual(second.inserted_bars, 0)

    def test_systemic_provider_failure_is_failed_not_a_partial_warning(self):
        self.create_universe(symbol_count=1)
        client = Mock()
        client.fetch_historical_ohlc.side_effect = EODHDError("provider unavailable")

        with self.assertRaisesRegex(CSI300DailyRefreshError, "aucune action"):
            refresh_csi300_daily_data(refresh_date=date(2023, 1, 4), client=client)

        self.assertEqual(DailyBar.objects.count(), 0)

    @patch("core.tasks._acquire_csi300_operation_lock", return_value=(True, None))
    @patch("core.tasks.refresh_csi300_daily_data")
    def test_manual_task_uses_shared_service_and_exposes_done_with_warning(self, service_mock, _lock_mock):
        service_mock.return_value = SimpleNamespace(
            status="READY_WITH_WARNINGS",
            actions_expected=300,
            benchmarks_expected=10,
            inserted_bars=2,
            updated_bars=1,
            warnings=["partiel"],
            as_dict=lambda: {"status": "READY_WITH_WARNINGS", "warnings": ["partiel"]},
        )
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.REFRESH_CSI300_DATA,
            status=ProcessingJob.Status.PENDING,
        )

        refresh_csi300_daily_data_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn('"operational_status": "DONE_WITH_WARNING"', job.message)
        service_mock.assert_called_once()

    def test_periodic_and_manual_paths_share_refresh_executor(self):
        manual_source = inspect.getsource(refresh_csi300_daily_data_job_task)
        periodic_source = inspect.getsource(daily_system_refresh_job_task)
        self.assertIn("_execute_csi300_daily_refresh_job", manual_source)
        self.assertIn("_execute_csi300_daily_refresh_job", periodic_source)

    def test_legacy_daily_provider_scope_excludes_csi300_actions_and_benchmarks(self):
        universe, _batch, symbols = self.create_universe(symbol_count=1)
        market = Symbol.objects.create(ticker="000300", exchange="SHG", instrument_type="INDEX")
        other = Symbol.objects.create(ticker="AAPL", exchange="US")

        scoped = _without_csi300_eodhd_scope(Symbol.objects.filter(active=True), universe)

        self.assertNotIn(symbols[0].id, scoped.values_list("id", flat=True))
        self.assertNotIn(market.id, scoped.values_list("id", flat=True))
        self.assertIn(other.id, scoped.values_list("id", flat=True))


class CSI300OperationsDashboardTests(CSI300OperationsFixtureMixin, TestCase):
    def setUp(self):
        self.staff = get_user_model().objects.create_user(
            username="csi-ops-staff",
            password="secret123",
            is_staff=True,
        )
        self.user = get_user_model().objects.create_user(username="csi-ops-user", password="secret123")

    @patch("core.tasks.generate_csi300_historical_csv_job_task.apply_async", return_value=SimpleNamespace(id="gen-1"))
    @patch("core.job_launch.broker_queue_snapshot", return_value=SimpleNamespace(queue_name="celery", length=0, samples=[]))
    @patch("core.job_launch.transaction.on_commit", side_effect=lambda callback: callback())
    def test_staff_can_launch_generation_job(self, _commit, _broker, task_apply):
        self.client.force_login(self.staff)
        response = self.client.post(reverse("symbols_csi300_generate_csv"))
        job = ProcessingJob.objects.get()

        self.assertEqual(response.url, reverse("job_detail", args=[job.id]))
        self.assertEqual(job.job_type, ProcessingJob.JobType.GENERATE_CSI300_CSV)
        task_apply.assert_called_once()

    def test_non_staff_cannot_launch_or_download(self):
        self.client.force_login(self.user)
        launch = self.client.post(reverse("symbols_csi300_generate_csv"))
        download = self.client.get(reverse("symbols_csi300_generation_download", args=["csv"]))

        self.assertEqual(launch.status_code, 302)
        self.assertIn("/admin/login/", launch.url)
        self.assertEqual(download.status_code, 302)
        self.assertEqual(ProcessingJob.objects.count(), 0)

    def test_active_equivalent_job_prevents_duplicate_launch(self):
        self.client.force_login(self.staff)
        active = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.GENERATE_CSI300_CSV,
            status=ProcessingJob.Status.RUNNING,
        )

        response = self.client.post(reverse("symbols_csi300_generate_csv"))

        self.assertEqual(response.url, reverse("job_detail", args=[active.id]))
        self.assertEqual(ProcessingJob.objects.count(), 1)

    @patch("core.tasks.refresh_csi300_daily_data_job_task.apply_async", return_value=SimpleNamespace(id="refresh-1"))
    @patch("core.job_launch.broker_queue_snapshot", return_value=SimpleNamespace(queue_name="celery", length=0, samples=[]))
    @patch("core.job_launch.transaction.on_commit", side_effect=lambda callback: callback())
    def test_staff_manual_refresh_uses_tracked_job(self, _commit, _broker, task_apply):
        self.client.force_login(self.staff)

        response = self.client.post(reverse("symbols_csi300_refresh_china"))
        job = ProcessingJob.objects.get()

        self.assertEqual(response.url, reverse("job_detail", args=[job.id]))
        self.assertEqual(job.job_type, ProcessingJob.JobType.REFRESH_CSI300_DATA)
        task_apply.assert_called_once_with(kwargs={"user_id": self.staff.id, "job_id": job.id})

    def test_download_accepts_only_known_latest_artifacts(self):
        self.client.force_login(self.staff)
        with tempfile.TemporaryDirectory() as temporary, override_settings(CSI300_GENERATION_ROOT=temporary):
            generate_csi300_historical_csv(job_id=1, converter_runner=_valid_converter_runner([]))
            response = self.client.get(reverse("symbols_csi300_generation_download", args=["csv"]))
            invalid = self.client.get(reverse("symbols_csi300_generation_download", args=["secret"]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(invalid.status_code, 404)

    def test_observability_reports_ready_with_warnings_for_partial_ohlc(self):
        _universe, _batch, symbols = self.create_universe(symbol_count=2, snapshot_days=2)
        DailyBar.objects.create(
            symbol=symbols[0],
            date=date(2023, 1, 4),
            open=1,
            high=1,
            low=1,
            close=1,
            volume=1,
        )

        status = build_csi300_operations_status()

        self.assertEqual(status["status"], "READY_WITH_WARNINGS")
        self.assertEqual(status["referential"]["status"], "READY")
        self.assertEqual(status["referential"]["memberships"], 2)
        self.assertEqual(status["metadata"]["name_en_present"], 2)
        self.assertEqual(status["ohlc"]["symbols_with_bars"], 1)
        self.assertEqual(status["ohlc"]["symbols_without_bars"], 1)

    def test_observability_reports_not_ready_for_snapshot_mismatch(self):
        universe, _batch, _symbols = self.create_universe(symbol_count=1)
        UniverseCoverageSnapshot.objects.filter(universe=universe).update(actual_member_count=0)

        status = build_csi300_operations_status()

        self.assertEqual(status["status"], "NOT_READY")
        self.assertEqual(status["referential"]["counter_mismatches"], 1)

    def test_staff_page_displays_operations_and_separate_import_link(self):
        self.create_universe(symbol_count=1)
        self.client.force_login(self.staff)

        response = self.client.get(reverse("symbols_page"))
        body = response.content.decode()

        self.assertContains(response, "Générer le CSV historique CSI300")
        self.assertContains(response, "Rafraîchir les données Chine")
        self.assertIn(reverse("symbols_import"), body)
        self.assertIn("Génération et import sont strictement séparés", body)

    def test_processing_job_types_are_explicit(self):
        self.assertEqual(ProcessingJob.JobType.GENERATE_CSI300_CSV.label, "Génération du CSV CSI300")
        self.assertEqual(ProcessingJob.JobType.REFRESH_CSI300_DATA.label, "Rafraîchissement des données CSI300")
