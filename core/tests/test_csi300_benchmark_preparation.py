from __future__ import annotations

import inspect
import json
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.job_tracking import JobCancelled
from core.models import DailyBar, ProcessingJob, Symbol
from core.services.china_benchmark_registry import (
    CSI300_MARKET_BENCHMARK,
    CSI300_MARKET_FALLBACK,
    CSI300_SECTOR_BENCHMARKS,
    RAW_SECTOR_TO_CANONICAL,
    STATUS_UNSUPPORTED,
    canonical_sector_for_raw,
    expected_primary_benchmarks,
    supported_sector_benchmarks,
    unsupported_sector_benchmarks,
)
from core.services.csi300_benchmark_preparation import (
    DEFAULT_START_DATE,
    CSI300BenchmarkPreparationReport,
    prepare_csi300_benchmarks,
    registry_summary,
)
from core.services.dynamic_universe_readiness import CHECK_ERROR, check_dynamic_universe_readiness
from core.services.trend_filters import market_benchmark_ticker_for_symbol, sector_benchmark_ticker_for_symbol
from core.tasks import prepare_csi300_benchmarks_job_task


def _ohlc_row(row_date=date(2021, 8, 20), close="10.5"):
    return {
        "date": row_date,
        "open": Decimal("10"),
        "high": Decimal("11"),
        "low": Decimal("9"),
        "close": Decimal(close),
        "volume": 1000,
    }


def _report_json_from_message(message: str) -> dict:
    for line in (message or "").splitlines():
        if line.startswith("report_json="):
            return json.loads(line.split("=", 1)[1])
    raise AssertionError("missing report_json line")


class CSI300BenchmarkRegistryTests(TestCase):
    def test_raw_sector_mapping_is_explicit(self):
        self.assertEqual(RAW_SECTOR_TO_CANONICAL["Basic Materials"], "Materials")
        self.assertEqual(RAW_SECTOR_TO_CANONICAL["Consumer Goods"], "Consumer Discretionary")
        self.assertEqual(canonical_sector_for_raw("Technology"), "Information Technology")

    def test_materials_and_consumer_goods_are_normalized(self):
        self.assertEqual(canonical_sector_for_raw("Materials"), "Materials")
        self.assertEqual(canonical_sector_for_raw("Consumer Goods"), "Consumer Discretionary")

    def test_nine_sector_benchmarks_are_supported(self):
        supported = supported_sector_benchmarks()
        self.assertEqual(len(supported), 9)
        self.assertEqual({item.canonical_sector for item in supported}, {
            "Materials",
            "Consumer Discretionary",
            "Consumer Staples",
            "Health Care",
            "Financials",
            "Information Technology",
            "Communication Services",
            "Energy",
            "Real Estate",
        })

    def test_industrials_and_utilities_are_unsupported(self):
        unsupported = {item.canonical_sector: item for item in unsupported_sector_benchmarks()}
        self.assertEqual(set(unsupported), {"Industrials", "Utilities"})
        self.assertEqual(unsupported["Industrials"].status, STATUS_UNSUPPORTED)
        self.assertIsNone(unsupported["Utilities"].provider_symbol)

    def test_market_primary_and_fallback_are_distinct_and_not_automatic(self):
        self.assertEqual(CSI300_MARKET_BENCHMARK.provider_symbol, "000300.SHG")
        self.assertEqual(CSI300_MARKET_FALLBACK.provider_symbol, "510300.SHG")
        self.assertFalse(CSI300_MARKET_BENCHMARK.is_fallback)
        self.assertTrue(CSI300_MARKET_FALLBACK.is_fallback)
        self.assertNotIn(CSI300_MARKET_FALLBACK, expected_primary_benchmarks())

    def test_registry_summary_exposes_market_and_sector_entries(self):
        summary = registry_summary()
        self.assertEqual(summary["market"]["provider_symbol"], "000300.SHG")
        self.assertEqual(summary["sectors"]["Utilities"]["status"], STATUS_UNSUPPORTED)


class CSI300BenchmarkPreparationServiceTests(TestCase):
    def test_dry_run_creates_nothing_and_does_not_contact_provider(self):
        with patch("core.services.csi300_benchmark_preparation.EODHDClient") as client_cls:
            report = prepare_csi300_benchmarks(dry_run=True)

        self.assertEqual(Symbol.objects.count(), 0)
        self.assertEqual(DailyBar.objects.count(), 0)
        client_cls.assert_not_called()
        self.assertEqual(report.expected, 12)
        self.assertEqual(report.supported, 10)

    def test_apply_creates_benchmark_symbols_idempotently(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = []

        first = prepare_csi300_benchmarks(dry_run=False, client=client)
        second = prepare_csi300_benchmarks(dry_run=False, client=client)

        self.assertEqual(Symbol.objects.filter(ticker="000300", exchange="SHG").count(), 1)
        self.assertEqual(Symbol.objects.filter(ticker="159944", exchange="SHE").count(), 1)
        self.assertEqual(first.created_symbols, 10)
        self.assertEqual(second.created_symbols, 0)
        self.assertEqual(second.existing_symbols, 10)

    def test_market_symbol_uses_index_type_and_etfs_use_etf_type(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = []

        prepare_csi300_benchmarks(dry_run=False, client=client)

        self.assertEqual(Symbol.objects.get(ticker="000300", exchange="SHG").instrument_type, "INDEX")
        self.assertEqual(Symbol.objects.get(ticker="159944", exchange="SHE").instrument_type, "ETF")

    def test_ohlc_rows_are_upserted_from_eodhd(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row()]

        report = prepare_csi300_benchmarks(dry_run=False, client=client)
        market = Symbol.objects.get(ticker="000300", exchange="SHG")

        self.assertEqual(DailyBar.objects.filter(symbol=market).count(), 1)
        self.assertEqual(DailyBar.objects.filter(source="eodhd").count(), 10)
        self.assertEqual(report.inserted_bars, 10)
        self.assertEqual(report.first_ohlc["000300.SHG"], "2021-08-20")

    def test_existing_bar_is_updated_idempotently(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row(close="12.5")]
        market = Symbol.objects.create(
            ticker="000300",
            exchange="SHG",
            name="CSI 300 Index",
            instrument_type="INDEX",
            country="China",
            currency="CNY",
        )
        DailyBar.objects.create(symbol=market, date=date(2021, 8, 20), open=1, high=1, low=1, close=1, volume=1)

        report = prepare_csi300_benchmarks(dry_run=False, client=client)

        market_bar = DailyBar.objects.get(symbol=market, date=date(2021, 8, 20))
        self.assertEqual(market_bar.close, Decimal("12.500000"))
        self.assertGreaterEqual(report.updated_bars, 1)

    def test_existing_etf_with_incompatible_type_is_conflict_and_skipped(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row()]
        bad_symbol = Symbol.objects.create(
            ticker="159944",
            exchange="SHE",
            name="Wrong Materials",
            instrument_type="STOCK",
            country="China",
            currency="CNY",
        )

        report = prepare_csi300_benchmarks(dry_run=False, client=client)

        self.assertEqual(report.conflicts, 1)
        self.assertEqual(report.skipped_conflicts, 1)
        self.assertFalse(DailyBar.objects.filter(symbol=bad_symbol).exists())
        called_symbols = [call.args[0] for call in client.fetch_historical_ohlc.call_args_list]
        self.assertNotIn("159944.SHE", called_symbols)
        self.assertIn("000300.SHG", called_symbols)
        conflict = next(item for item in report.per_benchmark if item.get("provider_symbol") == "159944.SHE")
        self.assertEqual(conflict["status"], "conflict")
        self.assertEqual(conflict["expected_instrument_type"], "ETF")
        self.assertEqual(conflict["existing_instrument_type"], "STOCK")

    def test_existing_market_with_etf_type_is_conflict_and_skipped(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row()]
        market = Symbol.objects.create(
            ticker="000300",
            exchange="SHG",
            name="Wrong CSI 300",
            instrument_type="ETF",
            country="China",
            currency="CNY",
        )

        report = prepare_csi300_benchmarks(dry_run=False, client=client)

        self.assertEqual(report.conflicts, 1)
        self.assertEqual(report.skipped_conflicts, 1)
        self.assertFalse(DailyBar.objects.filter(symbol=market).exists())
        called_symbols = [call.args[0] for call in client.fetch_historical_ohlc.call_args_list]
        self.assertNotIn("000300.SHG", called_symbols)
        self.assertIn("159944.SHE", called_symbols)

    def test_empty_instrument_type_dry_run_reports_upgrade_without_write(self):
        symbol = Symbol.objects.create(
            ticker="159944",
            exchange="SHE",
            name="Existing Materials",
            instrument_type="",
            country="China",
            currency="CNY",
        )

        report = prepare_csi300_benchmarks(dry_run=True)

        symbol.refresh_from_db()
        self.assertEqual(symbol.instrument_type, "")
        self.assertEqual(report.updated_symbol_metadata, 1)
        detail = next(item for item in report.per_benchmark if item.get("provider_symbol") == "159944.SHE")
        self.assertEqual(detail["status"], "dry_run_update_type")
        self.assertEqual(detail["expected_instrument_type"], "ETF")

    def test_empty_instrument_type_apply_updates_only_type_and_fetches(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = [_ohlc_row()]
        symbol = Symbol.objects.create(
            ticker="159944",
            exchange="SHE",
            name="Existing Materials",
            instrument_type="",
            country="CN",
            currency="RMB",
            sector="Keep Me",
            active=False,
        )

        report = prepare_csi300_benchmarks(dry_run=False, client=client)

        symbol.refresh_from_db()
        self.assertEqual(symbol.instrument_type, "ETF")
        self.assertEqual(symbol.name, "Existing Materials")
        self.assertEqual(symbol.country, "CN")
        self.assertEqual(symbol.currency, "RMB")
        self.assertEqual(symbol.sector, "Keep Me")
        self.assertFalse(symbol.active)
        self.assertEqual(report.updated_symbol_metadata, 1)
        called_symbols = [call.args[0] for call in client.fetch_historical_ohlc.call_args_list]
        self.assertIn("159944.SHE", called_symbols)
        self.assertTrue(DailyBar.objects.filter(symbol=symbol).exists())

    def test_isolated_provider_error_is_reported_without_stopping(self):
        client = Mock()
        client.fetch_historical_ohlc.side_effect = [Exception("unexpected")]

        with self.assertRaises(Exception):
            prepare_csi300_benchmarks(dry_run=False, client=client)

    def test_eodhd_error_is_non_blocking(self):
        from core.services.provider_eodhd import EODHDError

        client = Mock()
        client.fetch_historical_ohlc.side_effect = [EODHDError("HTTP 404?api_token=secret"), *_ohlc_successes(9)]

        report = prepare_csi300_benchmarks(dry_run=False, client=client)

        self.assertEqual(report.errors, 1)
        self.assertEqual(report.provider_successes, 9)
        self.assertNotIn("secret", str(report.per_benchmark))

    def test_unsupported_sectors_are_never_called_at_provider(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = []

        prepare_csi300_benchmarks(dry_run=False, client=client)

        called_symbols = [call.args[0] for call in client.fetch_historical_ohlc.call_args_list]
        self.assertNotIn(None, called_symbols)
        self.assertNotIn(CSI300_SECTOR_BENCHMARKS["Industrials"].provider_symbol, called_symbols)
        self.assertNotIn(CSI300_SECTOR_BENCHMARKS["Utilities"].provider_symbol, called_symbols)
        self.assertEqual(len(called_symbols), 10)

    def test_progress_callback_is_called(self):
        client = Mock()
        client.fetch_historical_ohlc.return_value = []
        callback = Mock()

        prepare_csi300_benchmarks(dry_run=False, client=client, progress_callback=callback)

        self.assertEqual(callback.call_count, 10)

    def test_report_contains_required_fields(self):
        report = prepare_csi300_benchmarks(dry_run=True)
        payload = report.as_dict()

        for key in (
            "expected",
            "supported",
            "unsupported_sectors",
            "existing_symbols",
            "created_symbols",
            "updated_symbol_metadata",
            "conflicts",
            "skipped_conflicts",
            "provider_successes",
            "errors",
            "inserted_bars",
            "updated_bars",
            "first_ohlc",
            "last_ohlc",
            "dry_run",
        ):
            self.assertIn(key, payload)


def _ohlc_successes(count: int):
    return [[_ohlc_row()] for _idx in range(count)]


class CSI300BenchmarkUIViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="csi-bench", password="secret123")
        self.client.force_login(self.user)

    def _post_with_real_launch(self, data: dict, *, task_id: str = "task-csi-bench-1"):
        task_apply = Mock(return_value=SimpleNamespace(id=task_id))
        broker_snapshot = SimpleNamespace(queue_name="celery", length=0, samples=[])
        with (
            patch("core.tasks.prepare_csi300_benchmarks_job_task.apply_async", task_apply),
            patch("core.job_launch.broker_queue_snapshot", return_value=broker_snapshot),
            patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: fn()),
        ):
            response = self.client.post(reverse("symbols_csi300_benchmarks"), data)
        return response, task_apply

    def test_symbols_page_displays_csi300_benchmark_section(self):
        response = self.client.get(reverse("symbols_page"))

        body = response.content.decode()
        self.assertIn("Benchmarks CSI300 — marché et secteurs", body)
        self.assertIn("Analyser les benchmarks CSI300", body)
        self.assertIn("Préparer les benchmarks CSI300 et leurs OHLC", body)
        self.assertIn("Industrials", body)
        self.assertIn("Utilities", body)

    def test_benchmark_action_rejects_get(self):
        response = self.client.get(reverse("symbols_csi300_benchmarks"))

        self.assertEqual(response.status_code, 405)

    def test_dry_run_creates_fetch_bars_job_and_schedules_apply_false(self):
        response, task_apply = self._post_with_real_launch({"mode": "dry_run"})

        job = ProcessingJob.objects.get()
        self.assertEqual(response.url, reverse("job_detail", args=[job.id]))
        self.assertEqual(job.job_type, ProcessingJob.JobType.FETCH_BARS)
        task_apply.assert_called_once_with(kwargs={"apply": False, "user_id": self.user.id, "job_id": job.id})

    def test_apply_without_confirmation_creates_no_job(self):
        response, task_apply = self._post_with_real_launch({"mode": "apply"})

        self.assertEqual(response.url, reverse("symbols_page"))
        self.assertEqual(ProcessingJob.objects.count(), 0)
        task_apply.assert_not_called()

    def test_apply_confirmed_creates_job_and_schedules_apply_true(self):
        response, task_apply = self._post_with_real_launch({"mode": "apply", "confirm_apply": "1"})

        job = ProcessingJob.objects.get()
        self.assertEqual(response.url, reverse("job_detail", args=[job.id]))
        task_apply.assert_called_once_with(kwargs={"apply": True, "user_id": self.user.id, "job_id": job.id})

    @patch("core.services.csi300_benchmark_preparation.prepare_csi300_benchmarks")
    @patch("core.views.launch_processing_job")
    def test_view_does_not_call_provider_service(self, launch_mock, service_mock):
        launch_mock.return_value = SimpleNamespace(job=SimpleNamespace(id=55), dispatch_error=None)

        response = self.client.post(reverse("symbols_csi300_benchmarks"), {"mode": "dry_run"})

        self.assertEqual(response.status_code, 302)
        service_mock.assert_not_called()

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    def test_existing_us_benchmark_action_is_unchanged(self, sync_mock):
        sync_mock.return_value = {
            "source_symbols": 0,
            "benchmark_tickers": [],
            "created": 0,
            "existing": 0,
            "dry_run": False,
            "skip_enrichment": False,
            "skip_ohlc": True,
        }

        response = self.client.post(reverse("symbols_ensure_benchmark_etfs"))

        self.assertEqual(response.status_code, 302)
        sync_mock.assert_called_once()
        self.assertTrue(sync_mock.call_args.kwargs["skip_ohlc"])


class CSI300BenchmarkJobTaskTests(TestCase):
    def _job(self) -> ProcessingJob:
        return ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.PENDING,
            message="queued",
        )

    @patch("core.tasks.prepare_csi300_benchmarks")
    def test_task_calls_service_with_apply_dates(self, service_mock):
        service_mock.return_value = CSI300BenchmarkPreparationReport(
            dry_run=False,
            start_date=DEFAULT_START_DATE,
            end_date=date(2021, 8, 31),
        )
        job = self._job()

        prepare_csi300_benchmarks_job_task.apply(
            kwargs={
                "job_id": job.id,
                "apply": True,
                "start_date": "2021-08-20",
                "end_date": "2021-08-31",
            }
        ).get()

        self.assertEqual(service_mock.call_args.kwargs["dry_run"], False)
        self.assertEqual(service_mock.call_args.kwargs["start_date"], "2021-08-20")
        self.assertEqual(service_mock.call_args.kwargs["end_date"], "2021-08-31")

    @patch("core.tasks.prepare_csi300_benchmarks")
    def test_task_saves_structured_report(self, service_mock):
        service_mock.return_value = CSI300BenchmarkPreparationReport(
            dry_run=True,
            start_date=DEFAULT_START_DATE,
            end_date=date(2021, 8, 31),
            expected=12,
            supported=10,
            unsupported_sectors=["Industrials", "Utilities"],
        )
        job = self._job()

        prepare_csi300_benchmarks_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        payload = _report_json_from_message(job.message)
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertEqual(payload["expected"], 12)
        self.assertEqual(payload["unsupported_sectors"], ["Industrials", "Utilities"])

    @patch("core.tasks.prepare_csi300_benchmarks")
    def test_task_report_exposes_conflicts_and_skipped_conflicts(self, service_mock):
        service_mock.return_value = CSI300BenchmarkPreparationReport(
            dry_run=True,
            start_date=DEFAULT_START_DATE,
            end_date=date(2021, 8, 31),
            expected=12,
            supported=10,
            conflicts=1,
            skipped_conflicts=1,
            per_benchmark=[
                {
                    "provider_symbol": "159944.SHE",
                    "canonical_sector": "Materials",
                    "status": "conflict",
                    "expected_instrument_type": "ETF",
                    "existing_instrument_type": "STOCK",
                    "error": "instrument_type differs",
                }
            ],
        )
        job = self._job()

        prepare_csi300_benchmarks_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        payload = _report_json_from_message(job.message)
        self.assertEqual(payload["conflicts"], 1)
        self.assertEqual(payload["skipped_conflicts"], 1)
        self.assertEqual(payload["error_details"][0]["provider_symbol"], "159944.SHE")

    @patch("core.tasks._job_update")
    @patch("core.tasks.prepare_csi300_benchmarks")
    def test_task_records_progress(self, service_mock, update_mock):
        def _run(**kwargs):
            report = CSI300BenchmarkPreparationReport(
                dry_run=True,
                start_date=DEFAULT_START_DATE,
                end_date=date(2021, 8, 31),
            )
            kwargs["progress_callback"](report, 1, 10)
            return report

        service_mock.side_effect = _run
        job = self._job()

        prepare_csi300_benchmarks_job_task.apply(kwargs={"job_id": job.id}).get()

        self.assertTrue(update_mock.called)

    @patch("core.tasks.prepare_csi300_benchmarks", side_effect=JobCancelled)
    def test_task_handles_cancellation(self, service_mock):
        job = self._job()

        prepare_csi300_benchmarks_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)


class CSI300BenchmarkIsolationTests(TestCase):
    def test_trend_filters_do_not_auto_enable_csi300_benchmarks(self):
        symbol = Symbol.objects.create(ticker="600000", exchange="SHG", country="China", sector="Financial Services")

        self.assertIsNone(market_benchmark_ticker_for_symbol(symbol))
        self.assertNotIn(sector_benchmark_ticker_for_symbol(symbol), {"000300.SHG", "159931.SHE"})

    def test_csi300_readiness_gm_block_remains(self):
        report = check_dynamic_universe_readiness(
            universe="CSI300",
            start=date(2021, 8, 20),
            end=date(2021, 8, 31),
            require_gm_market=True,
        )

        check = next(item for item in report.checks if item.code == "gm_market_daily_bars")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("GM market non supporté pour CSI300 V1", check.message)

    def test_backtest_engine_does_not_call_csi300_benchmark_preparation(self):
        from core.services.backtesting import engine

        source = inspect.getsource(engine)
        self.assertNotIn("prepare_csi300_benchmarks", source)
