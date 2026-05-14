from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import Client, TestCase, override_settings
from django.urls import reverse

from core.job_launch import JobLaunchOutcome
from core.models import Backtest, ProcessingJob, Scenario, Symbol
from core.tasks import sync_market_caps_job_task


class SyncMarketCapsJobTaskTests(TestCase):
    def setUp(self):
        self.symbol_a = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.symbol_b = Symbol.objects.create(ticker="BBB", exchange="NASDAQ", active=True)
        self.scenario = Scenario.objects.create(name="Scenario Market Cap Jobs", active=True)
        self.scenario.symbols.set([self.symbol_a, self.symbol_b])
        self.backtest = Backtest.objects.create(
            name="BT Market Cap Jobs",
            scenario=self.scenario,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            universe_snapshot=[{"ticker": "AAA"}],
        )

    @patch("core.tasks.sync_market_caps_for_symbols")
    def test_task_calls_shared_service_with_backtest_scope_and_dates(self, mock_sync):
        mock_sync.return_value = {
            "fetched": 3,
            "inserted": 2,
            "updated": 1,
            "existing": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [],
        }
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.SYNC_MARKET_CAPS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        result = sync_market_caps_job_task.run(job_id=job.id, backtest_id=self.backtest.id)

        self.assertIn("scope=backtest=", result)
        args, kwargs = mock_sync.call_args
        self.assertEqual([symbol.ticker for symbol in args[0]], ["AAA"])
        self.assertEqual(args[1], date(2024, 1, 1))
        self.assertEqual(args[2], date(2024, 1, 31))
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("inserted=2", job.message)

    @patch("core.tasks.sync_market_caps_for_symbols")
    def test_task_stays_done_when_service_reports_per_symbol_errors(self, mock_sync):
        mock_sync.return_value = {
            "fetched": 1,
            "inserted": 0,
            "updated": 0,
            "existing": 0,
            "skipped": 0,
            "errors": 1,
            "per_symbol": [],
        }
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.SYNC_MARKET_CAPS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        sync_market_caps_job_task.run(job_id=job.id, backtest_id=self.backtest.id)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("errors=1", job.message)

    @patch("core.tasks.sync_market_caps_for_symbols", side_effect=RuntimeError("provider exploded"))
    def test_task_marks_failed_on_unexpected_service_exception(self, mock_sync):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.SYNC_MARKET_CAPS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        with self.assertRaises(RuntimeError):
            sync_market_caps_job_task.run(job_id=job.id, backtest_id=self.backtest.id)

        self.assertTrue(mock_sync.called)
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("provider exploded", job.error)

    @override_settings(EODHD_MARKET_CAP_SYNC_START_DATE="2020-01-01")
    @patch("core.tasks.sync_market_caps_for_symbols")
    def test_task_uses_global_default_date_range_without_scope(self, mock_sync):
        mock_sync.return_value = {
            "fetched": 0,
            "inserted": 0,
            "updated": 0,
            "existing": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [],
        }
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.SYNC_MARKET_CAPS,
            status=ProcessingJob.Status.PENDING,
        )

        sync_market_caps_job_task.run(job_id=job.id)

        args, kwargs = mock_sync.call_args
        self.assertEqual(args[1], date(2020, 1, 1))
        self.assertEqual(args[2], date.today())


class MarketCapSyncBacktestViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="mc-user", password="secret123")
        self.client = Client()
        self.client.force_login(self.user)
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(name="Scenario View Market Cap", active=True)
        self.scenario.symbols.set([self.symbol])
        self.backtest = Backtest.objects.create(
            name="BT View Market Cap",
            scenario=self.scenario,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            universe_snapshot=[{"ticker": "AAA"}],
        )

    def test_backtest_detail_renders_sync_market_caps_button_and_warning(self):
        response = self.client.get(reverse("backtest_detail", args=[self.backtest.id]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Sync Market Caps", body)
        self.assertIn("Uses EODHD quota; data is cached locally; no API calls occur during backtests.", body)

    @patch("core.views.launch_processing_job")
    def test_backtest_sync_market_caps_post_enqueues_processing_job(self, mock_launch):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.SYNC_MARKET_CAPS,
            status=ProcessingJob.Status.PENDING,
            backtest=self.backtest,
            scenario=self.scenario,
        )
        mock_launch.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

        response = self.client.post(reverse("backtest_sync_market_caps", args=[self.backtest.id]))

        self.assertEqual(response.status_code, 302)
        self.assertTrue(mock_launch.called)
        self.assertEqual(mock_launch.call_args.kwargs["job_type"], ProcessingJob.JobType.SYNC_MARKET_CAPS)
        self.assertEqual(mock_launch.call_args.kwargs["task_kwargs"]["from_date"], "2024-01-01")
        self.assertEqual(mock_launch.call_args.kwargs["task_kwargs"]["to_date"], "2024-01-31")
        messages = list(response.wsgi_request._messages)
        self.assertTrue(any("Sync Market Caps demandée" in str(m) for m in messages))

    def test_backtest_sync_market_caps_is_blocked_when_active_job_exists(self):
        ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.FETCH_BARS,
            status=ProcessingJob.Status.RUNNING,
            backtest=self.backtest,
            scenario=self.scenario,
        )

        response = self.client.post(reverse("backtest_sync_market_caps", args=[self.backtest.id]), follow=True)

        self.assertEqual(response.status_code, 200)
        messages = list(response.context["messages"])
        self.assertTrue(any("Impossible de lancer la sync market caps" in str(m) for m in messages))
        self.assertTrue(
            ProcessingJob.objects.filter(
                backtest=self.backtest,
                job_type=ProcessingJob.JobType.SYNC_MARKET_CAPS,
                status=ProcessingJob.Status.FAILED,
            ).exists()
        )


class MarketCapSyncTriggerPageTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="trigger-user", password="secret123")
        self.client = Client()
        self.client.force_login(self.user)

    def test_trigger_page_renders_market_cap_actions(self):
        response = self.client.get(reverse("trigger_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Sync Market Caps (global)", body)
        self.assertIn("Market Caps (EODHD)", body)
