from __future__ import annotations

from datetime import date, timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from core.models import Backtest, GameScenario, ProcessingJob, Scenario, Symbol


class JobControlViewTests(TestCase):
    def _make_backtest(self):
        scenario = Scenario.objects.create(name="Scenario Jobs")
        return Backtest.objects.create(name="BT Jobs", scenario=scenario)

    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="ops", password="secret123")
        self.client.force_login(self.user)

    def test_cancel_marks_pending_job_cancelled_immediately(self):
        bt = self._make_backtest()
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.RUN_BACKTEST, status=ProcessingJob.Status.PENDING, backtest=bt)
        response = self.client.post(reverse("job_cancel", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)
        self.assertIsNotNone(job.finished_at)
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.FAILED)
        self.assertIn("Cancelled", bt.error_message)

    def test_cancel_marks_running_job_for_cooperative_stop(self):
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.RUN_BACKTEST, status=ProcessingJob.Status.RUNNING)
        response = self.client.post(reverse("job_cancel", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.RUNNING)

    @patch("celery.app.control.Control.revoke")
    def test_kill_marks_pending_job_killed_immediately(self, revoke_mock):
        bt = self._make_backtest()
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.PENDING,
            task_id="abc123",
            backtest=bt,
        )
        response = self.client.post(reverse("job_kill", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.kill_requested)
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.KILLED)
        self.assertGreaterEqual(revoke_mock.call_count, 1)
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.FAILED)
        self.assertIn("Killed", bt.error_message)

    @patch("celery.app.control.Control.revoke")
    def test_kill_marks_running_job_requested_without_fake_completion(self, revoke_mock):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.RUNNING,
            task_id="def456",
        )
        response = self.client.post(reverse("job_kill", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.kill_requested)
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.RUNNING)
        self.assertGreaterEqual(revoke_mock.call_count, 1)


class CleanupProcessingJobsCommandTests(TestCase):
    def test_cleanup_command_updates_old_running_jobs(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.RUNNING,
        )
        ProcessingJob.objects.filter(id=job.id).update(created_at=timezone.now() - timedelta(minutes=180))
        out = StringIO()
        call_command(
            "cleanup_processing_jobs",
            "--older-than-minutes", "120",
            "--include-pending",
            "--status", "FAILED",
            stdout=out,
        )
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("Updated jobs: 1", out.getvalue())

from core.job_tracking import JobCancelled, JobCheckpointPulse, JobKilled, job_checkpoint, mark_job_running


class JobTrackingHelperTests(TestCase):
    def test_mark_job_running_stamps_worker_and_checkpoint(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
        )

        class Req:
            id = "task-123"
            hostname = "celery@worker-1"

        mark_job_running(job, task_request=Req(), message="boot")
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.RUNNING)
        self.assertEqual(job.task_id, "task-123")
        self.assertEqual(job.worker_hostname, "celery@worker-1")
        self.assertEqual(job.last_checkpoint, "boot")
        self.assertIsNotNone(job.heartbeat_at)
        self.assertIsNotNone(job.started_at)

    def test_job_checkpoint_updates_metadata(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.RUNNING,
        )

        class Req:
            hostname = "celery@worker-2"

        job_checkpoint(job, checkpoint="phase:metrics", task_request=Req())
        job.refresh_from_db()
        self.assertEqual(job.last_checkpoint, "phase:metrics")
        self.assertEqual(job.worker_hostname, "celery@worker-2")
        self.assertIsNotNone(job.heartbeat_at)

    def test_job_checkpoint_raises_when_cancel_or_kill_requested(self):
        job_cancel = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.RUNNING,
            cancel_requested=True,
        )
        with self.assertRaises(JobCancelled):
            job_checkpoint(job_cancel, checkpoint="loop")

        job_kill = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.RUNNING,
            kill_requested=True,
        )
        with self.assertRaises(JobKilled):
            job_checkpoint(job_kill, checkpoint="loop")


class JobCheckpointPulseTests(TestCase):
    def test_pulse_triggers_on_counter_threshold(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.EXPORT_DATA_XLSX,
            status=ProcessingJob.Status.RUNNING,
        )
        pulse = JobCheckpointPulse(job, every_n=3, every_seconds=3600, base_label="export")

        self.assertFalse(pulse.hit(checkpoint="row1"))
        self.assertFalse(pulse.hit(checkpoint="row2"))
        self.assertTrue(pulse.hit(checkpoint="row3"))

        job.refresh_from_db()
        self.assertEqual(job.last_checkpoint, "export:row3")
        self.assertIsNotNone(job.heartbeat_at)

    def test_pulse_force_triggers_immediately(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.RUNNING,
        )
        pulse = JobCheckpointPulse(job, every_n=999, every_seconds=3600, base_label="compute")

        self.assertTrue(pulse.hit(checkpoint="start", force=True))
        job.refresh_from_db()
        self.assertEqual(job.last_checkpoint, "compute:start")






class JobStatusSyncGameScenarioTests(TestCase):
    def test_sync_related_state_marks_game_scenario_failed_from_terminal_job(self):
        game = GameScenario.objects.create(name="Game Sync")
        game.last_run_status = "running"
        game.last_run_message = ""
        game.save(update_fields=["last_run_status", "last_run_message"])

        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.FAILED,
            game_scenario=game,
            message="Erreur (run game 1): boom",
        )

        from core.job_status_sync import sync_related_state_for_terminal_job

        sync_related_state_for_terminal_job(job)

        game.refresh_from_db()
        self.assertEqual(game.last_run_status, "failed")
        self.assertIn("boom", game.last_run_message)

    def test_recover_jobs_syncs_game_scenario_for_stale_running_job(self):
        game = GameScenario.objects.create(name="Game Recover")
        game.last_run_status = "running"
        game.save(update_fields=["last_run_status"])
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.RUNNING,
            game_scenario=game,
            started_at=timezone.now() - timedelta(minutes=200),
            heartbeat_at=timezone.now() - timedelta(minutes=180),
        )

        from core import tasks as core_tasks

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        game.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertEqual(game.last_run_status, "failed")
        self.assertIn("Recovered stale running job", game.last_run_message)
        self.assertGreaterEqual(result["synced_terminal"], 1)



class SchedulerConfigurationTests(TestCase):
    def test_beat_schedule_keeps_only_tracked_heavy_pipeline(self):
        from django.conf import settings

        schedule = settings.CELERY_BEAT_SCHEDULE
        self.assertIn("check-scheduled-alerts", schedule)
        self.assertIn("cleanup-stale-processing-jobs", schedule)
        self.assertIn("daily-system-refresh", schedule)

        self.assertNotIn("fetch-daily-bars", schedule)
        self.assertNotIn("compute-metrics", schedule)
        self.assertNotIn("send-daily-alerts", schedule)

        tasks = {entry["task"] for entry in schedule.values()}
        self.assertNotIn("core.tasks.fetch_daily_bars_task", tasks)
        self.assertNotIn("core.tasks.compute_metrics_task", tasks)
        self.assertNotIn("core.tasks.send_daily_alerts_task", tasks)
        self.assertIn("core.tasks.daily_system_refresh_job_task", tasks)


class DailySystemRefreshTaskTests(TestCase):
    @patch("redis.Redis.from_url")
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks.sync_market_caps_for_symbols")
    @patch("core.tasks._compute_metrics_for_scenario")
    @patch("core.tasks._fetch_daily_bars_for_symbols")
    def test_daily_refresh_calls_market_cap_sync_after_fetch_and_before_compute(
        self,
        mock_fetch,
        mock_compute,
        mock_sync_caps,
        mock_benchmark_sync,
        mock_redis_from_url,
    ):
        from core import tasks as core_tasks

        call_order = []
        sym = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        sc = Scenario.objects.create(name="Scenario Refresh", active=True, history_years=2)
        sc.symbols.set([sym])

        mock_redis_from_url.return_value.set.return_value = True
        mock_benchmark_sync.return_value = {
            "source_symbols": 1, "benchmark_tickers": ["SPY"], "created": 1, "existing": 0,
            "dry_run": False, "skip_enrichment": False, "skip_ohlc": False, "ohlc": {"symbols": 1, "bars": 1}, "enrichment": None, "per_symbol": [],
        }
        mock_fetch.side_effect = lambda **kwargs: call_order.append("fetch") or {"symbols": 1, "bars": 1}
        mock_sync_caps.side_effect = lambda *args, **kwargs: call_order.append("market_caps") or {
            "fetched": 1, "inserted": 0, "updated": 0, "existing": 1, "skipped": 0, "errors": 0, "per_symbol": []
        }
        mock_compute.side_effect = lambda **kwargs: call_order.append("compute") or {"symbols": 1, "rows": 1, "full": False}

        result = core_tasks.daily_system_refresh_job_task.run()

        self.assertEqual(call_order, ["fetch", "market_caps", "compute"])
        mock_benchmark_sync.assert_not_called()
        self.assertIn("market_caps_fetched=1", result)

    @override_settings(EODHD_MARKET_CAP_SYNC_START_DATE="2020-01-01")
    @patch("redis.Redis.from_url")
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks.sync_market_caps_for_symbols")
    @patch("core.tasks._compute_metrics_for_scenario", return_value={"symbols": 0, "rows": 0, "full": False})
    @patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 0, "bars": 0})
    def test_daily_refresh_passes_all_active_symbols_and_default_dates(
        self,
        mock_fetch,
        mock_compute,
        mock_sync_caps,
        mock_benchmark_sync,
        mock_redis_from_url,
    ):
        from core import tasks as core_tasks

        sym_a = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        sym_b = Symbol.objects.create(ticker="BBB", exchange="NASDAQ", active=True)
        Symbol.objects.create(ticker="ZZZ", exchange="NASDAQ", active=False)
        sc = Scenario.objects.create(name="Scenario Refresh Scope", active=True, history_years=2)
        sc.symbols.set([sym_a, sym_b])

        mock_redis_from_url.return_value.set.return_value = True
        mock_benchmark_sync.return_value = {
            "source_symbols": 2, "benchmark_tickers": ["SPY"], "created": 1, "existing": 0,
            "dry_run": False, "skip_enrichment": False, "skip_ohlc": False, "ohlc": {"symbols": 1, "bars": 1}, "enrichment": None, "per_symbol": [],
        }
        mock_sync_caps.return_value = {
            "fetched": 0, "inserted": 0, "updated": 0, "existing": 0, "skipped": 0, "errors": 0, "per_symbol": []
        }

        core_tasks.daily_system_refresh_job_task.run()

        args, kwargs = mock_sync_caps.call_args
        self.assertEqual([symbol.ticker for symbol in args[0]], ["AAA", "BBB"])
        self.assertEqual(args[1], date(2020, 1, 1))
        self.assertEqual(args[2], timezone.now().date())

    @patch("redis.Redis.from_url")
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks.sync_market_caps_for_symbols")
    @patch("core.tasks._compute_metrics_for_scenario", return_value={"symbols": 0, "rows": 0, "full": False})
    @patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 0, "bars": 0})
    def test_daily_refresh_keeps_running_when_market_cap_service_reports_symbol_errors(
        self,
        mock_fetch,
        mock_compute,
        mock_sync_caps,
        mock_benchmark_sync,
        mock_redis_from_url,
    ):
        from core import tasks as core_tasks

        sym = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        sc = Scenario.objects.create(name="Scenario Refresh Errors", active=True, history_years=2)
        sc.symbols.set([sym])

        mock_redis_from_url.return_value.set.return_value = True
        mock_benchmark_sync.return_value = {
            "source_symbols": 1, "benchmark_tickers": ["SPY"], "created": 1, "existing": 0,
            "dry_run": False, "skip_enrichment": False, "skip_ohlc": False, "ohlc": {"symbols": 1, "bars": 1}, "enrichment": None, "per_symbol": [],
        }
        mock_sync_caps.return_value = {
            "fetched": 1, "inserted": 0, "updated": 0, "existing": 0, "skipped": 0, "errors": 2, "per_symbol": []
        }

        result = core_tasks.daily_system_refresh_job_task.run()

        self.assertIn("market_caps_errors=2", result)

    @patch("redis.Redis.from_url")
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks.sync_market_caps_for_symbols", side_effect=RuntimeError("eodhd down"))
    @patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 0, "bars": 0})
    def test_daily_refresh_fails_on_fatal_market_cap_sync_exception(
        self,
        mock_fetch,
        mock_sync_caps,
        mock_benchmark_sync,
        mock_redis_from_url,
    ):
        from core import tasks as core_tasks

        sym = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        sc = Scenario.objects.create(name="Scenario Refresh Fatal", active=True, history_years=2)
        sc.symbols.set([sym])

        mock_redis_from_url.return_value.set.return_value = True
        mock_benchmark_sync.return_value = {
            "source_symbols": 1, "benchmark_tickers": ["SPY"], "created": 1, "existing": 0,
            "dry_run": False, "skip_enrichment": False, "skip_ohlc": False, "ohlc": {"symbols": 1, "bars": 1}, "enrichment": None, "per_symbol": [],
        }
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.COMPUTE_METRICS, status=ProcessingJob.Status.PENDING)

        with self.assertRaises(RuntimeError):
            core_tasks.daily_system_refresh_job_task.run(job_id=job.id)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("eodhd down", job.error)

    @override_settings(ENABLE_DAILY_BENCHMARK_ETF_SYNC=True)
    @patch("redis.Redis.from_url")
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks.sync_market_caps_for_symbols")
    @patch("core.tasks._compute_metrics_for_scenario")
    @patch("core.tasks._fetch_daily_bars_for_symbols")
    def test_daily_refresh_calls_shared_benchmark_service_when_flag_enabled(
        self,
        mock_fetch,
        mock_compute,
        mock_sync_caps,
        mock_benchmark_sync,
        mock_redis_from_url,
    ):
        from core import tasks as core_tasks

        call_order = []
        sym = Symbol.objects.create(ticker="AAA", exchange="NYSE", country="US", sector="Technology", active=True)
        sc = Scenario.objects.create(name="Scenario Refresh Bench", active=True, history_years=2)
        sc.symbols.set([sym])

        mock_redis_from_url.return_value.set.return_value = True
        mock_benchmark_sync.side_effect = lambda *args, **kwargs: call_order.append("benchmark") or {
            "source_symbols": 1, "benchmark_tickers": ["SPY", "XLK"], "created": 2, "existing": 0,
            "dry_run": False, "skip_enrichment": False, "skip_ohlc": False, "ohlc": {"symbols": 2, "bars": 50}, "enrichment": None, "per_symbol": [],
        }
        mock_fetch.side_effect = lambda **kwargs: call_order.append("fetch") or {"symbols": 3, "bars": 99}
        mock_sync_caps.side_effect = lambda *args, **kwargs: call_order.append("market_caps") or {
            "fetched": 1, "inserted": 0, "updated": 0, "existing": 1, "skipped": 0, "errors": 0, "per_symbol": []
        }
        mock_compute.side_effect = lambda **kwargs: call_order.append("compute") or {"symbols": 1, "rows": 1, "full": False}

        core_tasks.daily_system_refresh_job_task.run()

        self.assertEqual(call_order, ["benchmark", "fetch", "market_caps", "compute"])
        mock_benchmark_sync.assert_called_once()
        self.assertEqual(mock_benchmark_sync.call_args.kwargs["skip_ohlc"], False)

    @override_settings(ENABLE_DAILY_BENCHMARK_ETF_SYNC=False)
    @patch("redis.Redis.from_url")
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks.sync_market_caps_for_symbols", return_value={"fetched": 0, "inserted": 0, "updated": 0, "existing": 0, "skipped": 0, "errors": 0, "per_symbol": []})
    @patch("core.tasks._compute_metrics_for_scenario", return_value={"symbols": 0, "rows": 0, "full": False})
    @patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 0, "bars": 0})
    def test_daily_refresh_does_not_call_benchmark_service_when_flag_disabled(
        self,
        mock_fetch,
        mock_compute,
        mock_sync_caps,
        mock_benchmark_sync,
        mock_redis_from_url,
    ):
        from core import tasks as core_tasks

        sym = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        sc = Scenario.objects.create(name="Scenario Refresh Bench Off", active=True, history_years=2)
        sc.symbols.set([sym])

        mock_redis_from_url.return_value.set.return_value = True

        core_tasks.daily_system_refresh_job_task.run()

        mock_benchmark_sync.assert_not_called()

    @override_settings(ENABLE_DAILY_BENCHMARK_ETF_SYNC=True)
    @patch("core.tasks.sync_benchmark_etfs_for_symbols")
    @patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 1, "bars": 10})
    def test_fetch_daily_bars_task_calls_benchmark_service_before_normal_refresh(self, mock_fetch, mock_benchmark_sync):
        from core import tasks as core_tasks

        call_order = []
        Symbol.objects.create(ticker="AAA", exchange="NYSE", country="US", sector="Technology", active=True)
        Scenario.objects.create(name="Scenario Fetch Bench", active=True, history_years=2)

        mock_benchmark_sync.side_effect = lambda *args, **kwargs: call_order.append("benchmark") or {
            "source_symbols": 1, "benchmark_tickers": ["SPY", "XLK"], "created": 2, "existing": 0,
            "dry_run": False, "skip_enrichment": False, "skip_ohlc": False, "ohlc": {"symbols": 2, "bars": 50}, "enrichment": None, "per_symbol": [],
        }
        mock_fetch.side_effect = lambda **kwargs: call_order.append("fetch") or {"symbols": 3, "bars": 10}

        result = core_tasks.fetch_daily_bars_task()

        self.assertEqual(call_order, ["benchmark", "fetch"])
        self.assertIn("benchmark_sync", result)


class CleanupStaleProcessingJobsTaskTests(TestCase):
    def test_cleanup_task_marks_requested_pending_cancelled_via_recovery_engine(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            cancel_requested=True,
        )

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)
        self.assertEqual(result["cancelled"], 1)
        self.assertEqual(result["updated"], 1)

    def test_cleanup_task_marks_requested_pending_killed_via_recovery_engine(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            kill_requested=True,
            cancel_requested=True,
        )

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.KILLED)
        self.assertEqual(result["killed"], 1)
        self.assertEqual(result["updated"], 1)

    def test_cleanup_task_syncs_backtest_state_for_stale_running_job(self):
        bt = Backtest.objects.create(name="BT stale cleanup", scenario=Scenario.objects.create(name="Scenario cleanup"))
        bt.status = Backtest.Status.RUNNING
        bt.save(update_fields=["status"])
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.RUNNING,
            backtest=bt,
            started_at=timezone.now() - timedelta(minutes=200),
            heartbeat_at=timezone.now() - timedelta(minutes=180),
        )

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        bt.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertEqual(bt.status, Backtest.Status.FAILED)
        self.assertEqual(result["failed"], 1)
        self.assertGreaterEqual(result["synced_terminal"], 1)


class RecoverJobsCommandTests(TestCase):
    def _make_backtest(self):
        scenario = Scenario.objects.create(name="Scenario Recover")
        return Backtest.objects.create(name="BT Recover", scenario=scenario)

    def test_recover_jobs_marks_stale_running_failed_and_syncs_backtest(self):
        bt = self._make_backtest()
        bt.status = Backtest.Status.RUNNING
        bt.save(update_fields=["status"])
        stale_hb = timezone.now() - timedelta(minutes=120)
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.RUNNING,
            backtest=bt,
            started_at=timezone.now() - timedelta(minutes=150),
            heartbeat_at=stale_hb,
        )
        out = StringIO()
        call_command("recover_jobs", stdout=out)
        job.refresh_from_db()
        bt.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("Recovered stale running job", job.error)
        self.assertEqual(bt.status, Backtest.Status.FAILED)

    def test_recover_jobs_marks_requested_pending_cancelled(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.PENDING,
            cancel_requested=True,
        )
        out = StringIO()
        call_command("recover_jobs", stdout=out)
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)
        self.assertIsNotNone(job.finished_at)
        self.assertIn("cancel request", job.message)

    def test_recover_jobs_dry_run_keeps_job_unchanged(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.RUNNING,
            started_at=timezone.now() - timedelta(minutes=200),
            heartbeat_at=timezone.now() - timedelta(minutes=180),
        )
        out = StringIO()
        call_command("recover_jobs", "--dry-run", stdout=out)
        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.RUNNING)
        self.assertIn("Dry-run only", out.getvalue())


from unittest.mock import patch
from types import SimpleNamespace

from core import tasks as core_tasks


class RunBacktestTaskRegressionTests(TestCase):
    def test_run_backtest_task_accepts_task_request_without_self_nameerror(self):
        scenario = Scenario.objects.create(name="Scenario Backtest")
        bt = Backtest.objects.create(name="BT Backtest", scenario=scenario)

        fake_prep = SimpleNamespace(did_fetch_bars=False, did_compute_metrics=False, notes=[])
        fake_result = SimpleNamespace(results={"tickers": {}, "portfolio": {"daily": [], "kpi": {}}})

        with patch("core.services.backtesting.prep.prepare_backtest_data", return_value=fake_prep), \
             patch("core.services.backtesting.engine.run_backtest", return_value=fake_result), \
             patch("core.models.BacktestPortfolioDaily.objects.filter"), \
             patch("core.models.BacktestPortfolioKPI.objects.update_or_create"):
            msg = core_tasks.run_backtest_task(bt.id, task_request=SimpleNamespace(id="task-1", hostname="celery@test"))

        self.assertEqual(msg, "ok")


class RequestedStopRecoveryTests(TestCase):
    def test_cleanup_task_marks_cancel_requested_running_job_cancelled_quickly(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.RUNNING,
            cancel_requested=True,
            started_at=timezone.now() - timedelta(minutes=10),
            heartbeat_at=timezone.now() - timedelta(minutes=4),
        )

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)
        self.assertEqual(result["cancelled"], 1)
        self.assertEqual(result["requested_stop_minutes"], 1)

    def test_cleanup_task_marks_kill_requested_running_job_killed_quickly(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_GAME,
            status=ProcessingJob.Status.RUNNING,
            cancel_requested=True,
            kill_requested=True,
            started_at=timezone.now() - timedelta(minutes=10),
            heartbeat_at=timezone.now() - timedelta(minutes=4),
        )

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.KILLED)
        self.assertEqual(result["killed"], 1)


class SingleQueueRecoveryThresholdTests(TestCase):
    def test_cleanup_task_recovers_running_job_with_old_heartbeat_quickly(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            status=ProcessingJob.Status.RUNNING,
            started_at=timezone.now() - timedelta(minutes=5),
            heartbeat_at=timezone.now() - timedelta(minutes=3),
        )

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["heartbeat_minutes"], 2)

    def test_cleanup_task_recovers_pending_job_quickly(self):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.PENDING,
        )
        ProcessingJob.objects.filter(id=job.id).update(created_at=timezone.now() - timedelta(minutes=11))

        result = core_tasks.cleanup_stale_processing_jobs_task()

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertEqual(result["failed"], 1)
        self.assertEqual(result["pending_minutes"], 10)
