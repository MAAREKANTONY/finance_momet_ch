from __future__ import annotations

from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Backtest, GameScenario, ProcessingJob, Scenario


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
        self.assertEqual(result["requested_stop_minutes"], 3)

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
