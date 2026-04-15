from __future__ import annotations

from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import Backtest, ProcessingJob, Scenario


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

from core.job_tracking import JobCancelled, JobKilled, job_checkpoint, mark_job_running


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
