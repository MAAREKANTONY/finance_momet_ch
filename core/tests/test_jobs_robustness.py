from __future__ import annotations

from datetime import timedelta
from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, TestCase
from django.urls import reverse
from django.utils import timezone

from core.models import ProcessingJob


class JobControlViewTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="ops", password="secret123")
        self.client.force_login(self.user)

    def test_cancel_marks_pending_job_cancelled_immediately(self):
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.RUN_BACKTEST, status=ProcessingJob.Status.PENDING)
        response = self.client.post(reverse("job_cancel", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)
        self.assertIsNotNone(job.finished_at)

    def test_cancel_marks_running_job_for_cooperative_stop(self):
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.RUN_BACKTEST, status=ProcessingJob.Status.RUNNING)
        response = self.client.post(reverse("job_cancel", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.RUNNING)

    @patch("celery.app.control.Control.revoke")
    def test_kill_marks_pending_job_killed_immediately(self, revoke_mock):
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.PENDING,
            task_id="abc123",
        )
        response = self.client.post(reverse("job_kill", args=[job.pk]))
        self.assertEqual(response.status_code, 302)
        job.refresh_from_db()
        self.assertTrue(job.kill_requested)
        self.assertTrue(job.cancel_requested)
        self.assertEqual(job.status, ProcessingJob.Status.KILLED)
        self.assertGreaterEqual(revoke_mock.call_count, 1)

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
