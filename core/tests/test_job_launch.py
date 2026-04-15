from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock, patch

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.job_launch import dispatch_task_after_commit, launch_processing_job
from core.models import Backtest, ProcessingJob, Scenario


class LaunchProcessingJobTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="launcher", password="secret123")
        self.scenario = Scenario.objects.create(name="Scenario Launch")
        self.backtest = Backtest.objects.create(name="BT Launch", scenario=self.scenario)

    def test_launch_processing_job_registers_on_commit_and_persists_task_id(self):
        task = Mock()
        task.apply_async.return_value = SimpleNamespace(id="celery-task-123")
        callbacks = []

        with patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: callbacks.append(fn)) as mocked_on_commit:
            outcome = launch_processing_job(
                task=task,
                job_type=ProcessingJob.JobType.RUN_BACKTEST,
                backtest=self.backtest,
                scenario=self.scenario,
                created_by=self.user,
                message="En attente d'exécution",
                task_kwargs={"backtest_id": self.backtest.id, "user_id": self.user.id},
            )

        self.assertEqual(outcome.job.status, ProcessingJob.Status.PENDING)
        self.assertEqual(outcome.job.task_id, "")
        self.assertIsNone(outcome.dispatch_error)
        task.apply_async.assert_not_called()
        mocked_on_commit.assert_called_once()
        self.assertEqual(len(callbacks), 1)

        callbacks[0]()

        task.apply_async.assert_called_once_with(kwargs={"backtest_id": self.backtest.id, "user_id": self.user.id, "job_id": outcome.job.id})
        outcome.job.refresh_from_db()
        self.assertEqual(outcome.job.status, ProcessingJob.Status.PENDING)
        self.assertEqual(outcome.job.task_id, "celery-task-123")

    def test_launch_processing_job_marks_job_failed_when_dispatch_raises(self):
        task = Mock()
        task.apply_async.side_effect = RuntimeError("broker down")

        with patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: fn()):
            outcome = launch_processing_job(
                task=task,
                job_type=ProcessingJob.JobType.FETCH_BARS,
                scenario=self.scenario,
                created_by=self.user,
                message="En attente d'exécution",
                task_kwargs={"scenario_id": self.scenario.id, "user_id": self.user.id},
            )

        self.assertIsNotNone(outcome.dispatch_error)
        outcome.job.refresh_from_db()
        self.assertEqual(outcome.job.status, ProcessingJob.Status.FAILED)
        self.assertIn("Task dispatch failed: broker down", outcome.job.error)
        self.assertIsNotNone(outcome.job.finished_at)
        self.assertEqual(outcome.job.task_id, "")

    def test_dispatch_task_after_commit_returns_task_id(self):
        task = Mock()
        task.apply_async.return_value = SimpleNamespace(id="ops-task-456")
        callbacks = []

        with patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: callbacks.append(fn)):
            outcome = dispatch_task_after_commit(task=task, task_kwargs={"force": True})

        self.assertEqual(outcome.task_id, "")
        self.assertIsNone(outcome.dispatch_error)
        self.assertEqual(len(callbacks), 1)
        callbacks[0]()
        task.apply_async.assert_called_once_with(args=[], kwargs={"force": True})

    def test_dispatch_task_after_commit_surfaces_dispatch_error(self):
        task = Mock()
        task.apply_async.side_effect = RuntimeError("broker down")

        with patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: fn()):
            outcome = dispatch_task_after_commit(task=task)

        self.assertIsInstance(outcome.dispatch_error, RuntimeError)
        self.assertEqual(outcome.task_id, "")
