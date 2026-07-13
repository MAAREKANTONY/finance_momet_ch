from __future__ import annotations

from unittest.mock import patch

from django.conf import settings
from django.test import TestCase
from django.test.utils import override_settings

from core.job_broker import (
    BrokerQueueSnapshot,
    broker_queue_snapshot,
    get_broker_queue_length,
    get_redis_client,
    purge_broker_queue,
    sample_broker_queue,
)
from core.job_launch import launch_processing_job
from core.models import ProcessingJob
from core.tasks import daily_system_refresh_job_task
from stockalert.celery import app as celery_app


class TestCelerySettingsIsolationTests(TestCase):
    def test_test_settings_use_memory_celery_transport(self):
        self.assertTrue(settings.TESTING)
        self.assertEqual(settings.CELERY_BROKER_URL, "memory://")
        self.assertEqual(settings.CELERY_RESULT_BACKEND, "cache+memory://")
        self.assertFalse(settings.CELERY_TASK_ALWAYS_EAGER)
        self.assertTrue(settings.CELERY_TASK_EAGER_PROPAGATES)
        self.assertNotIn("redis://", settings.CELERY_BROKER_URL)
        self.assertNotIn("redis://", settings.CELERY_RESULT_BACKEND)

    def test_celery_app_uses_memory_transport_during_tests(self):
        self.assertEqual(celery_app.conf.broker_url, "memory://")
        self.assertEqual(celery_app.conf.result_backend, "cache+memory://")
        self.assertFalse(celery_app.conf.task_always_eager)
        self.assertTrue(celery_app.conf.task_eager_propagates)


class BrokerQueueTestIsolationTests(TestCase):
    @patch("core.job_broker.redis.Redis.from_url")
    def test_broker_queue_snapshot_does_not_construct_redis_client_during_tests(self, redis_from_url):
        snapshot = broker_queue_snapshot(queue_name="critical", sample_limit=5)

        self.assertEqual(snapshot, BrokerQueueSnapshot(queue_name="critical", length=0, samples=[], error=""))
        redis_from_url.assert_not_called()

    @patch("core.job_broker.redis.Redis.from_url")
    def test_purge_broker_queue_does_not_construct_redis_client_during_tests(self, redis_from_url):
        removed = purge_broker_queue(queue_name="critical")

        self.assertEqual(removed, 0)
        redis_from_url.assert_not_called()

    @patch("core.job_broker.redis.Redis.from_url")
    def test_queue_length_and_samples_do_not_construct_redis_client_during_tests(self, redis_from_url):
        self.assertEqual(get_broker_queue_length("critical"), 0)
        self.assertEqual(sample_broker_queue("critical", limit=5), [])
        redis_from_url.assert_not_called()

    @patch("core.job_broker.redis.Redis.from_url")
    def test_get_redis_client_is_disabled_during_tests_without_constructing_redis(self, redis_from_url):
        with self.assertRaisesMessage(RuntimeError, "disabled during tests"):
            get_redis_client()

        redis_from_url.assert_not_called()

    @override_settings(TESTING=False, CELERY_BROKER_URL="redis://runtime-redis:6379/9")
    @patch("core.job_broker.redis.Redis.from_url")
    def test_broker_helpers_still_use_configured_redis_outside_test_mode(self, redis_from_url):
        client = redis_from_url.return_value
        client.llen.return_value = 4
        client.lrange.return_value = []

        self.assertEqual(get_broker_queue_length("celery"), 4)
        self.assertEqual(purge_broker_queue("celery"), 4)
        self.assertEqual(sample_broker_queue("celery", limit=2), [])

        redis_from_url.assert_any_call("redis://runtime-redis:6379/9")
        client.delete.assert_called_once_with("celery")


class JobLaunchBrokerIsolationTests(TestCase):
    @patch("core.job_broker.redis.Redis.from_url")
    @patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: fn())
    def test_launch_processing_job_publishes_only_to_memory_broker_during_tests(
        self,
        _on_commit,
        redis_from_url,
    ):
        outcome = launch_processing_job(
            task=daily_system_refresh_job_task,
            job_type=ProcessingJob.JobType.COMPUTE_METRICS,
            message="En attente d'execution",
        )

        self.assertIsNone(outcome.dispatch_error)
        outcome.job.refresh_from_db()
        self.assertEqual(outcome.job.status, ProcessingJob.Status.PENDING)
        self.assertTrue(outcome.job.task_id)
        redis_from_url.assert_not_called()
