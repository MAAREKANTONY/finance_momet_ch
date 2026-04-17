from __future__ import annotations

from io import StringIO
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import Client, TestCase

from core.job_broker import BrokerQueueSnapshot


class SanitizeJobQueueCommandTests(TestCase):
    @patch("core.management.commands.sanitize_job_queue.broker_queue_snapshot")
    def test_command_inspects_queue_without_mutation(self, snapshot_mock):
        snapshot_mock.side_effect = [
            BrokerQueueSnapshot(queue_name="celery", length=7, samples=["core.tasks.compute_metrics_job_task#abc"], error=""),
            BrokerQueueSnapshot(queue_name="celery", length=7, samples=["core.tasks.compute_metrics_job_task#abc"], error=""),
        ]
        out = StringIO()

        call_command("sanitize_job_queue", stdout=out)

        text = out.getvalue()
        self.assertIn("Queue length before: 7", text)
        self.assertIn("Queue length after: 7", text)
        self.assertIn("compute_metrics_job_task", text)

    @patch("core.management.commands.sanitize_job_queue.purge_broker_queue", return_value=12)
    @patch("core.management.commands.sanitize_job_queue.broker_queue_snapshot")
    def test_command_purges_when_force_is_provided(self, snapshot_mock, purge_mock):
        snapshot_mock.side_effect = [
            BrokerQueueSnapshot(queue_name="celery", length=12, samples=[], error=""),
            BrokerQueueSnapshot(queue_name="celery", length=0, samples=[], error=""),
        ]
        out = StringIO()

        call_command("sanitize_job_queue", "--purge-broker", "--force", stdout=out)

        purge_mock.assert_called_once_with(queue_name="celery")
        self.assertIn("Broker queue purged: removed 12 queued message(s).", out.getvalue())


class JobsPageBrokerWarningTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="ops-broker", password="secret123")
        self.client = Client()
        self.client.force_login(self.user)

    @patch("core.job_views.broker_queue_snapshot")
    def test_jobs_page_shows_broker_backlog_warning(self, snapshot_mock):
        snapshot_mock.return_value = BrokerQueueSnapshot(
            queue_name="celery",
            length=9,
            samples=["core.tasks.compute_metrics_job_task#abc", "core.tasks.fetch_daily_bars_task#def"],
            error="",
        )

        response = self.client.get("/jobs/")

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Broker queue")
        self.assertContains(response, "Longueur actuelle: <strong>9</strong>", html=True)
        self.assertContains(response, "backlog broker détecté")


class JobsMaintenanceActionsTests(TestCase):
    def setUp(self):
        self.staff = get_user_model().objects.create_user(username="ops-admin", password="secret123", is_staff=True)
        self.user = get_user_model().objects.create_user(username="ops-user", password="secret123")
        self.client = Client()

    @patch("core.job_views.recover_jobs")
    def test_staff_can_trigger_recover_stale(self, recover_mock):
        recover_mock.return_value = ([], type("Stats", (), {"matched": 1, "updated": 1, "failed": 1, "cancelled": 0, "killed": 0, "synced_terminal": 0})())
        self.client.force_login(self.staff)

        response = self.client.post("/jobs/maintenance/recover/")

        self.assertEqual(response.status_code, 302)
        recover_mock.assert_called_once()

    @patch("core.job_views.purge_broker_queue", return_value=7)
    def test_staff_can_purge_broker_with_confirmation(self, purge_mock):
        self.client.force_login(self.staff)

        response = self.client.post("/jobs/maintenance/purge-broker/", {"confirm": "PURGE"})

        self.assertEqual(response.status_code, 302)
        purge_mock.assert_called_once()

    @patch("core.job_views.purge_broker_queue")
    def test_non_admin_cannot_purge_broker(self, purge_mock):
        self.client.force_login(self.user)

        response = self.client.post("/jobs/maintenance/purge-broker/", {"confirm": "PURGE"})

        self.assertEqual(response.status_code, 302)
        purge_mock.assert_not_called()
