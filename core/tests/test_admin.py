from django.contrib import admin
from django.contrib.admin.sites import AdminSite
from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase

from core.models import Backtest, ProcessingJob, Scenario, Symbol


class SymbolAdminTests(TestCase):
    def test_english_name_is_visible_and_searchable(self):
        model_admin = admin.site._registry[Symbol]

        self.assertIn("name_en", model_admin.list_display)
        self.assertIn("name_en", model_admin.search_fields)


class BacktestAdminTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.user = get_user_model().objects.create_superuser(
            username="admin",
            email="admin@example.com",
            password="secret123",
        )
        self.scenario = Scenario.objects.create(name="Scenario Admin")

    def test_backtest_admin_form_excludes_large_json_fields(self):
        model_admin = admin.site._registry[Backtest]
        request = self.factory.get("/admin/core/backtest/1/change/")
        request.user = self.user

        form_class = model_admin.get_form(request)

        self.assertNotIn("results", form_class.base_fields)
        self.assertNotIn("signal_lines", form_class.base_fields)
        self.assertNotIn("settings", form_class.base_fields)
        self.assertNotIn("universe_snapshot", form_class.base_fields)

    def test_backtest_admin_has_readonly_summaries_for_large_json_fields(self):
        model_admin = admin.site._registry[Backtest]

        self.assertIn("results_summary", model_admin.readonly_fields)
        self.assertIn("signal_lines_summary", model_admin.readonly_fields)
        self.assertIn("settings_summary", model_admin.readonly_fields)
        self.assertIn("universe_snapshot_summary", model_admin.readonly_fields)

    def test_backtest_admin_results_page_link_points_to_results_view(self):
        model_admin = admin.site._registry[Backtest]
        bt = Backtest.objects.create(name="BT Admin", scenario=self.scenario)

        html = model_admin.results_page_link(bt)

        self.assertIn(f"/backtests/{bt.id}/results/", html)


class ProcessingJobAdminTests(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.user = get_user_model().objects.create_superuser(
            username="ops-admin",
            email="ops-admin@example.com",
            password="secret123",
        )

    def test_processing_job_admin_form_excludes_large_text_fields(self):
        model_admin = admin.site._registry[ProcessingJob]
        request = self.factory.get("/admin/core/processingjob/1/change/")
        request.user = self.user

        form_class = model_admin.get_form(request)

        self.assertNotIn("message", form_class.base_fields)
        self.assertNotIn("error", form_class.base_fields)

    def test_processing_job_admin_has_readonly_summaries_for_large_text_fields(self):
        model_admin = admin.site._registry[ProcessingJob]

        self.assertIn("message_summary", model_admin.readonly_fields)
        self.assertIn("error_summary", model_admin.readonly_fields)

    def test_processing_job_admin_message_summary_reports_presence_and_size(self):
        model_admin = admin.site._registry[ProcessingJob]
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.FAILED,
            message="x" * 300,
            error="boom",
        )

        msg = model_admin.message_summary(job)
        err = model_admin.error_summary(job)

        self.assertIn("Message: yes", msg)
        self.assertIn("~300 bytes", msg)
        self.assertIn("Error: yes", err)
