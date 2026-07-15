import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.contrib.messages import get_messages
from django.http import Http404
from django.test import Client, RequestFactory, SimpleTestCase, TestCase
from django.urls import reverse

from core import views
from core.models import Alert, Backtest, GameScenario, ProcessingJob, Scenario, Study, Symbol
from core.path_safety import resolve_existing_file_within


class ScenarioDeleteSecurityTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="release-gate", password="secret")

    def test_anonymous_get_is_redirected_without_deleting(self):
        scenario = Scenario.objects.create(name="Anonymous GET")

        response = self.client.get(reverse("scenario_delete", args=[scenario.pk]))

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith("/accounts/login/"))
        self.assertTrue(Scenario.objects.filter(pk=scenario.pk).exists())

    def test_anonymous_post_is_redirected_without_deleting(self):
        scenario = Scenario.objects.create(name="Anonymous POST")

        response = self.client.post(reverse("scenario_delete", args=[scenario.pk]))

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith("/accounts/login/"))
        self.assertTrue(Scenario.objects.filter(pk=scenario.pk).exists())

    def test_authenticated_get_is_method_not_allowed_without_deleting(self):
        scenario = Scenario.objects.create(name="Authenticated GET")
        self.client.force_login(self.user)

        response = self.client.get(reverse("scenario_delete", args=[scenario.pk]))

        self.assertEqual(response.status_code, 405)
        self.assertTrue(Scenario.objects.filter(pk=scenario.pk).exists())

    def test_authenticated_post_deletes_unprotected_scenario(self):
        scenario = Scenario.objects.create(name="Deletable")
        self.client.force_login(self.user)

        response = self.client.post(reverse("scenario_delete", args=[scenario.pk]))

        self.assertRedirects(response, reverse("scenarios_page"), fetch_redirect_response=False)
        self.assertFalse(Scenario.objects.filter(pk=scenario.pk).exists())

    def test_authenticated_post_preserves_protected_error_handling(self):
        scenario = Scenario.objects.create(name="Protected")
        backtest = Backtest.objects.create(name="Protected backtest", scenario=scenario)
        self.client.force_login(self.user)

        response = self.client.post(reverse("scenario_delete", args=[scenario.pk]))

        self.assertRedirects(response, reverse("scenarios_page"), fetch_redirect_response=False)
        self.assertTrue(Scenario.objects.filter(pk=scenario.pk).exists())
        self.assertTrue(Backtest.objects.filter(pk=backtest.pk).exists())
        self.assertTrue(any("Impossible de supprimer" in str(message) for message in get_messages(response.wsgi_request)))

    def test_ui_uses_post_and_csrf_token(self):
        scenario = Scenario.objects.create(name="CSRF UI")
        csrf_client = Client(enforce_csrf_checks=True)
        csrf_client.force_login(self.user)

        page = csrf_client.get(reverse("scenarios_page"))

        delete_url = reverse("scenario_delete", args=[scenario.pk])
        self.assertContains(page, f'<form method="post" action="{delete_url}"', html=False)
        self.assertContains(page, 'name="csrfmiddlewaretoken"', html=False)
        rejected = csrf_client.post(delete_url)
        self.assertEqual(rejected.status_code, 403)
        self.assertTrue(Scenario.objects.filter(pk=scenario.pk).exists())

        token = csrf_client.cookies["csrftoken"].value
        accepted = csrf_client.post(delete_url, {"csrfmiddlewaretoken": token})
        self.assertEqual(accepted.status_code, 302)
        self.assertFalse(Scenario.objects.filter(pk=scenario.pk).exists())


class GameScenarioListSecurityTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="game-list", password="secret")

    def test_anonymous_user_is_redirected(self):
        response = self.client.get(reverse("game_scenarios_page"))

        self.assertEqual(response.status_code, 302)
        self.assertTrue(response.url.startswith("/accounts/login/"))

    def test_authenticated_user_is_accepted(self):
        self.client.force_login(self.user)

        response = self.client.get(reverse("game_scenarios_page"))

        self.assertEqual(response.status_code, 200)


class ExportPathConfinementTests(SimpleTestCase):
    def test_existing_child_file_is_accepted(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "exports"
            child = root / "nested" / "result.xlsx"
            child.parent.mkdir(parents=True)
            child.write_text("result", encoding="utf-8")

            self.assertEqual(resolve_existing_file_within(child, root), child.resolve())

    def test_prefixed_sibling_directory_is_rejected(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "exports"
            sibling_file = Path(tmp) / "exports-secret" / "result.xlsx"
            root.mkdir()
            sibling_file.parent.mkdir()
            sibling_file.write_text("secret", encoding="utf-8")

            self.assertIsNone(resolve_existing_file_within(sibling_file, root))

    def test_nonexistent_or_invalid_path_is_rejected(self):
        with TemporaryDirectory() as tmp:
            root = Path(tmp) / "exports"
            root.mkdir()

            self.assertIsNone(resolve_existing_file_within(root / "missing.xlsx", root))
            self.assertIsNone(resolve_existing_file_within("invalid\x00path", root))


class DownloadPathCallSiteTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="download-user", password="secret")
        self.job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.EXPORT_ALERTS_CSV,
            created_by=self.user,
            output_file="/data/exports-secret/report.csv",
        )

    @patch("core.job_views.resolve_existing_file_within", return_value=None)
    def test_routed_download_uses_shared_confinement(self, resolve_path):
        self.client.force_login(self.user)

        response = self.client.get(reverse("job_download", args=[self.job.pk]))

        self.assertEqual(response.status_code, 404)
        resolve_path.assert_called_once_with(self.job.output_file, "/data/exports")

    @patch("core.views.resolve_existing_file_within", return_value=None)
    def test_legacy_download_call_site_uses_shared_confinement(self, resolve_path):
        request = RequestFactory().get(f"/jobs/{self.job.pk}/download/")
        request.user = self.user

        with self.assertRaises(Http404):
            views.job_download(request, self.job.pk)

        resolve_path.assert_called_once_with(self.job.output_file, "/data/exports")


class InvalidWebFilterTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="invalid-filter", password="secret")
        self.client.force_login(self.user)

    def test_non_numeric_alert_scenario_filter_is_ignored(self):
        symbol = Symbol.objects.create(ticker="SAFE", exchange="US")
        scenario = Scenario.objects.create(name="Visible scenario")
        Alert.objects.create(symbol=symbol, scenario=scenario, date="2026-01-02", alerts="A1")

        response = self.client.get(reverse("alerts_table"), {"scenario": "abc"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "SAFE")

    @patch("core.views.purge_scenario_derived_data")
    @patch("core.views._apply_universe_to_scenario")
    def test_non_numeric_universe_id_redirects_without_modification(self, apply_universe, purge_data):
        selected = Symbol.objects.create(ticker="KEEP", exchange="US")
        scenario = Scenario.objects.create(name="Study scenario")
        scenario.symbols.add(selected)
        study = Study.objects.create(name="Study", scenario=scenario, created_by=self.user)
        symbol_ids_before = list(scenario.symbols.values_list("id", flat=True))

        response = self.client.post(
            reverse("study_apply_universe", args=[study.pk]),
            {"universe_id": "abc", "mode": "replace"},
        )

        self.assertRedirects(response, reverse("study_edit", args=[study.pk]), fetch_redirect_response=False)
        self.assertEqual(list(scenario.symbols.values_list("id", flat=True)), symbol_ids_before)
        apply_universe.assert_not_called()
        purge_data.assert_not_called()
        self.assertTrue(any("Universe invalide" in str(message) for message in get_messages(response.wsgi_request)))


class SecretKeySettingsTests(SimpleTestCase):
    project_root = Path(__file__).resolve().parents[2]

    def run_settings_import(self, *, secret_key: str, debug: str, testing: bool = False):
        env = os.environ.copy()
        env.update({"DJANGO_SECRET_KEY": secret_key, "DJANGO_DEBUG": debug})
        prefix = "import sys; sys.argv.append('test'); " if testing else ""
        return subprocess.run(
            [sys.executable, "-c", f"{prefix}from stockalert import settings; print(settings.SECRET_KEY)"],
            cwd=self.project_root,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_production_without_secret_key_fails_explicitly(self):
        result = self.run_settings_import(secret_key="", debug="0")

        self.assertNotEqual(result.returncode, 0)
        self.assertIn("DJANGO_SECRET_KEY is required", result.stderr)

    def test_production_with_secret_key_starts(self):
        result = self.run_settings_import(secret_key="production-secret", debug="0")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "production-secret")

    def test_django_tests_use_local_fallback(self):
        result = self.run_settings_import(secret_key="", debug="0", testing=True)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "dev-secret-key")

    def test_explicit_development_uses_local_fallback(self):
        result = self.run_settings_import(secret_key="", debug="1")

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "dev-secret-key")
