from __future__ import annotations

import json
import inspect
from io import StringIO
from datetime import date
from types import SimpleNamespace
from unittest.mock import ANY, Mock, patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase
from django.urls import reverse

from core.models import ProcessingJob, Symbol, UniverseDefinition, UniverseMembership
from core.services.csi300_eodhd_metadata import CSI300EODHDMetadataReport
from core.tasks import enrich_csi300_metadata_job_task


def _report_json_from_message(message: str) -> dict:
    for line in (message or "").splitlines():
        if line.startswith("report_json="):
            return json.loads(line.split("=", 1)[1])
    raise AssertionError("missing report_json line")


def _fake_report(*, dry_run: bool = True, errors: int = 0) -> CSI300EODHDMetadataReport:
    report = CSI300EODHDMetadataReport(
        dry_run=dry_run,
        processed=2,
        fetched=2,
        updated=1,
        unchanged=1,
        errors=errors,
        missing_sector=0,
        generic_sector=1,
        industries_present=2,
    )
    report.field_updates = {"sector": 1}
    report.raw_sector_counts = {"Financial Services": 1, "Other": 1}
    report.applied_sector_counts = {"Financial Services": 1}
    report.per_symbol = [
        {
            "symbol": "600000:SHG",
            "provider_symbol": "600000.SHG",
            "updated_fields": ["sector"],
            "error": "",
            "raw_sector": "Financial Services",
            "sector": "Financial Services",
            "industry_present": True,
        }
    ]
    if errors:
        report.per_symbol.append(
            {
                "symbol": "000001:SHE",
                "provider_symbol": "000001.SHE",
                "updated_fields": [],
                "error": "HTTP 404 url=https://eodhd.com/api/fundamentals/000001.SHE?api_token=secret",
                "raw_sector": "",
                "sector": "",
                "industry_present": False,
            }
        )
    return report


class CSI300MetadataUIViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="ui-csi300", password="secret123")
        self.client.force_login(self.user)

    def _create_csi300_membership(self) -> UniverseMembership:
        symbol = Symbol.objects.create(ticker="600000", exchange="SHG", name="Pudong Bank")
        universe = UniverseDefinition.objects.create(code="CSI300", name="CSI 300", active=True, source="manual_csv")
        return UniverseMembership.objects.create(
            universe=universe,
            symbol=symbol,
            ticker=symbol.ticker,
            exchange=symbol.exchange,
            provider_symbol="600000.SHG",
            valid_from=date(2020, 1, 1),
            source="manual_csv",
        )

    def _post_with_real_launch(self, data: dict, *, task_id: str = "task-csi300-1"):
        task_apply = Mock(return_value=SimpleNamespace(id=task_id))
        broker_snapshot = SimpleNamespace(queue_name="celery", length=0, samples=[])
        with (
            patch("core.tasks.enrich_csi300_metadata_job_task.apply_async", task_apply),
            patch("core.job_launch.broker_queue_snapshot", return_value=broker_snapshot),
            patch("core.job_launch.transaction.on_commit", side_effect=lambda fn: fn()),
        ):
            response = self.client.post(reverse("symbols_csi300_eodhd_metadata"), data)
        return response, task_apply

    def test_symbols_page_displays_csi300_eodhd_section(self):
        self._create_csi300_membership()

        response = self.client.get(reverse("symbols_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Métadonnées CSI300 — EODHD", body)
        self.assertIn("Analyser les métadonnées CSI300", body)
        self.assertIn("Appliquer l’enrichissement CSI300", body)
        self.assertIn("1 symboles distincts", body)

    def test_csi300_eodhd_action_requires_login(self):
        self.client.logout()

        response = self.client.post(reverse("symbols_csi300_eodhd_metadata"), {"mode": "dry_run"})

        self.assertEqual(response.status_code, 302)
        self.assertIn("/accounts/login/", response.url)

    def test_csi300_eodhd_action_rejects_get(self):
        response = self.client.get(reverse("symbols_csi300_eodhd_metadata"))

        self.assertEqual(response.status_code, 405)

    def test_invalid_mode_creates_no_job(self):
        response, task_apply = self._post_with_real_launch({"mode": "other"})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("symbols_page"))
        self.assertEqual(ProcessingJob.objects.count(), 0)
        task_apply.assert_not_called()

    def test_dry_run_creates_processing_job(self):
        response, _task_apply = self._post_with_real_launch({"mode": "dry_run"})

        job = ProcessingJob.objects.get()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("job_detail", args=[job.id]))
        self.assertEqual(job.job_type, ProcessingJob.JobType.ENRICH_METADATA)
        self.assertEqual(job.created_by, self.user)

    def test_enrich_metadata_job_type_has_french_label(self):
        self.assertEqual(ProcessingJob.JobType.ENRICH_METADATA.label, "Enrichissement des métadonnées")

    def test_makemigrations_check_dry_run_is_clean(self):
        out = StringIO()

        call_command("makemigrations", "--check", "--dry-run", stdout=out)

        self.assertIn("No changes detected", out.getvalue())

    def test_dry_run_schedules_task_with_apply_false(self):
        _response, task_apply = self._post_with_real_launch({"mode": "dry_run"})

        job = ProcessingJob.objects.get()
        task_apply.assert_called_once_with(
            kwargs={
                "apply": False,
                "user_id": self.user.id,
                "job_id": job.id,
            }
        )

    def test_dry_run_view_does_not_modify_symbols(self):
        symbol = Symbol.objects.create(ticker="600000", exchange="SHG", name="Before", sector="")

        self._post_with_real_launch({"mode": "dry_run"})

        symbol.refresh_from_db()
        self.assertEqual(symbol.name, "Before")
        self.assertEqual(symbol.sector, "")

    def test_apply_without_backend_confirmation_creates_no_job(self):
        response, task_apply = self._post_with_real_launch({"mode": "apply"})

        self.assertEqual(response.status_code, 302)
        self.assertEqual(ProcessingJob.objects.count(), 0)
        task_apply.assert_not_called()

    def test_apply_with_confirmation_creates_and_schedules_task(self):
        response, task_apply = self._post_with_real_launch({"mode": "apply", "confirm_apply": "1"})

        job = ProcessingJob.objects.get()
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("job_detail", args=[job.id]))
        task_apply.assert_called_once_with(
            kwargs={
                "apply": True,
                "user_id": self.user.id,
                "job_id": job.id,
            }
        )

    @patch("core.services.csi300_eodhd_metadata.enrich_csi300_symbols_from_eodhd_metadata")
    @patch("core.views.launch_processing_job")
    def test_view_does_not_call_provider_service(self, launch_mock, service_mock):
        launch_mock.return_value = SimpleNamespace(job=SimpleNamespace(id=77), dispatch_error=None)

        response = self.client.post(reverse("symbols_csi300_eodhd_metadata"), {"mode": "dry_run"})

        self.assertEqual(response.status_code, 302)
        service_mock.assert_not_called()

    @patch("core.views.enrich_symbols_metadata")
    def test_existing_twelvedata_missing_metadata_action_is_unchanged(self, enrich_mock):
        enrich_mock.return_value = {"processed": 1, "updated": 0, "unchanged": 1, "skipped": 0, "errors": 0, "per_symbol": []}
        Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)

        response = self.client.post(reverse("symbols_update_missing_metadata"))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("symbols_page"))
        enrich_mock.assert_called_once()

    @patch("core.views.launch_processing_job")
    def test_sp500_membership_is_not_modified_by_csi300_ui_launch(self, launch_mock):
        launch_mock.return_value = SimpleNamespace(job=SimpleNamespace(id=78), dispatch_error=None)
        symbol = Symbol.objects.create(ticker="AAPL", exchange="US", name="Apple")
        universe = UniverseDefinition.objects.create(code="SP500", name="S&P 500", active=True)
        membership = UniverseMembership.objects.create(
            universe=universe,
            symbol=symbol,
            ticker="AAPL",
            exchange="US",
            provider_symbol="AAPL.US",
            valid_from=date(2020, 1, 1),
        )

        self.client.post(reverse("symbols_csi300_eodhd_metadata"), {"mode": "dry_run"})

        membership.refresh_from_db()
        self.assertEqual(membership.provider_symbol, "AAPL.US")
        self.assertEqual(UniverseMembership.objects.filter(universe__code="SP500").count(), 1)


class CSI300MetadataJobTaskTests(TestCase):
    def _job(self) -> ProcessingJob:
        return ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.ENRICH_METADATA,
            status=ProcessingJob.Status.PENDING,
            message="queued",
        )

    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_task_calls_existing_service(self, service_mock):
        service_mock.return_value = _fake_report(dry_run=True)
        job = self._job()

        enrich_csi300_metadata_job_task.apply(kwargs={"job_id": job.id}).get()

        service_mock.assert_called_once()

    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_task_passes_apply_tickers_and_limit_to_service(self, service_mock):
        service_mock.return_value = _fake_report(dry_run=False)
        job = self._job()

        enrich_csi300_metadata_job_task.apply(
            kwargs={
                "job_id": job.id,
                "apply": True,
                "tickers": ["600000", "000001"],
                "limit": 2,
            }
        ).get()

        service_mock.assert_called_once_with(
            dry_run=False,
            tickers=["600000", "000001"],
            limit=2,
            progress_callback=ANY,
        )

    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_task_saves_structured_report_in_job_message(self, service_mock):
        service_mock.return_value = _fake_report(dry_run=True)
        job = self._job()

        enrich_csi300_metadata_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        payload = _report_json_from_message(job.message)
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("Métadonnées EODHD CSI300", job.message)
        self.assertIn("Rapport:", job.message)
        self.assertIn("Secteurs bruts:", job.message)
        self.assertEqual(payload["requested"], 2)
        self.assertEqual(payload["fetched"], 2)
        self.assertEqual(payload["updated"], 1)
        self.assertEqual(payload["raw_sector_distribution"], {"Financial Services": 1, "Other": 1})
        self.assertIn("useful_sectors", payload)
        self.assertIn("generic_sectors", payload)
        self.assertIn("missing_sectors", payload)

    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_partial_provider_errors_are_done_with_warning(self, service_mock):
        service_mock.return_value = _fake_report(dry_run=True, errors=1)
        job = self._job()

        enrich_csi300_metadata_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        payload = _report_json_from_message(job.message)
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("Avertissement: erreurs_partielles=1", job.message)
        self.assertIn("Erreurs:", job.message)
        self.assertEqual(payload["errors"], 1)
        self.assertEqual(payload["error_details"][0]["error"].count("secret"), 0)
        self.assertIn("api_token=***", payload["error_details"][0]["error"])

    @patch("core.tasks._job_update")
    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_task_records_progress_without_excessive_writes(self, service_mock, job_update_mock):
        job = self._job()

        def fake_service(*, dry_run, tickers, limit, progress_callback):
            report = CSI300EODHDMetadataReport(dry_run=dry_run)
            for processed in range(1, 61):
                report.processed = processed
                report.fetched = processed
                if processed % 3 == 0:
                    report.updated += 1
                progress_callback(report=report, candidate=None, processed=processed, total=60)
            return report

        service_mock.side_effect = fake_service

        enrich_csi300_metadata_job_task.apply(kwargs={"job_id": job.id}).get()

        self.assertEqual(job_update_mock.call_count, 3)
        messages = [call.kwargs.get("message", "") for call in job_update_mock.call_args_list]
        self.assertTrue(any("traités=25/60" in message for message in messages))
        self.assertTrue(any("traités=50/60" in message for message in messages))
        self.assertTrue(any("traités=60/60" in message for message in messages))

    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_task_detects_cancel_during_progress_checkpoint(self, service_mock):
        job = self._job()

        def fake_service(*, dry_run, tickers, limit, progress_callback):
            report = CSI300EODHDMetadataReport(dry_run=dry_run)
            for processed in range(1, 26):
                report.processed = processed
                if processed == 25:
                    ProcessingJob.objects.filter(id=job.id).update(cancel_requested=True)
                progress_callback(report=report, candidate=None, processed=processed, total=60)
            return report

        service_mock.side_effect = fake_service

        result = enrich_csi300_metadata_job_task.apply(kwargs={"job_id": job.id}).get()

        job.refresh_from_db()
        self.assertEqual(result, "annulé")
        self.assertEqual(job.status, ProcessingJob.Status.CANCELLED)
        self.assertIn("Annulé par l'utilisateur", job.message)

    @patch("core.tasks.enrich_csi300_symbols_from_eodhd_metadata")
    def test_global_service_error_marks_job_failed_and_sanitizes_secret(self, service_mock):
        service_mock.side_effect = RuntimeError(
            "provider failed https://eodhd.com/api/fundamentals/600000.SHG?api_token=secret"
        )
        job = self._job()

        with self.assertRaises(RuntimeError):
            enrich_csi300_metadata_job_task.apply(kwargs={"job_id": job.id}).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("api_token=***", job.error)
        self.assertNotIn("secret", job.error)

    def test_run_backtest_does_not_reference_csi300_metadata_job(self):
        from core.services.backtesting import engine

        source = inspect.getsource(engine.run_backtest)
        self.assertNotIn("enrich_csi300_metadata_job_task", source)
        self.assertNotIn("csi300_eodhd_metadata", source)
