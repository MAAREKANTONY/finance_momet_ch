from __future__ import annotations

from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase
from django.urls import reverse

from core.job_launch import JobLaunchOutcome
from core.models import (
    Backtest,
    ProcessingJob,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.backtest_currency import ResolvedCurrencyValidationError
from core.services.dynamic_universe_readiness import ReadinessAction, ReadinessCheck, ReadinessReport, readiness_confirmation_hash
from core.services.dynamic_universe_symbols import UniverseSymbolMappingError, UniverseSymbolMappingReport
from core.tasks import map_universe_membership_symbols_job_task, run_backtest_job_task
from tools.csi300_policy import CSI300_SUPPORTED_HISTORY_MESSAGE


class DynamicUniverseTriggerPageTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="du-trigger-user", password="secret123")
        self.staff_user = get_user_model().objects.create_user(
            username="du-trigger-admin",
            password="secret123",
            is_staff=True,
        )
        self.client = Client()
        self.client.force_login(self.user)

    def _login_staff(self):
        self.client.force_login(self.staff_user)

    def _uploaded_csv(self, body: str, *, name: str = "memberships.csv") -> SimpleUploadedFile:
        return SimpleUploadedFile(name, body.encode("utf-8"), content_type="text/csv")

    def _csi300_csv(self) -> str:
        return (
            "universe_code,symbol,exchange,mic,name,start_date,end_date,weight,provider_symbol,source,country,currency,sector,industry\n"
            "CSI300,600519,SHG,XSHG,Kweichow Moutai,2020-01-01,,0.052,600519.SHG,manual_csv,CN,CNY,Consumer Staples,Distillers & Wineries\n"
            "CSI300,000001,SHE,XSHE,Ping An Bank,2020-01-01,2023-06-30,0.014,000001.SHE,manual_csv,CN,CNY,Financials,Banks\n"
        )

    def _legacy_sp500_csv(self) -> str:
        return (
            "universe_code,ticker,exchange,provider_symbol,valid_from,valid_to,company_name,source\n"
            "SP500,AAPL,US,AAPL.US,2020-01-01,,Apple Inc,eodhd_csv\n"
        )

    def _report(self, *, ready: bool = False) -> ReadinessReport:
        return ReadinessReport(
            universe="SP500",
            start=date(2022, 1, 1),
            end=date(2022, 1, 3),
            ready=ready,
            status="READY" if ready else "NOT_READY",
            checks=[
                ReadinessCheck(
                    code="coverage_snapshots",
                    label="Coverage snapshots",
                    status="OK" if ready else "ERROR",
                    message="Coverage OK" if ready else "Coverage non validée: missing coverage snapshot for 2022-01-01.",
                    suggested_commands=["python manage.py sync_sp500_historical_memberships --coverage-start 2022-01-01 --coverage-end 2022-01-03 --apply"],
                )
            ],
            suggested_actions=[
                ReadinessAction(
                    code="sync_sp500_historical_memberships",
                    label="Synchroniser memberships",
                    command="python manage.py sync_sp500_historical_memberships --coverage-start {start} --coverage-end {end} --apply",
                )
            ],
        )

    def test_trigger_page_renders_dynamic_universe_section_and_readiness_action(self):
        response = self.client.get(reverse("trigger_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Parcours S&amp;P500 historique", body)
        self.assertIn("Parcours CSI300 historique via CSV", body)
        self.assertIn("Créer / mapper les actions depuis les memberships CSV", body)
        self.assertIn("GM_market est supporté avec le benchmark CSI 300 / 000300.SHG", body)
        self.assertIn("GM_sector reste non supporté", body)
        self.assertIn("Vérifier si l’univers est prêt", body)
        self.assertIn('id="du-universe-selector"', body)
        self.assertIn('value="SP500"', body)
        self.assertIn('value="CSI300"', body)
        self.assertIn("Initialiser le référentiel de base", body)
        self.assertIn("Créer les actions historiques manquantes", body)
        self.assertIn("Récupérer la composition historique", body)
        self.assertIn("Télécharger les prix des actions", body)
        self.assertIn("Préparer les ETFs de marché et de secteur", body)
        self.assertIn("du_readiness", body)

    @patch("core.views.check_dynamic_universe_readiness")
    def test_trigger_readiness_calls_service_and_renders_not_ready(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=False)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_readiness",
                "du_universe": "SP500",
                "du_start": "2022-01-01",
                "du_end": "2022-01-03",
                "du_require_gm_market": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        readiness_mock.assert_called_once()
        self.assertTrue(readiness_mock.call_args.kwargs["require_gm_market"])
        body = response.content.decode()
        self.assertIn("Résultat de la vérification : Données à compléter", body)
        self.assertIn("La période demandée", body)
        self.assertNotIn("missing coverage snapshot", body)

    @patch("core.services.dynamic_universe_ohlc_prepare.prepare_dynamic_universe_ohlc")
    @patch("core.services.universe_eodhd_sync.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.sp500_symbol_bootstrap.bootstrap_sp500_symbols_from_eodhd")
    def test_trigger_readiness_does_not_call_provider_helpers(self, bootstrap_mock, sync_mock, prepare_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_readiness",
                "du_universe": "SP500",
                "du_start": "2022-01-01",
                "du_end": "2022-01-03",
            },
        )

        self.assertEqual(response.status_code, 200)
        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        prepare_mock.assert_not_called()

    @patch("core.views.call_command")
    def test_trigger_init_reference_data_calls_management_command(self, call_command_mock):
        call_command_mock.return_value = None

        response = self.client.post(reverse("trigger_page"), {"action": "du_init_reference_data", "du_universe": "SP500"}, follow=True)

        self.assertEqual(response.status_code, 200)
        call_command_mock.assert_called_once()
        self.assertEqual(call_command_mock.call_args.args[0], "init_reference_data")
        messages = list(response.context["messages"])
        self.assertTrue(any("Référentiel initialisé" in str(message) for message in messages))

    def test_main_dynamic_universe_actions_use_business_wording(self):
        response = self.client.get(reverse("trigger_page"))

        body = response.content.decode()
        main_section = body.split("Actions avancées / techniques", 1)[0]
        self.assertIn("Modifie la base", main_section)
        self.assertIn("Utilise EODHD", main_section)
        self.assertIn("Peut prendre du temps", main_section)
        self.assertIn("Télécharger les prix", main_section)
        for technical in (
            "bootstrap_sp500_symbols_from_eodhd",
            "sync_benchmark_etfs",
            "DailyBars",
            "OHLC",
            "Appelle provider",
            "Path serveur requis",
            "python manage.py",
        ):
            self.assertNotIn(technical, main_section)
        self.assertNotIn("Actions avancées / techniques", body)
        self.assertNotIn("Import univers historique par CSV", body)
        self.assertNotIn("du_map_membership_symbols", body)

    def test_non_staff_does_not_see_historical_universe_csv_import_block(self):
        response = self.client.get(reverse("trigger_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Import univers historique par CSV", body)
        self.assertNotIn("du_map_membership_symbols", body)
        self.assertNotIn("du_import_csv_file", body)

    def test_staff_sees_historical_universe_csv_import_block(self):
        self._login_staff()

        response = self.client.get(reverse("trigger_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Import univers historique par CSV", body)
        self.assertIn("Créer / mapper les actions depuis les memberships CSV", body)
        self.assertIn("du_map_membership_symbols", body)
        self.assertIn("Réservé aux administrateurs/staff", body)
        self.assertIn('enctype="multipart/form-data"', body)
        self.assertIn("du_import_csv_file", body)
        self.assertIn("Dry-run / Vérifier", body)
        self.assertIn("Importer réellement", body)

    def test_trigger_get_invalid_universe_shows_warning_without_trusting_query_string(self):
        response = self.client.get(reverse("trigger_page"), {"universe": "sp500"})

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Univers ignoré: sp500", body)
        self.assertIn('value="SP500" selected', body)

    def test_trigger_dynamic_universe_lists_are_filtered_by_selected_universe(self):
        sp500 = Scenario.objects.create(name="Dynamic SP500", universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)
        csi300 = Scenario.objects.create(name="Dynamic CSI300", universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        Backtest.objects.create(name="BT SP500", scenario=sp500)
        Backtest.objects.create(name="BT CSI300", scenario=csi300)

        response = self.client.get(reverse("trigger_page"), {"universe": "CSI300"})

        self.assertEqual(response.status_code, 200)
        dynamic_section = response.content.decode().split("Collecte (Fetch Daily Bars)", 1)[0]
        self.assertIn("Dynamic CSI300", dynamic_section)
        self.assertIn("BT CSI300", dynamic_section)
        self.assertNotIn("Dynamic SP500", dynamic_section)
        self.assertNotIn("BT SP500", dynamic_section)

    def test_trigger_readiness_missing_params_shows_clean_error(self):
        response = self.client.post(reverse("trigger_page"), {"action": "du_readiness"})

        self.assertEqual(response.status_code, 200)
        messages = list(response.context["messages"])
        self.assertTrue(any("Univers est requis" in str(message) for message in messages))

    @patch("core.views.check_dynamic_universe_readiness")
    def test_trigger_readiness_rejects_lowercase_universe_without_service_call(self, readiness_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_readiness",
                "du_universe": "sp500",
                "du_start": "2022-01-01",
                "du_end": "2022-01-03",
            },
        )

        self.assertEqual(response.status_code, 200)
        readiness_mock.assert_not_called()
        messages = list(response.context["messages"])
        self.assertTrue(any("Univers non supporté: sp500" in str(message) for message in messages))

    @patch("core.views.import_universe_memberships_from_csv")
    def test_non_staff_upload_import_is_rejected_without_writes(self, import_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "CSI300",
                "du_import_csv_file": self._uploaded_csv(self._csi300_csv(), name="csi300.csv"),
                "du_import_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        import_mock.assert_not_called()
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("réservé aux administrateurs/staff" in str(message) for message in messages))

    @patch("core.views.import_universe_memberships_from_csv")
    def test_non_staff_server_path_import_is_rejected_without_writes(self, import_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "CSI300",
                "du_import_file": "/tmp/csi300.csv",
                "du_import_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        import_mock.assert_not_called()
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("réservé aux administrateurs/staff" in str(message) for message in messages))

    def test_staff_import_memberships_missing_file_shows_clean_error(self):
        self._login_staff()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "CSI300",
                "du_import_start": "2022-01-01",
                "du_import_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        messages = list(response.context["messages"])
        self.assertTrue(any("fichier CSV à uploader" in str(message) for message in messages))

    def test_staff_upload_import_dry_run_shows_summary_without_writes(self):
        self._login_staff()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "CSI300",
                "du_import_universe_name": "CSI 300",
                "du_import_expected_member_count": "1",
                "du_import_csv_file": self._uploaded_csv(self._csi300_csv(), name="csi300.csv"),
                "du_import_mode": "dry_run",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("Import univers historique CSV (dry-run)" in str(message) for message in messages))
        self.assertTrue(any("lignes lues=2" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    @patch("core.views.call_command")
    def test_staff_upload_import_apply_creates_csi300_without_provider_or_jobs(self, call_command_mock, launch_mock):
        self._login_staff()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "CSI300",
                "du_import_universe_name": "CSI 300",
                "du_import_expected_member_count": "1",
                "du_import_csv_file": self._uploaded_csv(self._csi300_csv(), name="csi300.csv"),
                "du_import_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        call_command_mock.assert_not_called()
        launch_mock.assert_not_called()
        universe = UniverseDefinition.objects.get(code="CSI300")
        self.assertEqual(universe.name, "CSI 300")
        self.assertEqual(UniverseMembership.objects.filter(universe=universe).count(), 2)
        self.assertTrue(UniverseMembership.objects.filter(universe=universe, ticker="000001", exchange="SHE").exists())
        messages = list(response.context["messages"])
        self.assertTrue(any("Import univers historique CSV (apply)" in str(message) for message in messages))

    def test_staff_upload_import_rejects_universe_code_mismatch(self):
        self._login_staff()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "SP500",
                "du_import_csv_file": self._uploaded_csv(self._csi300_csv(), name="csi300.csv"),
                "du_import_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("does not match requested universe_code=SP500" in str(message) for message in messages))

    def test_staff_upload_import_invalid_csv_shows_clean_error(self):
        self._login_staff()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "CSI300",
                "du_import_csv_file": self._uploaded_csv("not,a,valid\n1,2,3\n", name="csi300.csv"),
                "du_import_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(UniverseDefinition.objects.count(), 0)
        self.assertEqual(UniverseMembership.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("CSV is missing required columns" in str(message) for message in messages))

    def test_staff_upload_import_accepts_sp500_legacy_csv(self):
        self._login_staff()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_import_memberships",
                "du_import_universe": "SP500",
                "du_import_universe_name": "S&P 500",
                "du_import_expected_member_count": "1",
                "du_import_csv_file": self._uploaded_csv(self._legacy_sp500_csv(), name="sp500.csv"),
                "du_import_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        universe = UniverseDefinition.objects.get(code="SP500")
        self.assertEqual(UniverseMembership.objects.filter(universe=universe, ticker="AAPL", exchange="US").count(), 1)
        messages = list(response.context["messages"])
        self.assertTrue(any("Import univers historique CSV (apply)" in str(message) for message in messages))

    def _partial_csi300_membership(self):
        universe = UniverseDefinition.objects.create(code="CSI300", name="CSI 300", source="manual_csv", active=True)
        UniverseMembership.objects.create(
            universe=universe,
            ticker="000001",
            exchange="SHE",
            provider_symbol="000001.SHE",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            source="manual_csv",
            source_payload={"company_name": "Ping An Bank", "row": {"country": "CN", "currency": "CNY"}},
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="manual_csv",
            source_name="manual_csv",
            period_start=date(2020, 1, 1),
            period_end=date(2020, 1, 1),
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=0,
            unmapped_member_count=1,
            status=UniverseCoverageStatus.PARTIAL,
        )
        UniverseCoverageSnapshot.objects.create(
            universe=universe,
            import_batch=batch,
            coverage_date=date(2020, 1, 1),
            expected_member_count=1,
            actual_member_count=1,
            mapped_member_count=0,
            unmapped_member_count=1,
            status=UniverseCoverageStatus.PARTIAL,
        )
        return universe

    @patch("core.views.launch_processing_job")
    def test_non_staff_mapping_membership_symbols_is_rejected(self, launch_mock):
        self._partial_csi300_membership()

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_map_membership_symbols",
                "du_symbol_mapping_universe": "CSI300",
                "du_symbol_mapping_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(Symbol.objects.filter(ticker="000001", exchange="SHE").count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("réservé aux administrateurs/staff" in str(message) for message in messages))

    @patch("core.tasks.ensure_universe_membership_symbols")
    @patch("core.views.launch_processing_job")
    @patch("core.views.call_command")
    def test_staff_mapping_membership_symbols_launches_async_job_without_provider_or_inline_mapping(
        self,
        call_command_mock,
        launch_mock,
        ensure_symbols_mock,
    ):
        self._login_staff()
        universe = self._partial_csi300_membership()
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)
        launch_mock.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_map_membership_symbols",
                "du_symbol_mapping_universe": "CSI300",
                "du_symbol_mapping_mode": "apply",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        call_command_mock.assert_not_called()
        ensure_symbols_mock.assert_not_called()
        self.assertTrue(launch_mock.called)
        self.assertEqual(launch_mock.call_args.kwargs["task"], map_universe_membership_symbols_job_task)
        self.assertEqual(launch_mock.call_args.kwargs["job_type"], ProcessingJob.JobType.FETCH_BARS)
        self.assertEqual(launch_mock.call_args.kwargs["task_kwargs"]["universe_code"], "CSI300")
        self.assertTrue(launch_mock.call_args.kwargs["task_kwargs"]["create_missing"])
        self.assertFalse(launch_mock.call_args.kwargs["task_kwargs"]["dry_run"])
        self.assertFalse(Symbol.objects.filter(ticker="000001", exchange="SHE").exists())
        self.assertIsNone(UniverseMembership.objects.get(universe=universe, ticker="000001").symbol_id)
        messages = list(response.context["messages"])
        self.assertTrue(any("lancé en arrière-plan" in str(message) for message in messages))

    @patch("core.tasks.ensure_universe_membership_symbols")
    def test_mapping_membership_symbols_task_marks_done(self, ensure_symbols_mock):
        ensure_symbols_mock.return_value = UniverseSymbolMappingReport(
            universe_code="CSI300",
            memberships_total=1,
            created_symbols=1,
            coverage_batches_updated=1,
            coverage_snapshots_updated=1,
        )
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)

        message = map_universe_membership_symbols_job_task.apply(kwargs={
            "job_id": job.id,
            "universe_code": "CSI300",
            "create_missing": True,
            "dry_run": False,
        }).get(propagate=True)

        job.refresh_from_db()
        ensure_symbols_mock.assert_called_once_with("CSI300", create_missing=True, dry_run=False)
        self.assertEqual(job.status, ProcessingJob.Status.DONE)
        self.assertIn("Mapping symbols univers historique (apply)", message)
        self.assertEqual(job.error, "")

    @patch("core.tasks.ensure_universe_membership_symbols")
    def test_mapping_membership_symbols_task_marks_failed_on_mapping_error(self, ensure_symbols_mock):
        ensure_symbols_mock.side_effect = UniverseSymbolMappingError("UniverseDefinition CSI300 is missing or inactive.")
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)

        message = map_universe_membership_symbols_job_task.apply(kwargs={
            "job_id": job.id,
            "universe_code": "CSI300",
            "create_missing": True,
            "dry_run": False,
        }).get(propagate=True)

        job.refresh_from_db()
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("UniverseDefinition CSI300", message)
        self.assertIn("UniverseDefinition CSI300", job.error)

    @patch("core.views.check_dynamic_universe_readiness")
    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_launches_existing_job(self, launch_mock, readiness_mock):
        readiness_mock.return_value = self._report(ready=True)
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)
        launch_mock.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "SP500",
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(launch_mock.called)
        self.assertEqual(launch_mock.call_args.kwargs["job_type"], ProcessingJob.JobType.FETCH_BARS)
        self.assertEqual(launch_mock.call_args.kwargs["task_kwargs"]["start_date"], "2022-01-01")

    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_requires_explicit_universe_without_sp500_fallback(self, launch_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("Univers est requis" in str(message) for message in messages))

    @patch("core.views.check_dynamic_universe_readiness")
    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_warning_requires_confirmation_without_job(self, launch_mock, readiness_mock):
        self._login_staff()
        scenario = Scenario.objects.create(
            name="CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        readiness_mock.return_value = ReadinessReport(
            universe="CSI300",
            start=date(2018, 1, 1),
            end=date(2018, 1, 2),
            ready=True,
            status="READY_WITH_WARNINGS",
            checks=[
                ReadinessCheck(
                    code="coverage_snapshots",
                    label="Coverage",
                    status="WARNING",
                    message="La composition historique CSI300 est incomplète sur 2 dates.",
                    details={"partial_snapshot_count": 2},
                )
            ],
        )

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "CSI300",
                "scenario_id": str(scenario.id),
                "du_ohlc_start": "2018-01-01",
                "du_ohlc_end": "2018-01-02",
            },
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        body = response.content.decode()
        self.assertIn("Je souhaite continuer avec les données disponibles", body)

    @patch("core.views.check_dynamic_universe_readiness")
    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_warning_with_confirmation_dispatches_job(self, launch_mock, readiness_mock):
        self._login_staff()
        scenario = Scenario.objects.create(
            name="CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        report = ReadinessReport(
            universe="CSI300",
            start=date(2018, 1, 1),
            end=date(2018, 1, 2),
            ready=True,
            status="READY_WITH_WARNINGS",
            checks=[
                ReadinessCheck(
                    code="coverage_snapshots",
                    label="Coverage",
                    status="WARNING",
                    message="La composition historique CSI300 est incomplète sur 2 dates.",
                    details={"partial_snapshot_count": 2},
                )
            ],
        )
        readiness_mock.return_value = report
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)
        launch_mock.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "CSI300",
                "scenario_id": str(scenario.id),
                "du_ohlc_start": "2018-01-01",
                "du_ohlc_end": "2018-01-02",
                "du_readiness_warning_ack": "1",
                "du_readiness_warning_hash": readiness_confirmation_hash(report),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(launch_mock.called)
        kwargs = launch_mock.call_args.kwargs["task_kwargs"]
        self.assertEqual(kwargs["universe_code"], "CSI300")
        self.assertTrue(kwargs["allow_partial_coverage"])
        self.assertEqual(kwargs["provider"], "eodhd")

    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_rejects_universe_scenario_mismatch(self, launch_mock):
        scenario = Scenario.objects.create(
            name="CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "SP500",
                "scenario_id": str(scenario.id),
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("ne correspond pas au scénario/backtest" in str(message) for message in messages))

    @patch("core.views.check_dynamic_universe_readiness")
    def test_trigger_readiness_allows_csi300_gm_market(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=True)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_readiness",
                "du_universe": "CSI300",
                "du_start": "2022-01-01",
                "du_end": "2022-01-03",
                "du_require_gm_market": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        readiness_mock.assert_called_once()
        self.assertTrue(readiness_mock.call_args.kwargs["require_gm_market"])
        self.assertFalse(readiness_mock.call_args.kwargs["require_gm_sector"])

    @patch("core.views.check_dynamic_universe_readiness")
    def test_trigger_readiness_rejects_csi300_gm_sector(self, readiness_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_readiness",
                "du_universe": "CSI300",
                "du_start": "2022-01-01",
                "du_end": "2022-01-03",
                "du_require_gm_sector": "on",
            },
        )

        self.assertEqual(response.status_code, 200)
        readiness_mock.assert_not_called()
        messages = list(response.context["messages"])
        self.assertTrue(any("GM secteur CSI300 n'est pas encore supporté" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    def test_trigger_backtest_run_rejects_invalid_capital_without_processing_job(self, launch_mock):
        scenario = Scenario.objects.create(name="Static trigger capital", universe_mode=Scenario.UniverseMode.STATIC_TICKERS)
        backtest = Backtest.objects.create(
            name="BT invalid capital",
            scenario=scenario,
            status=Backtest.Status.DONE,
            capital_total=Decimal("50"),
            capital_per_ticker=Decimal("100"),
        )

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "bt_run",
                "backtest_id": str(backtest.id),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        backtest.refresh_from_db()
        self.assertEqual(backtest.status, Backtest.Status.DONE)
        messages = list(response.context["messages"])
        self.assertTrue(any("capital total" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    def test_trigger_backtest_run_uses_common_currency_validating_task(self, launch_mock):
        scenario = Scenario.objects.create(
            name="CSI300 Trigger guarded",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        backtest = Backtest.objects.create(
            name="BT CSI300 Trigger guarded",
            scenario=scenario,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
        )
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.PENDING,
            backtest=backtest,
            scenario=scenario,
        )
        launch_mock.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

        response = self.client.post(
            reverse("trigger_page"),
            {"action": "bt_run", "backtest_id": str(backtest.id)},
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_called_once()
        self.assertIs(launch_mock.call_args.kwargs["task"], run_backtest_job_task)

    def test_trigger_common_task_blocks_usd_csi300_market_benchmark(self):
        start = date(2024, 1, 1)
        end = date(2024, 1, 3)
        scenario = Scenario.objects.create(
            name="CSI300 Trigger GM market guarded",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        member = Symbol.objects.create(
            ticker="600519",
            exchange="SHG",
            currency="CNY",
            active=True,
        )
        benchmark = Symbol.objects.create(
            ticker="000300",
            exchange="SHG",
            currency="USD",
            active=True,
        )
        universe = UniverseDefinition.objects.create(
            code="CSI300",
            name="CSI 300",
            source="manual_csv",
            active=True,
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=member,
            ticker=member.ticker,
            exchange=member.exchange,
            provider_symbol="600519.SHG",
            valid_from=start,
            valid_to=None,
            source="manual_csv",
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="manual_csv",
            source_name="manual_csv",
            period_start=start,
            period_end=end,
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=1,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
        )
        for offset in range(3):
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=date.fromordinal(start.toordinal() + offset),
                expected_member_count=1,
                actual_member_count=1,
                mapped_member_count=1,
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
        previous_results = {"meta": {"effective_currency": "CNY"}, "legacy": True}
        backtest = Backtest.objects.create(
            name="BT CSI300 Trigger GM market guarded",
            scenario=scenario,
            start_date=start,
            end_date=end,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            signal_lines=[
                {"buy": ["A1"], "sell": ["B1"], "buy_market_gm_market": "GM_POS"}
            ],
            results=previous_results,
        )
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.PENDING,
            backtest=backtest,
            scenario=scenario,
            created_by=self.user,
        )

        with patch("core.tasks.determine_backtest_result_mode") as preflight_mock:
            with patch("core.services.backtesting.prep.prepare_backtest_data") as prep_mock:
                with patch("core.services.backtesting.engine.run_backtest") as engine_mock:
                    with self.assertRaisesMessage(ResolvedCurrencyValidationError, "000300.SHG"):
                        run_backtest_job_task.run(
                            backtest_id=backtest.id,
                            user_id=self.user.id,
                            job_id=job.id,
                        )

        preflight_mock.assert_not_called()
        prep_mock.assert_not_called()
        engine_mock.assert_not_called()
        backtest.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(backtest.status, Backtest.Status.FAILED)
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertIn("000300.SHG", backtest.error_message)
        self.assertIn("devise attendue CNY", backtest.error_message)
        self.assertIn("devise trouvée USD", backtest.error_message)
        self.assertIn("000300.SHG", job.error)
        self.assertEqual(backtest.results, previous_results)
        self.assertEqual(benchmark.currency, "USD")

    def test_trigger_common_task_marks_backtest_and_job_failed_before_engine_for_unsupported_history(self):
        scenario = Scenario.objects.create(
            name="CSI300 Trigger pre-cutoff",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        previous_results = {"legacy": True}
        backtest = Backtest.objects.create(
            name="BT CSI300 Trigger pre-cutoff",
            scenario=scenario,
            start_date=date(2023, 1, 2),
            end_date=date(2023, 1, 3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            results=previous_results,
        )
        job = ProcessingJob.objects.create(
            job_type=ProcessingJob.JobType.RUN_BACKTEST,
            status=ProcessingJob.Status.PENDING,
            backtest=backtest,
            scenario=scenario,
            created_by=self.user,
        )

        with patch("core.tasks.determine_backtest_result_mode") as preflight_mock:
            with patch("core.services.backtesting.prep.prepare_backtest_data") as prep_mock:
                with patch("core.services.backtesting.engine.run_backtest") as engine_mock:
                    with self.assertRaisesMessage(ValueError, CSI300_SUPPORTED_HISTORY_MESSAGE):
                        run_backtest_job_task.run(
                            backtest_id=backtest.id,
                            user_id=self.user.id,
                            job_id=job.id,
                        )

        preflight_mock.assert_not_called()
        prep_mock.assert_not_called()
        engine_mock.assert_not_called()
        backtest.refresh_from_db()
        job.refresh_from_db()
        self.assertEqual(backtest.status, Backtest.Status.FAILED)
        self.assertEqual(job.status, ProcessingJob.Status.FAILED)
        self.assertEqual(backtest.results, previous_results)
        self.assertIn("3 janvier 2023", backtest.error_message)
        self.assertIn("3 janvier 2023", job.error)

    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_rejects_backtest_universe_mismatch(self, launch_mock):
        scenario = Scenario.objects.create(
            name="SP500",
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
        )
        backtest = Backtest.objects.create(name="BT SP500", scenario=scenario)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "CSI300",
                "backtest_id": str(backtest.id),
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("ne correspond pas au scénario/backtest" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_rejects_static_scenario_without_processing_job(self, launch_mock):
        scenario = Scenario.objects.create(
            name="Static",
            universe_mode=Scenario.UniverseMode.STATIC_TICKERS,
        )

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "SP500",
                "scenario_id": str(scenario.id),
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("doit utiliser un univers historique dynamique" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_rejects_lowercase_universe_without_processing_job(self, launch_mock):
        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "sp500",
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        messages = list(response.context["messages"])
        self.assertTrue(any("Univers non supporté: sp500" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    def test_non_staff_cannot_trigger_csi300_ohlc_prepare(self, launch_mock):
        scenario = Scenario.objects.create(
            name="CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "CSI300",
                "scenario_id": str(scenario.id),
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        messages = list(response.context["messages"])
        self.assertTrue(any("Préparation OHLC CSI300 via EODHD réservée" in str(message) for message in messages))

    @patch("core.views.check_dynamic_universe_readiness")
    @patch("core.views.launch_processing_job")
    def test_staff_can_trigger_csi300_ohlc_prepare(self, launch_mock, readiness_mock):
        readiness_mock.return_value = ReadinessReport(
            universe="CSI300",
            start=date(2022, 1, 1),
            end=date(2022, 1, 3),
            ready=True,
            status="READY",
            checks=[ReadinessCheck(code="coverage_snapshots", label="Coverage", status="OK", message="OK")],
        )
        self._login_staff()
        scenario = Scenario.objects.create(
            name="CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)
        launch_mock.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

        response = self.client.post(
            reverse("trigger_page"),
            {
                "action": "du_download_prices",
                "du_universe": "CSI300",
                "scenario_id": str(scenario.id),
                "du_ohlc_start": "2022-01-01",
                "du_ohlc_end": "2022-01-03",
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(launch_mock.called)
        kwargs = launch_mock.call_args.kwargs["task_kwargs"]
        self.assertEqual(kwargs["universe_code"], "CSI300")
        self.assertEqual(kwargs["provider"], "eodhd")
