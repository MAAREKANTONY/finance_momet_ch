from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client, TestCase
from django.urls import reverse

from core.job_launch import JobLaunchOutcome
from core.models import ProcessingJob, Scenario, UniverseDefinition, UniverseMembership
from core.services.dynamic_universe_readiness import ReadinessAction, ReadinessCheck, ReadinessReport


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
        self.assertIn("Préparer le S&amp;P500 historique", body)
        self.assertIn("Vérifier si le S&amp;P500 est prêt", body)
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

        response = self.client.post(reverse("trigger_page"), {"action": "du_init_reference_data"}, follow=True)

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

    def test_non_staff_does_not_see_historical_universe_csv_import_block(self):
        response = self.client.get(reverse("trigger_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Import univers historique par CSV", body)
        self.assertNotIn("du_import_csv_file", body)

    def test_staff_sees_historical_universe_csv_import_block(self):
        self._login_staff()

        response = self.client.get(reverse("trigger_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Import univers historique par CSV", body)
        self.assertIn("Réservé aux administrateurs/staff", body)
        self.assertIn('enctype="multipart/form-data"', body)
        self.assertIn("du_import_csv_file", body)
        self.assertIn("Dry-run / Vérifier", body)
        self.assertIn("Importer réellement", body)

    def test_trigger_readiness_missing_params_shows_clean_error(self):
        response = self.client.post(reverse("trigger_page"), {"action": "du_readiness"})

        self.assertEqual(response.status_code, 200)
        messages = list(response.context["messages"])
        self.assertTrue(any("Start est requis" in str(message) for message in messages))

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

    @patch("core.views.launch_processing_job")
    def test_trigger_prepare_ohlc_launches_existing_job(self, launch_mock):
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.FETCH_BARS, status=ProcessingJob.Status.PENDING)
        launch_mock.return_value = JobLaunchOutcome(job=job, dispatch_error=None)

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
        self.assertTrue(launch_mock.called)
        self.assertEqual(launch_mock.call_args.kwargs["job_type"], ProcessingJob.JobType.FETCH_BARS)
        self.assertEqual(launch_mock.call_args.kwargs["task_kwargs"]["start_date"], "2022-01-01")

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

    @patch("core.views.launch_processing_job")
    def test_staff_can_trigger_csi300_ohlc_prepare(self, launch_mock):
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
