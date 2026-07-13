from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse

from core.models import (
    Backtest,
    DailyBar,
    ProcessingJob,
    Scenario,
    Symbol,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
)
from core.services.dynamic_universe_readiness import BACKTEST_READINESS_ACK_SETTINGS_KEY, ReadinessAction, ReadinessCheck, ReadinessReport, readiness_confirmation_hash


class DynamicUniverseBacktestWizardTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="du-wizard-user", password="secret123")
        self.client = Client()
        self.client.force_login(self.user)
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", sector="Technology", active=True)

    def _scenario(self, *, dynamic: bool = True) -> Scenario:
        scenario = Scenario.objects.create(
            name="Scenario DU" if dynamic else "Scenario static",
            universe_mode=(
                Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
                if dynamic
                else Scenario.UniverseMode.STATIC_TICKERS
            ),
        )
        scenario.symbols.add(self.symbol)
        return scenario

    def _backtest(self, *, scenario=None, start=date(2024, 1, 1), end=date(2024, 1, 3), signal_lines=None) -> Backtest:
        return Backtest.objects.create(
            name="BT DU Wizard",
            scenario=scenario or self._scenario(),
            start_date=start,
            end_date=end,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            include_all_tickers=True,
            signal_lines=signal_lines if signal_lines is not None else [{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[{"ticker": self.symbol.ticker, "exchange": self.symbol.exchange, "sector": self.symbol.sector}],
            status=Backtest.Status.PENDING,
        )

    def _report(self, *, ready: bool, checks=None) -> ReadinessReport:
        return ReadinessReport(
            universe="SP500",
            start=date(2024, 1, 1),
            end=date(2024, 1, 3),
            ready=ready,
            status="READY" if ready else "NOT_READY",
            checks=checks or [
                ReadinessCheck(
                    code="coverage_snapshots",
                    label="Coverage snapshots",
                    status="OK" if ready else "ERROR",
                    message="Coverage OK" if ready else "Coverage non validée: missing coverage snapshot for 2024-01-01.",
                    suggested_actions=[] if ready else [
                        ReadinessAction(
                            code="sync_sp500_historical_memberships",
                            label="Synchroniser memberships",
                            command="python manage.py sync_sp500_historical_memberships --coverage-start 2024-01-01 --coverage-end 2024-01-03 --apply",
                        )
                    ],
                )
            ],
            suggested_actions=[] if ready else [
                ReadinessAction(
                    code="sync_sp500_historical_memberships",
                    label="Synchroniser memberships",
                    command="python manage.py sync_sp500_historical_memberships --coverage-start 2024-01-01 --coverage-end 2024-01-03 --apply",
                )
            ],
        )

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_absent_for_static_tickers(self, readiness_mock):
        bt = self._backtest(scenario=self._scenario(dynamic=False))

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Préparation du S&amp;P500 historique", body)
        readiness_mock.assert_not_called()

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_present_for_dynamic_sp500(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=False)
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Préparation du S&amp;P500 historique", body)
        self.assertIn("Données à compléter", body)
        self.assertIn("Trigger", body)
        readiness_mock.assert_called_once()

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_missing_dates_shows_clean_message(self, readiness_mock):
        bt = self._backtest(start=None, end=None)

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Renseignez une date de début et une date de fin pour vérifier la readiness", body)
        readiness_mock.assert_not_called()

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_not_ready_status_is_rendered(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=False)
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "Certaines données doivent être préparées avant de lancer le backtest")

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_ready_status_is_rendered(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=True)
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "Prêt")
        self.assertContains(response, "Prêt pour le backtest")

    def test_missing_universe_definition_shows_init_reference_data_action(self):
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Le référentiel de base doit être initialisé", body)
        self.assertIn("Initialiser le référentiel", body)
        self.assertIn(reverse("trigger_page"), body)

    def test_missing_coverage_message_is_visible(self):
        universe = UniverseDefinition.objects.create(code="SP500", name="S&P 500", source="test", active=True)
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.symbol,
            ticker=self.symbol.ticker,
            exchange=self.symbol.exchange,
            provider_symbol="AAA.US",
            valid_from=date(2024, 1, 1),
            valid_to=date(2024, 1, 3),
            source="test",
        )
        UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="test",
            period_start=date(2024, 1, 1),
            period_end=date(2024, 1, 3),
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=1,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
        )
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "La période demandée")
        self.assertContains(response, "2024-01-01")

    @patch("core.views.check_dynamic_universe_readiness")
    def test_gm_market_check_is_requested_when_backtest_signal_requires_it(self, readiness_mock):
        readiness_mock.return_value = self._report(
            ready=False,
            checks=[
                ReadinessCheck(
                    code="gm_market_daily_bars",
                    label="DailyBars GM_market",
                    status="ERROR",
                    message="Benchmarks GM_market manquants.",
                )
            ],
        )
        bt = self._backtest(signal_lines=[{"buy": ["A1"], "sell": ["B1"], "buy_market_gm_market": "GM_POS"}])

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "Prix du filtre marché")
        self.assertTrue(readiness_mock.call_args.kwargs["require_gm_market"])

    @patch("core.views.check_dynamic_universe_readiness")
    def test_gm_sector_check_is_requested_when_backtest_signal_requires_it(self, readiness_mock):
        readiness_mock.return_value = self._report(
            ready=False,
            checks=[
                ReadinessCheck(
                    code="gm_sector_daily_bars",
                    label="DailyBars GM_sector",
                    status="ERROR",
                    message="ETFs sectoriels manquants.",
                )
            ],
        )
        bt = self._backtest(signal_lines=[{"buy": ["A1"], "sell": ["B1"], "buy_market_gm_sector": "GM_NEG"}])

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "Prix du filtre secteur")
        self.assertTrue(readiness_mock.call_args.kwargs["require_gm_sector"])


    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_uses_business_labels_and_hides_commands(self, readiness_mock):
        readiness_mock.return_value = ReadinessReport(
            universe="SP500",
            start=date(2024, 1, 1),
            end=date(2024, 1, 3),
            ready=True,
            status="READY_WITH_WARNINGS",
            checks=[
                ReadinessCheck(
                    code="universe_definition",
                    label="Référentiel SP500",
                    status="OK",
                    message="Référentiel SP500 actif.",
                ),
                ReadinessCheck(
                    code="memberships",
                    label="Memberships historiques",
                    status="OK",
                    message="2 memberships recouvrent la période.",
                ),
                ReadinessCheck(
                    code="import_batch",
                    label="Batch d'import validé",
                    status="OK",
                    message="UniverseImportBatch VALIDATED couvre la période.",
                ),
                ReadinessCheck(
                    code="coverage_snapshots",
                    label="Coverage snapshots",
                    status="OK",
                    message="3 coverage snapshots validés.",
                ),
                ReadinessCheck(
                    code="historical_symbols",
                    label="Symbols historiques",
                    status="OK",
                    message="2 memberships sont mappés vers des Symbols.",
                ),
                ReadinessCheck(
                    code="member_daily_bars",
                    label="DailyBars membres",
                    status="WARNING",
                    message="Prix manquants.",
                    details={"expected_symbols": 606, "ready_symbols": 603, "missing_symbols": 3, "missing_examples": ["AGN", "BF.B", "BRK.B"]},
                    suggested_actions=[ReadinessAction(code="prepare_dynamic_universe_ohlc", label="tech", command="python manage.py hidden")],
                ),
                ReadinessCheck(code="gm_market_daily_bars", label="DailyBars GM_market", status="SKIPPED", message="GM_market non demandé."),
                ReadinessCheck(code="gm_sector_daily_bars", label="DailyBars GM_sector", status="SKIPPED", message="GM_sector non demandé."),
            ],
            suggested_actions=[ReadinessAction(code="prepare_dynamic_universe_ohlc", label="tech", command="python manage.py hidden")],
        )
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        body = response.content.decode()
        for label in (
            "Référentiel de base",
            "Composition historique",
            "Import de composition validé",
            "Couverture de la période",
            "Actions historiques",
            "Prix des actions",
            "Prix du filtre marché",
            "Prix du filtre secteur",
        ):
            self.assertIn(label, body)
        self.assertIn("Actions recommandées", body)
        self.assertIn("Prêt avec avertissement", body)
        self.assertIn("603 actions sur 606 ont des prix disponibles", body)
        self.assertIn("AGN, BF.B, BRK.B", body)
        self.assertIn("Télécharger les prix des actions", body)
        for technical in ("Coverage snapshots", "DailyBars", "Symbols", "GM_market non demandé", "GM_sector non demandé", "python manage.py"):
            self.assertNotIn(technical, body)

    def _warning_report(self, *, start=date(2018, 1, 1), end=date(2018, 1, 2)) -> ReadinessReport:
        return ReadinessReport(
            universe="CSI300",
            start=start,
            end=end,
            ready=True,
            status="READY_WITH_WARNINGS",
            checks=[
                ReadinessCheck(
                    code="coverage_snapshots",
                    label="Coverage snapshots",
                    status="WARNING",
                    message="La composition historique CSI300 est incomplète sur 2 dates.",
                    details={
                        "partial_snapshot_count": 2,
                        "minimum_actual_member_count": 299,
                        "expected_member_count": 300,
                        "minimum_coverage_ratio_percent": 99.6667,
                    },
                )
            ],
        )

    def _csi300_backtest(self, *, start=date(2018, 1, 1), end=date(2018, 1, 2)) -> Backtest:
        scenario = Scenario.objects.create(
            name="CSI300 Scenario",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
        )
        scenario.symbols.add(self.symbol)
        return self._backtest(scenario=scenario, start=start, end=end)

    @patch("core.views.check_dynamic_universe_readiness")
    def test_csi300_warning_is_visible_with_confirmation_checkbox(self, readiness_mock):
        readiness_mock.return_value = self._warning_report()
        bt = self._csi300_backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "Prêt avec avertissement")
        self.assertContains(response, "Composition historique incomplète")
        self.assertContains(response, "Je souhaite continuer avec les données disponibles")

    @patch("core.views.launch_processing_job")
    def test_backtest_run_rejects_invalid_capital_without_processing_job(self, launch_mock):
        bt = self._backtest(scenario=self._scenario(dynamic=False))
        Backtest.objects.filter(id=bt.id).update(
            status=Backtest.Status.DONE,
            capital_total=Decimal("0"),
            capital_per_ticker=Decimal("0"),
        )

        response = self.client.post(reverse("backtest_run", args=[bt.pk]), follow=True)

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        self.assertEqual(ProcessingJob.objects.count(), 0)
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        messages = list(response.context["messages"])
        self.assertTrue(any("capital par action" in str(message) for message in messages))

    @patch("core.views.launch_processing_job")
    @patch("core.views.check_dynamic_universe_readiness")
    def test_backtest_run_warning_without_confirmation_is_rejected(self, readiness_mock, launch_mock):
        readiness_mock.return_value = self._warning_report()
        bt = self._csi300_backtest()

        response = self.client.post(reverse("backtest_run", args=[bt.pk]), follow=True)

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        messages = list(response.context["messages"])
        self.assertTrue(any("Confirmez explicitement" in str(message) for message in messages))

    @patch("core.views.capture_backtest_configuration")
    @patch("core.views.launch_processing_job")
    @patch("core.views.check_dynamic_universe_readiness")
    def test_backtest_run_warning_with_confirmation_is_launched_and_stored(self, readiness_mock, launch_mock, capture_mock):
        report = self._warning_report()
        readiness_mock.return_value = report
        bt = self._csi300_backtest()
        job = ProcessingJob.objects.create(job_type=ProcessingJob.JobType.RUN_BACKTEST, status=ProcessingJob.Status.PENDING)
        launch_mock.return_value = type("Launch", (), {"job": job, "dispatch_error": None})()

        response = self.client.post(
            reverse("backtest_run", args=[bt.pk]),
            {
                "du_readiness_warning_ack": "1",
                "du_readiness_warning_hash": readiness_confirmation_hash(report),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertTrue(launch_mock.called)
        bt.refresh_from_db()
        ack = bt.settings[BACKTEST_READINESS_ACK_SETTINGS_KEY]
        self.assertEqual(ack["hash"], readiness_confirmation_hash(report))
        self.assertTrue(ack["allow_partial_coverage"])
        capture_mock.assert_called_once()

    @patch("core.views.launch_processing_job")
    @patch("core.views.check_dynamic_universe_readiness")
    def test_backtest_run_changed_period_requires_new_confirmation(self, readiness_mock, launch_mock):
        old_report = self._warning_report(start=date(2018, 1, 1), end=date(2018, 1, 2))
        new_report = self._warning_report(start=date(2018, 1, 1), end=date(2018, 1, 3))
        readiness_mock.return_value = new_report
        bt = self._csi300_backtest(start=date(2018, 1, 1), end=date(2018, 1, 3))

        response = self.client.post(
            reverse("backtest_run", args=[bt.pk]),
            {
                "du_readiness_warning_ack": "1",
                "du_readiness_warning_hash": readiness_confirmation_hash(old_report),
            },
            follow=True,
        )

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()

    @patch("core.views.launch_processing_job")
    @patch("core.services.dynamic_universe_ohlc_prepare.prepare_dynamic_universe_ohlc")
    @patch("core.services.universe_eodhd_sync.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.sp500_symbol_bootstrap.bootstrap_sp500_symbols_from_eodhd")
    def test_wizard_display_does_not_call_provider_or_launch_job(self, bootstrap_mock, sync_mock, prepare_mock, launch_mock):
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        prepare_mock.assert_not_called()
        launch_mock.assert_not_called()

    def test_wizard_display_does_not_write_domain_rows(self):
        bt = self._backtest()
        before = {
            "jobs": ProcessingJob.objects.count(),
            "universes": UniverseDefinition.objects.count(),
            "batches": UniverseImportBatch.objects.count(),
            "memberships": UniverseMembership.objects.count(),
            "coverage": UniverseCoverageSnapshot.objects.count(),
            "bars": DailyBar.objects.count(),
        }

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        after = {
            "jobs": ProcessingJob.objects.count(),
            "universes": UniverseDefinition.objects.count(),
            "batches": UniverseImportBatch.objects.count(),
            "memberships": UniverseMembership.objects.count(),
            "coverage": UniverseCoverageSnapshot.objects.count(),
            "bars": DailyBar.objects.count(),
        }
        self.assertEqual(after, before)

    @patch("core.views.check_dynamic_universe_readiness")
    def test_backtest_actions_remain_visible(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=False)
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "Lancer / Relancer")
        self.assertContains(response, "Calculer indicateurs")
