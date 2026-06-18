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
from core.services.dynamic_universe_readiness import ReadinessAction, ReadinessCheck, ReadinessReport


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
        self.assertNotIn("Préparation Dynamic Universe S&amp;P500", body)
        readiness_mock.assert_not_called()

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_present_for_dynamic_sp500(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=False)
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Préparation Dynamic Universe S&amp;P500", body)
        self.assertIn("NOT_READY", body)
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

        self.assertContains(response, "Backtest non prêt : compléter les étapes ci-dessous")

    @patch("core.views.check_dynamic_universe_readiness")
    def test_wizard_ready_status_is_rendered(self, readiness_mock):
        readiness_mock.return_value = self._report(ready=True)
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertContains(response, "READY")
        self.assertContains(response, "Prêt pour backtest")

    def test_missing_universe_definition_shows_init_reference_data_action(self):
        bt = self._backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("UniverseDefinition SP500 est absent", body)
        self.assertIn("init_reference_data", body)
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

        self.assertContains(response, "Coverage non validée: missing coverage snapshot for 2024-01-01")

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

        self.assertContains(response, "DailyBars GM_market")
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

        self.assertContains(response, "DailyBars GM_sector")
        self.assertTrue(readiness_mock.call_args.kwargs["require_gm_sector"])

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
