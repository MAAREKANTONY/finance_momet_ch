from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase
from django.utils import timezone

from core.models import (
    DailyBar,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.dynamic_universe_symbols import ensure_universe_membership_symbols
from core.services.dynamic_universe_readiness import (
    CHECK_ERROR,
    CHECK_OK,
    CHECK_SKIPPED,
    CHECK_WARNING,
    REPORT_NOT_READY,
    REPORT_READY_WITH_WARNINGS,
    check_dynamic_universe_readiness,
)


class DynamicUniverseReadinessTestCase(TestCase):
    start = date(2022, 1, 1)
    end = date(2022, 1, 3)

    def _symbol(self, ticker: str, *, exchange: str = "NYSE", sector: str = "Technology") -> Symbol:
        return Symbol.objects.create(ticker=ticker, exchange=exchange, sector=sector, active=True)

    def _sp500(self, *, active: bool = True) -> UniverseDefinition:
        return UniverseDefinition.objects.create(code="SP500", name="S&P 500", active=active, source="test")

    def _csi300(self, *, active: bool = True) -> UniverseDefinition:
        return UniverseDefinition.objects.create(code="CSI300", name="CSI 300", active=active, source="manual_csv")

    def _membership(self, universe: UniverseDefinition, symbol: Symbol, *, valid_to=None) -> UniverseMembership:
        return UniverseMembership.objects.create(
            universe=universe,
            symbol=symbol,
            ticker=symbol.ticker,
            exchange=symbol.exchange,
            provider_symbol=f"{symbol.ticker}.US",
            valid_from=self.start,
            valid_to=valid_to,
            source="test",
        )

    def _coverage(
        self,
        universe: UniverseDefinition,
        *,
        start=None,
        end=None,
        status=UniverseCoverageStatus.VALIDATED,
        batch_status=None,
        snapshot_status=None,
    ):
        start = start or self.start
        end = end or self.end
        batch_status = batch_status or status
        snapshot_status = snapshot_status or status
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="test",
            period_start=start,
            period_end=end,
            expected_member_count=1,
            imported_member_count=2,
            mapped_member_count=2,
            unmapped_member_count=0,
            status=batch_status,
            validated_at=timezone.now() if batch_status == UniverseCoverageStatus.VALIDATED else None,
        )
        current = start
        while current <= end:
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=2,
                mapped_member_count=2,
                unmapped_member_count=0,
                status=snapshot_status,
            )
            current += timedelta(days=1)
        return batch

    def _bar_edges(self, symbol: Symbol, *, start=None, end=None):
        start = start or self.start
        end = end or self.end
        for current in (start, end):
            DailyBar.objects.create(
                symbol=symbol,
                date=current,
                open=Decimal("10"),
                high=Decimal("10"),
                low=Decimal("10"),
                close=Decimal("10"),
                volume=1000,
                source="test",
            )

    def _check(self, report, code: str):
        return next(check for check in report.checks if check.code == code)

    def test_universe_definition_absent_is_not_ready_and_suggests_init(self):
        report = check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        self.assertFalse(report.ready)
        self.assertEqual(report.status, REPORT_NOT_READY)
        check = self._check(report, "universe_definition")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("init_reference_data", check.suggested_commands[0])

    def test_universe_definition_inactive_is_not_ready(self):
        self._sp500(active=False)

        report = check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        check = self._check(report, "universe_definition")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("inactif", check.message)

    def test_active_universe_without_memberships_is_not_ready(self):
        self._sp500()

        report = check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        check = self._check(report, "memberships")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("Aucun UniverseMembership", check.message)
        self.assertIn("sync_sp500_historical_memberships", " ".join(check.suggested_commands))

    def test_missing_coverage_snapshot_reports_current_blocker_message(self):
        universe = self._sp500()
        self._membership(universe, self._symbol("AAA"))
        self._coverage(universe, start=self.start + timedelta(days=1), end=self.end)

        report = check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        check = self._check(report, "coverage_snapshots")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("missing coverage snapshot for 2022-01-01", check.message)

    def test_csi300_valid_snapshots_in_partial_batch_pass_coverage_readiness(self):
        universe = self._csi300()
        symbol = self._symbol("600519", exchange="SHG", sector="Consumer Defensive")
        self._membership(universe, symbol)
        self._coverage(
            universe,
            batch_status=UniverseCoverageStatus.PARTIAL,
            snapshot_status=UniverseCoverageStatus.VALIDATED,
        )

        report = check_dynamic_universe_readiness(universe="CSI300", start=self.start, end=self.end)

        self.assertEqual(self._check(report, "import_batch").status, CHECK_OK)
        self.assertEqual(self._check(report, "coverage_snapshots").status, CHECK_OK)
        self.assertEqual(self._check(report, "historical_symbols").status, CHECK_OK)

    def test_csi300_partial_snapshots_in_partial_batch_block_coverage_readiness(self):
        universe = self._csi300()
        symbol = self._symbol("600519", exchange="SHG", sector="Consumer Defensive")
        self._membership(universe, symbol)
        self._coverage(universe, status=UniverseCoverageStatus.PARTIAL)

        report = check_dynamic_universe_readiness(universe="CSI300", start=self.start, end=self.end)

        check = self._check(report, "coverage_snapshots")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("snapshot_status=PARTIAL", check.message)

    def test_symbols_and_dailybars_are_skipped_when_memberships_or_coverage_are_unavailable(self):
        universe = self._sp500()
        self._membership(universe, self._symbol("AAA"))

        report = check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        self.assertEqual(self._check(report, "historical_symbols").status, CHECK_SKIPPED)
        self.assertEqual(self._check(report, "member_daily_bars").status, CHECK_SKIPPED)

    def test_member_dailybars_detect_missing_symbols_when_prerequisites_are_ready(self):
        universe = self._sp500()
        aaa = self._symbol("AAA")
        bbb = self._symbol("BBB")
        self._membership(universe, aaa)
        self._membership(universe, bbb)
        self._coverage(universe)
        self._bar_edges(aaa)

        report = check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        self.assertEqual(self._check(report, "historical_symbols").status, CHECK_OK)
        dailybars = self._check(report, "member_daily_bars")
        self.assertEqual(dailybars.status, CHECK_WARNING)
        self.assertEqual(report.status, REPORT_READY_WITH_WARNINGS)
        self.assertTrue(report.ready)
        self.assertEqual(dailybars.details["ready_symbols"], 1)
        self.assertEqual(dailybars.details["missing_examples"], ["BBB"])

    def test_require_gm_market_checks_benchmark_without_provider(self):
        universe = self._sp500()
        aaa = self._symbol("AAA", exchange="NYSE")
        self._membership(universe, aaa)
        self._coverage(universe)
        self._bar_edges(aaa)
        Symbol.objects.create(ticker="SPY", exchange="NYSE ARCA", active=True)

        report = check_dynamic_universe_readiness(
            universe="SP500",
            start=self.start,
            end=self.end,
            require_gm_market=True,
        )

        check = self._check(report, "gm_market_daily_bars")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertEqual(check.details["missing_ohlc"], ["SPY"])

    def test_require_gm_sector_checks_known_sector_etfs_without_provider(self):
        report = check_dynamic_universe_readiness(
            universe="SP500",
            start=self.start,
            end=self.end,
            require_gm_sector=True,
        )

        check = self._check(report, "gm_sector_daily_bars")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("XLK", check.details["missing_symbols"])
        self.assertIn("XLC", check.details["missing_symbols"])

    @patch("core.services.dynamic_universe_ohlc_prepare.prepare_dynamic_universe_ohlc")
    @patch("core.services.universe_eodhd_sync.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.sp500_symbol_bootstrap.bootstrap_sp500_symbols_from_eodhd")
    def test_readiness_does_not_call_provider_or_explicit_prepare_helpers(self, bootstrap_mock, sync_mock, prepare_mock):
        before = {
            "universes": UniverseDefinition.objects.count(),
            "memberships": UniverseMembership.objects.count(),
            "coverage": UniverseCoverageSnapshot.objects.count(),
            "dailybars": DailyBar.objects.count(),
        }

        check_dynamic_universe_readiness(universe="SP500", start=self.start, end=self.end)

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        prepare_mock.assert_not_called()
        after = {
            "universes": UniverseDefinition.objects.count(),
            "memberships": UniverseMembership.objects.count(),
            "coverage": UniverseCoverageSnapshot.objects.count(),
            "dailybars": DailyBar.objects.count(),
        }
        self.assertEqual(after, before)

    def test_csi300_without_memberships_suggests_generic_csv_import_only(self):
        self._csi300()

        report = check_dynamic_universe_readiness(universe="CSI300", start=self.start, end=self.end)

        check = self._check(report, "memberships")
        self.assertEqual(check.status, CHECK_ERROR)
        commands = " ".join(action.command for action in report.suggested_actions)
        self.assertIn("import_universe_memberships", commands)
        self.assertIn("--universe-code CSI300", commands)
        self.assertNotIn("sync_sp500_historical_memberships", commands)
        self.assertNotIn("bootstrap_sp500_symbols_from_eodhd", commands)

    @patch("core.services.dynamic_universe_ohlc_prepare.prepare_dynamic_universe_ohlc")
    @patch("core.services.universe_eodhd_sync.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.sp500_symbol_bootstrap.bootstrap_sp500_symbols_from_eodhd")
    def test_csi300_readiness_does_not_call_provider_helpers(self, bootstrap_mock, sync_mock, prepare_mock):
        check_dynamic_universe_readiness(universe="CSI300", start=self.start, end=self.end)

        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()
        prepare_mock.assert_not_called()

    def test_csi300_with_memberships_recommends_eodhd_ohlc_not_sp500_commands(self):
        universe = self._csi300()
        symbol = self._symbol("600519", exchange="SHG")
        self._membership(universe, symbol)
        self._coverage(universe)

        report = check_dynamic_universe_readiness(universe="CSI300", start=self.start, end=self.end)

        check = self._check(report, "member_daily_bars")
        self.assertEqual(check.status, CHECK_WARNING)
        commands = " ".join(action.command for action in report.suggested_actions)
        self.assertIn("Préparation OHLC CSI300 via EODHD", commands)
        self.assertIn("tickers issus du CSV importé", commands)
        self.assertNotIn("sync_sp500_historical_memberships", commands)
        self.assertNotIn("bootstrap_sp500_symbols_from_eodhd", commands)

    def test_csi300_mapping_refreshes_readiness_to_missing_dailybars_warning(self):
        universe = self._csi300()
        UniverseMembership.objects.create(
            universe=universe,
            ticker="600519",
            exchange="SHG",
            provider_symbol="600519.SHG",
            valid_from=self.start,
            valid_to=None,
            source="manual_csv",
            source_payload={"company_name": "Kweichow Moutai", "row": {"country": "CN", "currency": "CNY"}},
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="manual_csv",
            source_name="manual_csv",
            period_start=self.start,
            period_end=self.end,
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=0,
            unmapped_member_count=1,
            status=UniverseCoverageStatus.PARTIAL,
        )
        current = self.start
        while current <= self.end:
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=1,
                mapped_member_count=0,
                unmapped_member_count=1,
                status=UniverseCoverageStatus.PARTIAL,
            )
            current += timedelta(days=1)

        mapping = ensure_universe_membership_symbols("CSI300")
        report = check_dynamic_universe_readiness(universe="CSI300", start=self.start, end=self.end)

        self.assertEqual(mapping.created_symbols, 1)
        self.assertEqual(self._check(report, "coverage_snapshots").status, CHECK_OK)
        self.assertEqual(self._check(report, "historical_symbols").status, CHECK_OK)
        self.assertEqual(self._check(report, "member_daily_bars").status, CHECK_WARNING)

    def test_csi300_gm_market_is_blocked_without_explicit_benchmark(self):
        report = check_dynamic_universe_readiness(
            universe="CSI300",
            start=self.start,
            end=self.end,
            require_gm_market=True,
        )

        check = self._check(report, "gm_market_daily_bars")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("GM market non supporté pour CSI300 V1", check.message)
        self.assertNotIn("SPY", check.message)

    def test_csi300_gm_sector_is_blocked_for_us_sector_benchmarks(self):
        report = check_dynamic_universe_readiness(
            universe="CSI300",
            start=self.start,
            end=self.end,
            require_gm_sector=True,
        )

        check = self._check(report, "gm_sector_daily_bars")
        self.assertEqual(check.status, CHECK_ERROR)
        self.assertIn("GM sectoriel non supporté pour CSI300 V1", check.message)


class DynamicUniverseReadinessCommandTests(TestCase):
    def test_command_outputs_human_not_ready_report(self):
        out = StringIO()

        call_command(
            "check_dynamic_universe_readiness",
            "--universe", "SP500",
            "--start", "2022-01-01",
            "--end", "2022-12-31",
            stdout=out,
        )

        output = out.getvalue()
        self.assertIn("Dynamic Universe readiness", output)
        self.assertIn("Statut global : NOT_READY", output)
        self.assertIn("python manage.py init_reference_data", output)

    def test_command_json_outputs_valid_payload(self):
        out = StringIO()

        call_command(
            "check_dynamic_universe_readiness",
            "--universe", "SP500",
            "--start", "2022-01-01",
            "--end", "2022-01-03",
            "--json",
            stdout=out,
        )

        payload = json.loads(out.getvalue())
        self.assertEqual(payload["universe"], "SP500")
        self.assertEqual(payload["status"], "NOT_READY")
        self.assertIn("checks", payload)
        self.assertIn("suggested_actions", payload)

    def test_command_strict_exit_code_raises_when_not_ready(self):
        with self.assertRaises(CommandError):
            call_command(
                "check_dynamic_universe_readiness",
                "--universe", "SP500",
                "--start", "2022-01-01",
                "--end", "2022-01-03",
                "--strict-exit-code",
                stdout=StringIO(),
            )

    def test_command_without_strict_exit_code_returns_zero_style_diagnostic(self):
        out = StringIO()

        call_command(
            "check_dynamic_universe_readiness",
            "--universe", "SP500",
            "--start", "2022-01-01",
            "--end", "2022-01-03",
            stdout=out,
        )

        self.assertIn("NOT_READY", out.getvalue())
