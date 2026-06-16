from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase

from core.models import Scenario, Symbol
from core.services.dynamic_universe_auto_prepare import (
    DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR,
    DynamicUniverseAutoPrepareError,
    ensure_sp500_historical_universe_ready,
)
from core.services.provider_eodhd import EODHDError
from core.services.universe_resolver import UniverseCoverageError


class DynamicUniverseAutoPrepareTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.end = date(2024, 1, 3)
        self.symbol = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", active=True)
        self.resolved = SimpleNamespace(symbols=(self.symbol,), tickers=("AAPL",))

    def _scenario(self, *, dynamic: bool) -> Scenario:
        scenario = Scenario.objects.create(
            name="Dynamic" if dynamic else "Static",
            universe_mode=(
                Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
                if dynamic
                else Scenario.UniverseMode.STATIC_TICKERS
            ),
            active=True,
        )
        if not dynamic:
            scenario.symbols.add(self.symbol)
        return scenario

    @patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.UniverseResolver")
    def test_static_scenario_does_not_resolve_or_call_provider_helpers(self, resolver_cls, bootstrap_mock, sync_mock):
        result = ensure_sp500_historical_universe_ready(
            scenario=self._scenario(dynamic=False),
            start_date=self.start,
            end_date=self.end,
        )

        self.assertTrue(result.already_ready)
        self.assertIsNone(result.resolved_universe)
        resolver_cls.assert_not_called()
        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()

    @patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.UniverseResolver")
    def test_dynamic_already_ready_does_not_call_provider_helpers(self, resolver_cls, bootstrap_mock, sync_mock):
        resolver_cls.return_value.resolve.return_value = self.resolved

        result = ensure_sp500_historical_universe_ready(
            scenario=self._scenario(dynamic=True),
            start_date=self.start,
            end_date=self.end,
        )

        self.assertTrue(result.already_ready)
        self.assertIs(result.resolved_universe, self.resolved)
        bootstrap_mock.assert_not_called()
        sync_mock.assert_not_called()

    @patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.UniverseResolver")
    def test_dynamic_missing_coverage_bootstraps_syncs_and_resolves(self, resolver_cls, bootstrap_mock, sync_mock):
        resolver_cls.return_value.resolve.side_effect = [
            UniverseCoverageError("missing coverage snapshot for 2026-06-16"),
            self.resolved,
        ]
        bootstrap_mock.return_value = SimpleNamespace(created=10, warnings=["bootstrap warning"])
        sync_mock.return_value = SimpleNamespace(status="VALIDATED", warnings=["sync warning"])

        result = ensure_sp500_historical_universe_ready(
            scenario=self._scenario(dynamic=True),
            start_date=self.start,
            end_date=self.end,
        )

        self.assertFalse(result.already_ready)
        self.assertEqual(result.bootstrap_created, 10)
        self.assertEqual(result.sync_status, "VALIDATED")
        self.assertEqual(result.warnings, ["bootstrap warning", "sync warning"])
        self.assertIs(result.resolved_universe, self.resolved)
        bootstrap_mock.assert_called_once_with(coverage_start=self.start, coverage_end=self.end, dry_run=False)
        sync_mock.assert_called_once_with(coverage_start=self.start, coverage_end=self.end, dry_run=False)

    @patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.UniverseResolver")
    def test_dynamic_partial_after_sync_raises_user_friendly_error(self, resolver_cls, bootstrap_mock, sync_mock):
        resolver_cls.return_value.resolve.side_effect = [
            UniverseCoverageError("missing coverage snapshot"),
            UniverseCoverageError("snapshot_status=PARTIAL"),
        ]
        bootstrap_mock.return_value = SimpleNamespace(created=0, warnings=[])
        sync_mock.return_value = SimpleNamespace(
            status="PARTIAL",
            mapped_member_count=505,
            unmapped_member_count=1,
            warnings=["unmapped symbol XYZ"],
        )

        with self.assertRaisesMessage(DynamicUniverseAutoPrepareError, DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR) as ctx:
            ensure_sp500_historical_universe_ready(
                scenario=self._scenario(dynamic=True),
                start_date=self.start,
                end_date=self.end,
            )

        self.assertIn("sync_status=PARTIAL", ctx.exception.technical_detail)
        self.assertIn("unmapped=1", ctx.exception.technical_detail)

    @patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.UniverseResolver")
    def test_dynamic_eodhd_error_raises_user_friendly_error(self, resolver_cls, bootstrap_mock):
        resolver_cls.return_value.resolve.side_effect = UniverseCoverageError("missing coverage snapshot")
        bootstrap_mock.side_effect = EODHDError("EODHD_API_KEY is missing")

        with self.assertRaisesMessage(DynamicUniverseAutoPrepareError, DYNAMIC_UNIVERSE_AUTO_PREPARE_USER_ERROR) as ctx:
            ensure_sp500_historical_universe_ready(
                scenario=self._scenario(dynamic=True),
                start_date=self.start,
                end_date=self.end,
            )

        self.assertIn("EODHD_API_KEY is missing", ctx.exception.technical_detail)

    @patch("core.services.dynamic_universe_auto_prepare.sync_sp500_historical_memberships_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.bootstrap_sp500_symbols_from_eodhd")
    @patch("core.services.dynamic_universe_auto_prepare.UniverseResolver")
    def test_warmup_start_is_used_as_provider_coverage_start(self, resolver_cls, bootstrap_mock, sync_mock):
        warmup_start = date(2023, 12, 20)
        resolver_cls.return_value.resolve.side_effect = [
            UniverseCoverageError("missing coverage snapshot"),
            self.resolved,
        ]
        bootstrap_mock.return_value = SimpleNamespace(created=0, warnings=[])
        sync_mock.return_value = SimpleNamespace(status="VALIDATED", warnings=[])

        ensure_sp500_historical_universe_ready(
            scenario=self._scenario(dynamic=True),
            start_date=self.start,
            end_date=self.end,
            warmup_start_date=warmup_start,
        )

        bootstrap_mock.assert_called_once_with(coverage_start=warmup_start, coverage_end=self.end, dry_run=False)
        sync_mock.assert_called_once_with(coverage_start=warmup_start, coverage_end=self.end, dry_run=False)
