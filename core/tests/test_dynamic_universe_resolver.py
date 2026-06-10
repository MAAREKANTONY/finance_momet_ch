from __future__ import annotations

from datetime import date
from pathlib import Path

from django.db import IntegrityError, transaction
from django.test import TestCase

from core.models import GameScenario, Scenario, Symbol, UniverseDefinition, UniverseMembership
from core.services.universe_resolver import (
    SP500_UNIVERSE_CODE,
    UniverseConfigurationError,
    UniverseCoverageError,
    UniverseMappingError,
    UniverseResolver,
    resolve_universe_for_backtest,
)


class DynamicUniverseResolverTests(TestCase):
    def setUp(self):
        self.symbols = {
            ticker: Symbol.objects.create(ticker=ticker, exchange="NYSE", name=ticker, active=True)
            for ticker in ("AAA", "BBB", "CCC", "OLD", "NEW")
        }
        self.scenario = Scenario.objects.create(
            name="Dynamic SP500",
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            active=True,
        )
        self.resolver = UniverseResolver()

    def _create_sp500(self) -> UniverseDefinition:
        return UniverseDefinition.objects.create(
            code=SP500_UNIVERSE_CODE,
            name="S&P 500",
            source="manual_fixture",
            active=True,
            metadata={"fixture": True},
        )

    def _add_membership(
        self,
        universe: UniverseDefinition,
        ticker: str,
        valid_from: date,
        valid_to: date | None = None,
        symbol: Symbol | None = None,
        exchange: str = "NYSE",
    ) -> UniverseMembership:
        return UniverseMembership.objects.create(
            universe=universe,
            symbol=symbol if symbol is not None else self.symbols[ticker],
            ticker=ticker,
            exchange=exchange,
            provider_symbol=f"{ticker}.US",
            valid_from=valid_from,
            valid_to=valid_to,
            source="test_fixture",
            source_payload={"ticker": ticker},
        )

    def _create_fixture_memberships(self) -> UniverseDefinition:
        universe = self._create_sp500()
        self._add_membership(universe, "AAA", date(2020, 1, 1), None)
        self._add_membership(universe, "BBB", date(2020, 1, 1), date(2020, 1, 10))
        self._add_membership(universe, "CCC", date(2020, 1, 5), None)
        self._add_membership(universe, "OLD", date(2020, 1, 1), date(2020, 1, 10))
        self._add_membership(universe, "NEW", date(2020, 1, 11), None)
        return universe

    def test_models_store_definition_membership_and_open_interval(self):
        universe = self._create_sp500()
        membership = self._add_membership(universe, "AAA", date(2020, 1, 1), None)

        self.assertEqual(universe.code, "SP500")
        self.assertEqual(membership.valid_from, date(2020, 1, 1))
        self.assertIsNone(membership.valid_to)
        self.assertEqual(membership.ticker, "AAA")
        self.assertEqual(membership.symbol, self.symbols["AAA"])

    def test_universe_definition_code_is_unique(self):
        self._create_sp500()
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self._create_sp500()

    def test_resolves_sp500_superset_active_by_date_and_membership_intervals(self):
        self._create_fixture_memberships()

        result = self.resolver.resolve(
            self.scenario,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 20),
        )

        self.assertEqual(result.mode, Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)
        self.assertEqual(result.universe_code, "SP500")
        self.assertEqual(set(result.tickers), {"AAA", "BBB", "CCC", "OLD", "NEW"})
        self.assertEqual(result.active_by_date[date(2020, 1, 1)], frozenset({"AAA", "BBB", "OLD"}))
        self.assertEqual(result.active_by_date[date(2020, 1, 5)], frozenset({"AAA", "BBB", "CCC", "OLD"}))
        self.assertEqual(result.active_by_date[date(2020, 1, 11)], frozenset({"AAA", "CCC", "NEW"}))
        self.assertEqual(result.membership_by_ticker["OLD"][0].valid_to, date(2020, 1, 10))
        self.assertIsNone(result.membership_by_ticker["NEW"][0].valid_to)
        self.assertEqual(result.metadata["membership_count"], 5)

    def test_survivorship_bias_members_are_not_reduced_to_end_date_composition(self):
        self._create_fixture_memberships()

        result = resolve_universe_for_backtest(
            self.scenario,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 20),
        )

        final_members = result.active_by_date[date(2020, 1, 20)]
        self.assertIn("OLD", result.tickers)
        self.assertNotIn("OLD", final_members)
        self.assertIn("NEW", result.tickers)
        self.assertNotIn("NEW", result.active_by_date[date(2020, 1, 10)])
        self.assertNotEqual(set(result.tickers), set(final_members))

    def test_warmup_start_date_extends_coverage_requirement(self):
        universe = self._create_sp500()
        self._add_membership(universe, "AAA", date(2020, 1, 1), None)

        with self.assertRaisesRegex(UniverseCoverageError, "no active members on 2019-12-30"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 5),
                warmup_start_date=date(2019, 12, 30),
            )

    def test_missing_sp500_definition_raises_configuration_error(self):
        with self.assertRaisesRegex(UniverseConfigurationError, "SP500"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 20),
            )

    def test_no_memberships_raises_coverage_error(self):
        self._create_sp500()

        with self.assertRaisesRegex(UniverseCoverageError, "no memberships overlap"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 20),
            )

    def test_coverage_gap_raises_explicit_error(self):
        universe = self._create_sp500()
        self._add_membership(universe, "AAA", date(2020, 1, 1), date(2020, 1, 5))
        self._add_membership(universe, "BBB", date(2020, 1, 7), None)

        with self.assertRaisesRegex(UniverseCoverageError, "no active members on 2020-01-06"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 10),
            )

    def test_period_before_first_membership_raises_coverage_error(self):
        universe = self._create_sp500()
        self._add_membership(universe, "AAA", date(2020, 1, 5), None)

        with self.assertRaisesRegex(UniverseCoverageError, "no active members on 2020-01-01"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 10),
            )

    def test_period_after_closed_coverage_raises_coverage_error(self):
        universe = self._create_sp500()
        self._add_membership(universe, "AAA", date(2020, 1, 1), date(2020, 1, 5))

        with self.assertRaisesRegex(UniverseCoverageError, "no active members on 2020-01-06"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 10),
            )

    def test_unmapped_membership_raises_mapping_error(self):
        universe = self._create_sp500()
        UniverseMembership.objects.create(
            universe=universe,
            ticker="MISSING",
            exchange="NYSE",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            source="test_fixture",
        )

        with self.assertRaisesRegex(UniverseMappingError, "not mapped"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 2),
            )

    def test_ambiguous_membership_mapping_raises_mapping_error(self):
        universe = self._create_sp500()
        Symbol.objects.create(ticker="DUP", exchange="NYSE", active=True)
        Symbol.objects.create(ticker="DUP", exchange="NASDAQ", active=True)
        UniverseMembership.objects.create(
            universe=universe,
            ticker="DUP",
            exchange="",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            source="test_fixture",
        )

        with self.assertRaisesRegex(UniverseMappingError, "multiple local Symbols"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 2),
            )

    def test_membership_without_symbol_can_map_by_exact_ticker_exchange(self):
        universe = self._create_sp500()
        UniverseMembership.objects.create(
            universe=universe,
            ticker="AAA",
            exchange="NYSE",
            valid_from=date(2020, 1, 1),
            valid_to=None,
            source="test_fixture",
        )

        result = self.resolver.resolve(
            self.scenario,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 2),
        )

        self.assertEqual(result.symbols[0], self.symbols["AAA"])

    def test_static_tickers_mode_uses_scenario_symbols_without_universe_definition(self):
        static_scenario = Scenario.objects.create(
            name="Static",
            universe_mode=Scenario.UniverseMode.STATIC_TICKERS,
            active=True,
        )
        static_scenario.symbols.set([self.symbols["AAA"], self.symbols["BBB"]])

        result = self.resolver.resolve(
            static_scenario,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 3),
        )

        self.assertEqual(result.mode, Scenario.UniverseMode.STATIC_TICKERS)
        self.assertIsNone(result.universe_code)
        self.assertEqual(set(result.tickers), {"AAA", "BBB"})
        self.assertEqual(result.active_by_date[date(2020, 1, 2)], frozenset({"AAA", "BBB"}))

    def test_game_models_and_services_do_not_depend_on_dynamic_universe_resolver(self):
        self.assertFalse(hasattr(GameScenario, "universe_mode"))
        base = Path(__file__).resolve().parents[1]
        runner_source = (base / "services" / "game_scenarios" / "runner.py").read_text()
        sync_source = (base / "services" / "game_scenarios" / "sync.py").read_text()

        self.assertNotIn("universe_resolver", runner_source)
        self.assertNotIn("UniverseResolver", runner_source)
        self.assertNotIn("universe_resolver", sync_source)
        self.assertNotIn("UniverseResolver", sync_source)

    def test_backtest_engine_and_prep_do_not_import_dynamic_universe_resolver(self):
        base = Path(__file__).resolve().parents[1]
        engine_source = (base / "services" / "backtesting" / "engine.py").read_text()
        prep_source = (base / "services" / "backtesting" / "prep.py").read_text()

        self.assertNotIn("universe_resolver", engine_source)
        self.assertNotIn("UniverseResolver", engine_source)
        self.assertNotIn("universe_resolver", prep_source)
        self.assertNotIn("UniverseResolver", prep_source)
