from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from django.db import IntegrityError, transaction
from django.test import TestCase
from django.utils import timezone

from core.models import (
    GameScenario,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
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
        self.csi_symbols = {
            "600519": Symbol.objects.create(ticker="600519", exchange="XSHG", name="Kweichow Moutai", active=True),
            "000001": Symbol.objects.create(ticker="000001", exchange="XSHE", name="Ping An Bank", active=True),
            "600000": Symbol.objects.create(ticker="600000", exchange="XSHG", name="SPD Bank", active=True),
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

    def _create_csi300(self) -> UniverseDefinition:
        return UniverseDefinition.objects.create(
            code="CSI300",
            name="CSI 300",
            source="manual_csv",
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

    def _create_coverage(
        self,
        universe: UniverseDefinition,
        start: date,
        end: date,
        *,
        snapshot_status: str = UniverseCoverageStatus.VALIDATED,
        batch_status: str = UniverseCoverageStatus.VALIDATED,
        expected_member_count: int = 3,
        actual_member_count: int | None = None,
        mapped_member_count: int | None = None,
        unmapped_member_count: int = 0,
    ) -> UniverseImportBatch:
        actual = expected_member_count if actual_member_count is None else actual_member_count
        mapped = actual if mapped_member_count is None else mapped_member_count
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="fixture",
            source_reference="unit-test",
            period_start=start,
            period_end=end,
            expected_member_count=expected_member_count,
            imported_member_count=actual,
            mapped_member_count=mapped,
            unmapped_member_count=unmapped_member_count,
            status=batch_status,
            validated_at=timezone.now() if batch_status == UniverseCoverageStatus.VALIDATED else None,
        )
        current = start
        while current <= end:
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=expected_member_count,
                actual_member_count=actual,
                mapped_member_count=mapped,
                unmapped_member_count=unmapped_member_count,
                status=snapshot_status,
                metadata={"fixture": True},
            )
            current += timedelta(days=1)
        return batch

    def test_models_store_definition_membership_and_open_interval(self):
        universe = self._create_sp500()
        membership = self._add_membership(universe, "AAA", date(2020, 1, 1), None)

        self.assertEqual(universe.code, "SP500")
        self.assertEqual(membership.valid_from, date(2020, 1, 1))
        self.assertIsNone(membership.valid_to)
        self.assertEqual(membership.ticker, "AAA")
        self.assertEqual(membership.symbol, self.symbols["AAA"])

    def test_coverage_models_store_status_counts_and_constraints(self):
        universe = self._create_sp500()
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="manual",
            source_name="fixture",
            source_reference="sp500.csv",
            period_start=date(2020, 1, 1),
            period_end=date(2020, 1, 31),
            expected_member_count=500,
            imported_member_count=500,
            mapped_member_count=500,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
            validated_at=timezone.now(),
        )
        snapshot = UniverseCoverageSnapshot.objects.create(
            universe=universe,
            import_batch=batch,
            coverage_date=date(2020, 1, 1),
            expected_member_count=500,
            actual_member_count=500,
            mapped_member_count=500,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
        )

        self.assertEqual(batch.status, UniverseCoverageStatus.VALIDATED)
        self.assertEqual(batch.period_start, date(2020, 1, 1))
        self.assertEqual(batch.period_end, date(2020, 1, 31))
        self.assertEqual(snapshot.coverage_date, date(2020, 1, 1))
        self.assertEqual(snapshot.actual_member_count, 500)
        self.assertEqual(snapshot.mapped_member_count, 500)

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                UniverseCoverageSnapshot.objects.create(
                    universe=universe,
                    import_batch=batch,
                    coverage_date=date(2020, 1, 1),
                )

        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                UniverseImportBatch.objects.create(
                    universe=universe,
                    period_start=date(2020, 2, 1),
                    period_end=date(2020, 1, 1),
                )

    def test_universe_definition_code_is_unique(self):
        self._create_sp500()
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self._create_sp500()

    def test_resolves_sp500_superset_active_by_date_and_membership_intervals(self):
        universe = self._create_fixture_memberships()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 20))

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

    def test_resolves_csi300_historical_dynamic_from_csv_universe(self):
        universe = self._create_csi300()
        self._add_membership(universe, "600519", date(2020, 1, 1), None, symbol=self.csi_symbols["600519"], exchange="XSHG")
        self._add_membership(universe, "000001", date(2020, 1, 1), date(2020, 1, 2), symbol=self.csi_symbols["000001"], exchange="XSHE")
        self._add_membership(universe, "600000", date(2020, 1, 3), None, symbol=self.csi_symbols["600000"], exchange="XSHG")
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 4), expected_member_count=1)
        scenario = Scenario.objects.create(
            name="Dynamic CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )

        result = self.resolver.resolve(scenario, start_date=date(2020, 1, 1), end_date=date(2020, 1, 4))

        self.assertEqual(result.mode, Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)
        self.assertEqual(result.universe_code, "CSI300")
        self.assertEqual(set(result.tickers), {"600519", "000001", "600000"})
        self.assertEqual(result.active_by_date[date(2020, 1, 1)], frozenset({"600519", "000001"}))
        self.assertEqual(result.active_by_date[date(2020, 1, 3)], frozenset({"600519", "600000"}))
        self.assertEqual(result.membership_by_ticker["000001"][0].exchange, "XSHE")
        self.assertEqual(result.metadata["source"], "manual_csv")

    def test_missing_csi300_definition_raises_configuration_error(self):
        scenario = Scenario.objects.create(
            name="Missing CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )

        with self.assertRaisesRegex(UniverseConfigurationError, "CSI300"):
            self.resolver.resolve(scenario, start_date=date(2020, 1, 1), end_date=date(2020, 1, 4))

    def test_csi300_without_memberships_raises_coverage_error(self):
        universe = self._create_csi300()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 4), expected_member_count=1, actual_member_count=1, mapped_member_count=1)
        scenario = Scenario.objects.create(
            name="Empty CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )

        with self.assertRaisesRegex(UniverseCoverageError, "Historical CSI300 membership"):
            self.resolver.resolve(scenario, start_date=date(2020, 1, 1), end_date=date(2020, 1, 4))

    def test_survivorship_bias_members_are_not_reduced_to_end_date_composition(self):
        universe = self._create_fixture_memberships()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 20))

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

        with self.assertRaisesRegex(UniverseCoverageError, "missing coverage snapshot for 2019-12-30"):
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
        universe = self._create_sp500()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 20))

        with self.assertRaisesRegex(UniverseCoverageError, "no memberships overlap"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 20),
            )

    def test_coverage_gap_raises_explicit_error(self):
        universe = self._create_sp500()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 10), expected_member_count=1)
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
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 10), expected_member_count=1)
        self._add_membership(universe, "AAA", date(2020, 1, 5), None)

        with self.assertRaisesRegex(UniverseCoverageError, "no active members on 2020-01-01"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 10),
            )

    def test_period_after_closed_coverage_raises_coverage_error(self):
        universe = self._create_sp500()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 10), expected_member_count=1)
        self._add_membership(universe, "AAA", date(2020, 1, 1), date(2020, 1, 5))

        with self.assertRaisesRegex(UniverseCoverageError, "no active members on 2020-01-06"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 10),
            )

    def test_unmapped_membership_raises_mapping_error(self):
        universe = self._create_sp500()
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 2), expected_member_count=1)
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
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 2), expected_member_count=1)
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
        self._create_coverage(universe, date(2020, 1, 1), date(2020, 1, 2), expected_member_count=1)
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

    def test_missing_coverage_snapshot_blocks_dynamic_resolution(self):
        universe = self._create_fixture_memberships()
        self._create_coverage(universe, date(2020, 1, 2), date(2020, 1, 3))

        with self.assertRaisesRegex(UniverseCoverageError, "missing coverage snapshot for 2020-01-01"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 3),
            )

    def test_snapshot_status_must_be_validated(self):
        for status in (
            UniverseCoverageStatus.IMPORTED,
            UniverseCoverageStatus.PARTIAL,
            UniverseCoverageStatus.FAILED,
            UniverseCoverageStatus.STALE,
        ):
            with self.subTest(status=status):
                UniverseCoverageSnapshot.objects.all().delete()
                UniverseImportBatch.objects.all().delete()
                UniverseMembership.objects.all().delete()
                UniverseDefinition.objects.all().delete()
                universe = self._create_fixture_memberships()
                self._create_coverage(
                    universe,
                    date(2020, 1, 1),
                    date(2020, 1, 1),
                    snapshot_status=status,
                )

                with self.assertRaisesRegex(UniverseCoverageError, f"snapshot_status={status}"):
                    self.resolver.resolve(
                        self.scenario,
                        start_date=date(2020, 1, 1),
                        end_date=date(2020, 1, 1),
                    )

    def test_validated_csi300_snapshot_in_partial_batch_resolves_requested_period(self):
        universe = self._create_csi300()
        self._add_membership(
            universe,
            "600519",
            date(2020, 1, 1),
            None,
            symbol=self.csi_symbols["600519"],
            exchange="XSHG",
        )
        self._add_membership(
            universe,
            "000001",
            date(2020, 1, 1),
            None,
            symbol=self.csi_symbols["000001"],
            exchange="XSHE",
        )
        self._add_membership(
            universe,
            "600000",
            date(2020, 1, 1),
            None,
            symbol=self.csi_symbols["600000"],
            exchange="XSHG",
        )
        self._create_coverage(
            universe,
            date(2020, 1, 1),
            date(2020, 1, 1),
            expected_member_count=3,
            batch_status=UniverseCoverageStatus.PARTIAL,
        )
        scenario = Scenario.objects.create(
            name="Dynamic CSI300",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )

        result = self.resolver.resolve(
            scenario,
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 1),
        )

        self.assertEqual(result.universe_code, "CSI300")
        self.assertEqual(set(result.tickers), {"600519", "000001", "600000"})

    def test_blocking_import_batch_statuses_reject_dynamic_resolution(self):
        for status in (
            UniverseCoverageStatus.IMPORTED,
            UniverseCoverageStatus.FAILED,
            UniverseCoverageStatus.STALE,
        ):
            with self.subTest(status=status):
                UniverseCoverageSnapshot.objects.all().delete()
                UniverseImportBatch.objects.all().delete()
                UniverseMembership.objects.all().delete()
                UniverseDefinition.objects.all().delete()
                universe = self._create_fixture_memberships()
                self._create_coverage(
                    universe,
                    date(2020, 1, 1),
                    date(2020, 1, 1),
                    batch_status=status,
                )

                with self.assertRaisesRegex(UniverseCoverageError, f"batch_status={status}"):
                    self.resolver.resolve(
                        self.scenario,
                        start_date=date(2020, 1, 1),
                        end_date=date(2020, 1, 1),
                    )

    def test_expected_member_count_mismatch_blocks_dynamic_resolution(self):
        universe = self._create_fixture_memberships()
        self._create_coverage(
            universe,
            date(2020, 1, 1),
            date(2020, 1, 1),
            expected_member_count=5,
            actual_member_count=4,
            mapped_member_count=4,
        )

        with self.assertRaisesRegex(UniverseCoverageError, "actual_member_count=4 expected_member_count=5"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 1),
            )

    def test_mapping_count_mismatch_blocks_dynamic_resolution(self):
        universe = self._create_fixture_memberships()
        self._create_coverage(
            universe,
            date(2020, 1, 1),
            date(2020, 1, 1),
            expected_member_count=3,
            actual_member_count=3,
            mapped_member_count=2,
        )

        with self.assertRaisesRegex(UniverseCoverageError, "mapped_member_count=2"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 1),
            )

    def test_unmapped_count_blocks_dynamic_resolution(self):
        universe = self._create_fixture_memberships()
        self._create_coverage(
            universe,
            date(2020, 1, 1),
            date(2020, 1, 1),
            expected_member_count=3,
            actual_member_count=3,
            mapped_member_count=3,
            unmapped_member_count=1,
        )

        with self.assertRaisesRegex(UniverseCoverageError, "unmapped_member_count=1"):
            self.resolver.resolve(
                self.scenario,
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 1),
            )

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
        self.assertNotIn("UniverseCoverageSnapshot", runner_source)
        self.assertNotIn("UniverseImportBatch", runner_source)
        self.assertNotIn("universe_resolver", sync_source)
        self.assertNotIn("UniverseResolver", sync_source)
        self.assertNotIn("UniverseCoverageSnapshot", sync_source)
        self.assertNotIn("UniverseImportBatch", sync_source)

    def test_backtest_engine_and_prep_do_not_import_dynamic_universe_resolver(self):
        base = Path(__file__).resolve().parents[1]
        engine_source = (base / "services" / "backtesting" / "engine.py").read_text()
        prep_source = (base / "services" / "backtesting" / "prep.py").read_text()

        self.assertNotIn("universe_resolver", engine_source)
        self.assertNotIn("UniverseResolver", engine_source)
        self.assertNotIn("UniverseCoverageSnapshot", engine_source)
        self.assertNotIn("UniverseImportBatch", engine_source)
        self.assertNotIn("universe_resolver", prep_source)
        self.assertNotIn("UniverseResolver", prep_source)
        self.assertNotIn("UniverseCoverageSnapshot", prep_source)
        self.assertNotIn("UniverseImportBatch", prep_source)
