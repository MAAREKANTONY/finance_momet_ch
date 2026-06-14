from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from django.test import TestCase
from django.utils import timezone

from core.models import (
    Alert,
    Backtest,
    DailyBar,
    DailyMetric,
    Scenario,
    Symbol,
    UniverseCoverageSnapshot,
    UniverseCoverageStatus,
    UniverseDefinition,
    UniverseImportBatch,
    UniverseMembership,
)
from core.services.backtesting.engine import run_backtest, run_backtest_kpi_only
from core.services.backtesting.prep import prepare_backtest_data
from core.services.universe_resolver import UniverseResolver
from core.tasks import run_backtest_task


class DynamicUniverseBacktestIntegrationTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.end = date(2024, 1, 4)
        self.old = Symbol.objects.create(ticker="OLD", exchange="NYSE", sector="Tech", active=True)
        self.new = Symbol.objects.create(ticker="NEW", exchange="NYSE", sector="Energy", active=True)
        self.keep = Symbol.objects.create(ticker="KEEP", exchange="NASDAQ", sector="Health", active=True)

    def _scenario(self, *, dynamic: bool) -> Scenario:
        scenario = Scenario.objects.create(
            name="Dynamic" if dynamic else "Static",
            universe_mode=(
                Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
                if dynamic
                else Scenario.UniverseMode.STATIC_TICKERS
            ),
            active=True,
            nglobal=2,
        )
        if not dynamic:
            scenario.symbols.add(self.old)
        return scenario

    def _backtest(self, scenario: Scenario, *, close_positions_at_end: bool = False) -> Backtest:
        return Backtest.objects.create(
            name="Dynamic universe backtest",
            scenario=scenario,
            start_date=self.start,
            end_date=self.end,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode=Backtest.CapitalMode.FIXED,
            ratio_threshold=Decimal("0"),
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[] if scenario.universe_mode == Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC else [self.old.ticker],
            warmup_days=0,
            close_positions_at_end=close_positions_at_end,
        )

    def _bars_metrics(self, symbol: Symbol, closes: list[str] | None = None):
        closes = closes or ["10", "11", "12", "13"]
        for offset, raw_close in enumerate(closes):
            current = self.start + timedelta(days=offset)
            close = Decimal(raw_close)
            DailyBar.objects.create(
                symbol=symbol,
                date=current,
                open=close,
                high=close,
                low=close,
                close=close,
                volume=1000 + offset,
            )
            DailyMetric.objects.create(
                symbol=symbol,
                scenario=self.scenario,
                date=current,
                P=close,
                ratio_P=Decimal("1"),
            )

    def _market_data(self):
        for symbol in [self.old, self.new, self.keep]:
            self._bars_metrics(symbol)

    def _validated_sp500_universe(self):
        universe = UniverseDefinition.objects.create(
            code="SP500",
            name="S&P 500",
            source="test",
            active=True,
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.old,
            ticker=self.old.ticker,
            exchange=self.old.exchange,
            provider_symbol="OLD.US",
            valid_from=self.start,
            valid_to=self.start + timedelta(days=1),
            source="test",
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.keep,
            ticker=self.keep.ticker,
            exchange=self.keep.exchange,
            provider_symbol="KEEP.US",
            valid_from=self.start,
            valid_to=None,
            source="test",
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=self.new,
            ticker=self.new.ticker,
            exchange=self.new.exchange,
            provider_symbol="NEW.US",
            valid_from=self.start + timedelta(days=2),
            valid_to=None,
            source="test",
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="test",
            period_start=self.start,
            period_end=self.end,
            expected_member_count=1,
            imported_member_count=3,
            mapped_member_count=3,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
            validated_at=timezone.now(),
        )
        for offset in range((self.end - self.start).days + 1):
            current = self.start + timedelta(days=offset)
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=2,
                mapped_member_count=2,
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
        return universe

    def _fake_prep(self):
        return SimpleNamespace(did_fetch_bars=False, did_compute_metrics=False, notes=["test prep"])

    def test_static_backtest_does_not_use_dynamic_resolver_or_add_universe_meta(self):
        self.scenario = self._scenario(dynamic=False)
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)

        with patch("core.services.universe_resolver.UniverseResolver.resolve") as resolver_mock:
            with patch("core.services.backtesting.prep.prepare_backtest_data", return_value=self._fake_prep()):
                run_backtest_task(bt.id)

        resolver_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        self.assertEqual(bt.universe_snapshot, [self.old.ticker])
        self.assertNotIn("universe", bt.results["meta"])
        old_daily = bt.results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertTrue(old_daily)
        self.assertNotIn("universe_member", old_daily[0])
        self.assertNotIn("buy_blocked_by_universe", old_daily[0])
        self.assertNotIn("buy_blocked_reason", old_daily[0])

    def test_dynamic_resolver_error_fails_backtest_before_prep(self):
        self.scenario = self._scenario(dynamic=True)
        bt = self._backtest(self.scenario)

        with patch("core.services.backtesting.prep.prepare_backtest_data") as prep_mock:
            with self.assertRaisesMessage(Exception, "UniverseDefinition SP500 is missing or inactive"):
                run_backtest_task(bt.id)

        prep_mock.assert_not_called()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.FAILED)
        self.assertIn("UniverseDefinition SP500 is missing or inactive", bt.error_message)

    def test_dynamic_task_sets_superset_snapshot_and_metadata(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.keep, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)

        def assert_dynamic_snapshot(backtest):
            backtest.refresh_from_db()
            self.assertEqual({row["ticker"] for row in backtest.universe_snapshot}, {"KEEP", "NEW", "OLD"})
            return self._fake_prep()

        with patch("core.services.backtesting.prep.prepare_backtest_data", side_effect=assert_dynamic_snapshot):
            run_backtest_task(bt.id)

        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        self.assertEqual({row["ticker"] for row in bt.universe_snapshot}, {"KEEP", "NEW", "OLD"})
        universe_meta = bt.results["meta"]["universe"]
        self.assertEqual(universe_meta["mode"], Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)
        self.assertEqual(universe_meta["universe_code"], "SP500")
        self.assertEqual(universe_meta["superset_count"], 3)
        self.assertEqual(universe_meta["coverage_start"], self.start.isoformat())
        self.assertEqual(universe_meta["coverage_end"], self.end.isoformat())

    def test_prepare_backtest_data_uses_dict_snapshot_superset(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._bars_metrics(self.keep)
        bt = self._backtest(self.scenario)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [
            {"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector}
            for symbol in resolved.symbols
        ]
        bt.save(update_fields=["universe_snapshot"])

        with patch("core.tasks.fetch_daily_bars_task") as fetch_mock:
            with patch("core.tasks._compute_metrics_for_scenario") as compute_mock:
                report = prepare_backtest_data(bt)

        fetch_mock.assert_called_once()
        compute_mock.assert_called_once()
        self.assertTrue(report.did_fetch_bars)
        self.assertTrue(report.did_compute_metrics)
        self.assertIn("Missing DailyBar coverage for 2 symbols", " ".join(report.notes))

    def test_dynamic_buy_gate_blocks_new_buy_after_exit(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start + timedelta(days=2), alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertNotIn("BUY", {row.get("action") for row in old_daily})
        blocked_row = next(row for row in old_daily if row["date"] == (self.start + timedelta(days=2)).isoformat())
        self.assertIsNone(blocked_row["action"])
        self.assertEqual(blocked_row["shares"], 0)
        self.assertIs(blocked_row["universe_member"], False)
        self.assertIs(blocked_row["buy_blocked_by_universe"], True)
        self.assertEqual(blocked_row["buy_blocked_reason"], "not_active_in_universe")

    def test_dynamic_position_survives_exit_and_sell_after_exit_is_allowed(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.bulk_create([
            Alert(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1"),
            Alert(symbol=self.old, scenario=self.scenario, date=self.start + timedelta(days=2), alerts="B1"),
        ])
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertEqual(old_daily[0]["action"], "BUY")
        self.assertGreater(old_daily[0]["shares"], 0)
        self.assertIs(old_daily[0]["universe_member"], True)
        self.assertIs(old_daily[0]["buy_blocked_by_universe"], False)
        self.assertIsNone(old_daily[0]["buy_blocked_reason"])
        self.assertIsNone(old_daily[1]["action"])
        self.assertGreater(old_daily[1]["shares"], 0)
        self.assertEqual(old_daily[2]["action"], "SELL")
        self.assertIs(old_daily[2]["universe_member"], False)
        self.assertIs(old_daily[2]["buy_blocked_by_universe"], False)
        self.assertIsNone(old_daily[2]["buy_blocked_reason"])
        self.assertFalse(old_daily[1]["forced_close"])

    def test_dynamic_position_stays_valued_after_exit_without_universe_block_flag(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        post_exit_row = next(row for row in old_daily if row["date"] == (self.start + timedelta(days=2)).isoformat())
        self.assertIsNone(post_exit_row["action"])
        self.assertGreater(post_exit_row["shares"], 0)
        self.assertIs(post_exit_row["universe_member"], False)
        self.assertIs(post_exit_row["buy_blocked_by_universe"], False)
        self.assertIsNone(post_exit_row["buy_blocked_reason"])
        self.assertFalse(post_exit_row["forced_close"])

    def test_dynamic_no_signal_after_exit_is_not_marked_as_universe_blocked(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        bt = self._backtest(self.scenario, close_positions_at_end=False)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        post_exit_row = next(row for row in old_daily if row["date"] == (self.start + timedelta(days=2)).isoformat())
        self.assertIsNone(post_exit_row["action"])
        self.assertEqual(post_exit_row["shares"], 0)
        self.assertIs(post_exit_row["universe_member"], False)
        self.assertIs(post_exit_row["buy_blocked_by_universe"], False)
        self.assertIsNone(post_exit_row["buy_blocked_reason"])

    def test_dynamic_forced_sell_final_is_unchanged_after_index_exit(self):
        self.scenario = self._scenario(dynamic=True)
        self._validated_sp500_universe()
        self._market_data()
        Alert.objects.create(symbol=self.old, scenario=self.scenario, date=self.start, alerts="A1")
        bt = self._backtest(self.scenario, close_positions_at_end=True)
        resolved = UniverseResolver().resolve(self.scenario, self.start, self.end)
        bt.universe_snapshot = [{"ticker": symbol.ticker, "exchange": symbol.exchange, "sector": symbol.sector} for symbol in resolved.symbols]
        bt.save(update_fields=["universe_snapshot"])

        results = run_backtest(bt, resolved_universe=resolved).results

        old_daily = results["tickers"]["OLD"]["lines"][0]["daily"]
        self.assertEqual(old_daily[0]["action"], "BUY")
        self.assertIn("FORCED_SELL", old_daily[-1]["action"])
        self.assertTrue(old_daily[-1]["forced_close"])

    def test_game_runtime_and_kpi_only_stay_out_of_dynamic_universe_integration(self):
        base = Path(__file__).resolve().parents[1]
        for relative in [
            "services/game_scenarios/runner.py",
            "services/game_scenarios/sync.py",
        ]:
            source = (base / relative).read_text()
            self.assertNotIn("UniverseResolver", source)
            self.assertNotIn("universe_resolver", source)
        self.assertEqual(run_backtest_kpi_only.__name__, "run_backtest_kpi_only")
