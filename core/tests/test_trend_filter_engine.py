from __future__ import annotations

import copy
import json
from pathlib import Path
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.forms import BacktestForm
from core.forms import _normalize_gm_condition_entry as normalize_form_gm_condition_entry
from core.forms import _normalize_gm_push_condition_entry as normalize_form_gm_push_condition_entry
from core.models import Alert, Backtest, DailyBar, DailyMetric, HistoricalMarketCap, Scenario, Symbol
from core.services.backtesting.engine import (
    _normalize_gm_condition_entry as normalize_engine_gm_condition_entry,
    _normalize_gm_push_condition_entry as normalize_engine_gm_push_condition_entry,
    run_backtest,
    run_backtest_kpi_only,
)
from core.services.gm_push import (
    GM_PUSH_NEG_ACTIVE,
    GM_PUSH_POS_ACTIVE,
    GM_PUSH_UNKNOWN,
    compute_current_push_values_by_date,
    compute_push_state_by_date,
    compute_push_values_for_series,
)
from core.services.trend_filters import (
    TREND_FILTER_GM_CURRENT_KEY,
    TREND_FILTER_GM_MARKET_KEY,
    TREND_FILTER_GM_SECTOR_KEY,
    TREND_FILTER_OPERATOR_KEY,
    evaluate_trend_filters_for_symbol,
    preload_benchmark_price_cache,
)


class TrendFilterEngineTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self._symbol_seq = 0
        self.scenario = Scenario.objects.create(
            name="Trend Filter Scenario",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            npente=2,
            slope_threshold=Decimal("0.01"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.005"),
            nglobal=1,
            history_years=2,
        )
        self.symbol = Symbol.objects.create(
            ticker="AAA",
            exchange="NYSE",
            country="US",
            sector="Technology",
            active=True,
        )
        self.spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", country="US", sector="Financials", active=True)
        self.xlk = Symbol.objects.create(ticker="XLK", exchange="NYSE", country="US", sector="Technology", active=True)

    def _fresh_symbol(
        self,
        *,
        exchange: str = "NYSE",
        country: str = "US",
        sector: str = "Technology",
    ) -> Symbol:
        self._symbol_seq += 1
        return Symbol.objects.create(
            ticker=f"SYM{self._symbol_seq}",
            exchange=exchange,
            country=country,
            sector=sector,
            active=True,
        )

    def _add_bars(self, symbol: Symbol, rows: list[dict[str, str | date]]) -> None:
        DailyBar.objects.bulk_create([
            DailyBar(
                symbol=symbol,
                date=row["date"],
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=1000,
            )
            for row in rows
        ])

    def _add_symbol_fixture(
        self,
        *,
        symbol: Symbol | None = None,
        prices: list[str] | None = None,
        alerts_by_offset: dict[int, str] | None = None,
    ) -> Symbol:
        symbol = symbol or self.symbol
        prices = prices or ["10", "11", "12", "13"]
        alerts_by_offset = alerts_by_offset or {0: "Af", 1: "SPVa_basse"}
        self._add_bars(
            symbol,
            rows=[
                {
                    "date": self.start + timedelta(days=i),
                    "open": price,
                    "high": Decimal(price) + Decimal("1"),
                    "low": Decimal(price) - Decimal("1"),
                    "close": price,
                }
                for i, price in enumerate(prices)
            ],
        )
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=symbol,
                scenario=self.scenario,
                date=self.start + timedelta(days=i),
                P=Decimal(price),
                ratio_P=Decimal("1"),
                K1=Decimal("1"),
                slope_vrai_basse=Decimal("0.02"),
            )
            for i, price in enumerate(prices)
        ])
        Alert.objects.bulk_create([
            Alert(
                symbol=symbol,
                scenario=self.scenario,
                date=self.start + timedelta(days=offset),
                alerts=alerts,
            )
            for offset, alerts in sorted(alerts_by_offset.items())
        ])
        return symbol

    def _add_benchmark_fixture(self, symbol: Symbol, rows: list[dict[str, str | date]]) -> None:
        self._add_bars(symbol, rows)

    def _create_backtest(
        self,
        *,
        settings: dict | None = None,
        signal_lines: list[dict] | None = None,
        prices: list[str] | None = None,
        alerts_by_offset: dict[int, str] | None = None,
        symbol: Symbol | None = None,
        close_positions_at_end: bool = False,
    ) -> Backtest:
        symbol = self._add_symbol_fixture(symbol=symbol or self._fresh_symbol(), prices=prices, alerts_by_offset=alerts_by_offset)
        backtest = Backtest.objects.create(
            name="Trend Filter Backtest",
            scenario=self.scenario,
            start_date=self.start,
            end_date=self.start + timedelta(days=(len(prices or ["10", "11", "12", "13"]) - 1)),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=signal_lines or [{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "sell": [],
                "buy_gm_filter": "IGNORE",
                "sell_gm_filter": "IGNORE",
            }],
            universe_snapshot=[symbol.ticker],
            warmup_days=0,
            close_positions_at_end=close_positions_at_end,
            settings=settings or {},
        )
        backtest._test_symbol = symbol
        return backtest

    def _actions(self, backtest: Backtest) -> list[str | None]:
        ticker = backtest.universe_snapshot[0]
        return [row.get("action") for row in run_backtest(backtest).results["tickers"][ticker]["lines"][0]["daily"]]

    def _daily(self, backtest: Backtest) -> list[dict]:
        ticker = backtest.universe_snapshot[0]
        return run_backtest(backtest).results["tickers"][ticker]["lines"][0]["daily"]

    def _kpi_trade_count(self, backtest: Backtest) -> int:
        ticker = backtest.universe_snapshot[0]
        result = run_backtest_kpi_only(backtest)
        return int(result[ticker]["lines"][0]["final"]["N"] or 0)

    def _kpi_final(self, backtest: Backtest) -> dict:
        ticker = backtest.universe_snapshot[0]
        return run_backtest_kpi_only(backtest)[ticker]["lines"][0]["final"]

    def _backtest_for_symbol(
        self,
        *,
        symbol: Symbol,
        prices: list[str],
        alerts_by_offset: dict[int, str],
        signal_lines: list[dict],
        warmup_days: int = 0,
        start_offset: int = 0,
    ) -> Backtest:
        self._add_symbol_fixture(symbol=symbol, prices=prices, alerts_by_offset=alerts_by_offset)
        return Backtest.objects.create(
            name="GM Push Backtest",
            scenario=self.scenario,
            start_date=self.start + timedelta(days=start_offset),
            end_date=self.start + timedelta(days=len(prices) - 1),
            capital_total=Decimal("0"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=signal_lines,
            universe_snapshot=[symbol.ticker],
            warmup_days=warmup_days,
            close_positions_at_end=False,
            settings={},
        )

    def _count_dailybar_queries(self, func) -> int:
        with CaptureQueriesContext(connection) as ctx:
            func()
        return sum("core_dailybar" in query["sql"].lower() for query in ctx.captured_queries)

    def test_no_active_trend_filters_keeps_behavior_identical(self):
        baseline = self._create_backtest()
        same = self._create_backtest(settings={TREND_FILTER_OPERATOR_KEY: "AND"})
        self.assertEqual(self._actions(baseline), self._actions(same))

    def test_gm_push_formula_uses_sum_of_daily_returns_over_nglobal(self):
        values = {
            self.start: Decimal("100"),
            self.start + timedelta(days=1): Decimal("110"),
            self.start + timedelta(days=2): Decimal("121"),
        }
        out = compute_push_values_for_series(values, nglobal=2)
        self.assertEqual(out[self.start + timedelta(days=2)], Decimal("0.20"))

    def test_gm_push_state_crossing_and_memory(self):
        values = {
            self.start: Decimal("0.00"),
            self.start + timedelta(days=1): Decimal("0.02"),
            self.start + timedelta(days=2): Decimal("0.04"),
            self.start + timedelta(days=3): Decimal("0.01"),
            self.start + timedelta(days=4): Decimal("-0.04"),
            self.start + timedelta(days=5): Decimal("-0.02"),
        }
        states = compute_push_state_by_date(values, buy_threshold=Decimal("0.03"), sell_threshold=Decimal("-0.03"))
        self.assertEqual(states[self.start], GM_PUSH_UNKNOWN)
        self.assertEqual(states[self.start + timedelta(days=2)], GM_PUSH_POS_ACTIVE)
        self.assertEqual(states[self.start + timedelta(days=3)], GM_PUSH_POS_ACTIVE)
        self.assertEqual(states[self.start + timedelta(days=4)], GM_PUSH_NEG_ACTIVE)
        self.assertEqual(states[self.start + timedelta(days=5)], GM_PUSH_NEG_ACTIVE)

    def test_gm_push_normalization_defaults_active_entries_without_threshold_to_zero(self):
        for normalizer in (normalize_engine_gm_push_condition_entry, normalize_form_gm_push_condition_entry):
            pos_entry = normalizer({"mode": "POS"})
            self.assertEqual(pos_entry["mode"], "POS")
            self.assertEqual(pos_entry["buy_threshold"], "0")
            self.assertEqual(pos_entry["sell_threshold"], "0")
            self.assertFalse(pos_entry["explicit_threshold"])

            neg_entry = normalizer({"mode": "NEG"})
            self.assertEqual(neg_entry["mode"], "NEG")
            self.assertEqual(neg_entry["buy_threshold"], "0")
            self.assertEqual(neg_entry["sell_threshold"], "0")
            self.assertFalse(neg_entry["explicit_threshold"])

    def test_gm_buy_max_threshold_normalization_is_buy_pos_only(self):
        for normalizer in (normalize_engine_gm_condition_entry, normalize_form_gm_condition_entry):
            pos_entry = normalizer({"mode": "POS", "threshold": "0.4", "buy_max_threshold": "0.6"})
            self.assertEqual(pos_entry["threshold"], "0.4")
            self.assertEqual(pos_entry["buy_max_threshold"], "0.6")

            neg_entry = normalizer({"mode": "NEG", "threshold": "0.4", "buy_max_threshold": "0.6"})
            self.assertEqual(neg_entry["threshold"], "0.4")
            self.assertIsNone(neg_entry["buy_max_threshold"])

    def test_gm_push_buy_max_threshold_normalization_is_buy_pos_only(self):
        for normalizer in (normalize_engine_gm_push_condition_entry, normalize_form_gm_push_condition_entry):
            pos_entry = normalizer({"mode": "POS", "threshold": "0.4", "buy_max_threshold": "0.6"})
            self.assertEqual(pos_entry["buy_threshold"], "0.4")
            self.assertEqual(pos_entry["sell_threshold"], "0.4")
            self.assertEqual(pos_entry["buy_max_threshold"], "0.6")

            neg_entry = normalizer({"mode": "NEG", "threshold": "0.4", "buy_max_threshold": "0.6"})
            self.assertEqual(neg_entry["buy_threshold"], "0.4")
            self.assertEqual(neg_entry["sell_threshold"], "0.4")
            self.assertIsNone(neg_entry["buy_max_threshold"])

    def test_gm_push_normalization_uses_user_threshold_without_sign_inversion(self):
        for normalizer in (normalize_engine_gm_push_condition_entry, normalize_form_gm_push_condition_entry):
            neg_entry = normalizer({"mode": "NEG", "threshold": "0.2"})
            self.assertEqual(neg_entry["mode"], "NEG")
            self.assertEqual(neg_entry["threshold"], "0.2")
            self.assertEqual(neg_entry["buy_threshold"], "0.2")
            self.assertEqual(neg_entry["sell_threshold"], "0.2")
            self.assertTrue(neg_entry["explicit_threshold"])

            threshold_wins_entry = normalizer({
                "mode": "NEG",
                "threshold": "0.2",
                "buy_threshold": "0.2",
                "sell_threshold": "-0.2",
                "explicit_threshold": True,
            })
            self.assertEqual(threshold_wins_entry["threshold"], "0.2")
            self.assertEqual(threshold_wins_entry["buy_threshold"], "0.2")
            self.assertEqual(threshold_wins_entry["sell_threshold"], "0.2")
            self.assertTrue(threshold_wins_entry["explicit_threshold"])

            legacy_entry = normalizer({"mode": "NEG", "sell_threshold": "-0.2", "explicit_threshold": True})
            self.assertIsNone(legacy_entry["threshold"])
            self.assertEqual(legacy_entry["buy_threshold"], "-0.2")
            self.assertEqual(legacy_entry["sell_threshold"], "-0.2")
            self.assertTrue(legacy_entry["explicit_threshold"])

            positive_sell_entry = normalizer({"mode": "NEG", "sell_threshold": "0.2", "explicit_threshold": True})
            self.assertIsNone(positive_sell_entry["threshold"])
            self.assertEqual(positive_sell_entry["buy_threshold"], "0.2")
            self.assertEqual(positive_sell_entry["sell_threshold"], "0.2")
            self.assertTrue(positive_sell_entry["explicit_threshold"])

            pos_entry = normalizer({"mode": "POS", "threshold": "0.2"})
            self.assertEqual(pos_entry["threshold"], "0.2")
            self.assertEqual(pos_entry["buy_threshold"], "0.2")
            self.assertEqual(pos_entry["sell_threshold"], "0.2")

    def test_gm_push_ui_serializes_user_threshold_magnitude_only(self):
        for relative_path in (
            "templates/backtest_create.html",
            "templates/backtest_edit.html",
            "templates/game_scenario_form.html",
        ):
            source = Path(relative_path).read_text()
            self.assertIn("threshold: threshold || null", source)
            self.assertIn("buy_max_threshold", source)
            self.assertIn("Seuil haut optionnel", source)
            self.assertNotIn("sell_threshold: threshold ? String(-Math.abs(Number(threshold)))", source)

    def test_gm_ui_hides_regime_terms_from_main_choices(self):
        forbidden_terms = ("Positif", "Négatif", "Neutre", "positif", "négatif", "neutre")
        for relative_path in (
            "templates/backtest_create.html",
            "templates/backtest_edit.html",
            "templates/game_scenario_form.html",
        ):
            source = Path(relative_path).read_text()
            choices_block = source.split("const MARKET_CONDITION_CHOICES = [", 1)[1].split("];", 1)[0]
            for term in forbidden_terms:
                self.assertNotIn(term, choices_block)
            self.assertIn("Au-dessus du seuil", choices_block)
            self.assertIn("Sous le seuil", choices_block)
            self.assertIn("Autour du seuil", choices_block)
            self.assertNotIn("Option avancée", choices_block)

    def test_gm_buy_max_threshold_ui_is_buy_only(self):
        for relative_path in (
            "templates/backtest_create.html",
            "templates/backtest_edit.html",
            "templates/game_scenario_form.html",
        ):
            source = Path(relative_path).read_text()
            self.assertIn("buy_market_gm_current_max_threshold", source)
            self.assertIn("gm_push_buy_current_max_threshold", source)
            self.assertNotIn("sell_market_gm_current_max_threshold", source)
            self.assertNotIn("gm_push_sell_current_max_threshold", source)

    def test_gm_push_state_crosses_zero_with_default_thresholds(self):
        positive_values = {
            self.start: Decimal("-0.01"),
            self.start + timedelta(days=1): Decimal("0.02"),
        }
        positive_states = compute_push_state_by_date(positive_values, buy_threshold=Decimal("0"), sell_threshold=Decimal("0"))
        self.assertEqual(positive_states[self.start], GM_PUSH_UNKNOWN)
        self.assertEqual(positive_states[self.start + timedelta(days=1)], GM_PUSH_POS_ACTIVE)

        negative_values = {
            self.start: Decimal("0.01"),
            self.start + timedelta(days=1): Decimal("-0.02"),
        }
        negative_states = compute_push_state_by_date(negative_values, buy_threshold=Decimal("0"), sell_threshold=Decimal("0"))
        self.assertEqual(negative_states[self.start], GM_PUSH_UNKNOWN)
        self.assertEqual(negative_states[self.start + timedelta(days=1)], GM_PUSH_NEG_ACTIVE)

    def test_gm_push_negative_state_crosses_down_below_positive_threshold(self):
        values = {
            self.start: Decimal("0.25"),
            self.start + timedelta(days=1): Decimal("0.15"),
            self.start + timedelta(days=2): Decimal("-0.25"),
        }
        states = compute_push_state_by_date(values, buy_threshold=Decimal("0.2"), sell_threshold=Decimal("0.2"))
        self.assertEqual(states[self.start], GM_PUSH_UNKNOWN)
        self.assertEqual(states[self.start + timedelta(days=1)], GM_PUSH_NEG_ACTIVE)
        self.assertEqual(states[self.start + timedelta(days=2)], GM_PUSH_NEG_ACTIVE)

    def test_gm_push_current_averages_symbol_push_values(self):
        values = compute_current_push_values_by_date(
            {
                "AAA": {
                    self.start: Decimal("100"),
                    self.start + timedelta(days=1): Decimal("110"),
                    self.start + timedelta(days=2): Decimal("121"),
                },
                "BBB": {
                    self.start: Decimal("100"),
                    self.start + timedelta(days=1): Decimal("100"),
                    self.start + timedelta(days=2): Decimal("110"),
                },
            },
            nglobal=2,
        )
        self.assertEqual(values[self.start + timedelta(days=2)], Decimal("0.15"))

    def test_gm_push_buy_delays_authorization_until_latched_state_is_positive(self):
        symbol = self._fresh_symbol()
        bt = self._backtest_for_symbol(
            symbol=symbol,
            prices=["10", "10.1", "10.2", "10.6"],
            alerts_by_offset={1: "Af"},
            signal_lines=[{
                "trading_model": "PROGRESSIVE_AUTO_SELL",
                "buy": ["Af"],
                "sell": [],
                "gm_push_buy_conditions": {
                    "operator": "AND",
                    "current": {"mode": "POS", "buy_threshold": "0.03", "sell_threshold": "-0.03", "explicit_threshold": True},
                },
            }],
        )
        daily = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]["daily"]
        self.assertEqual([row.get("action") for row in daily], [None, None, None, "BUY"])
        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(int(kpi_final["N"]), 0)

    def test_gm_push_buy_max_threshold_blocks_temporarily_without_resetting_push_memory(self):
        symbol = self._fresh_symbol()
        bt = self._backtest_for_symbol(
            symbol=symbol,
            prices=["10", "10.2", "10.71", "11.03"],
            alerts_by_offset={1: "Af"},
            signal_lines=[{
                "trading_model": "PROGRESSIVE_AUTO_SELL",
                "buy": ["Af"],
                "sell": [],
                "gm_push_buy_conditions": {
                    "operator": "AND",
                    "current": {
                        "mode": "POS",
                        "buy_threshold": "0.03",
                        "sell_threshold": "-0.03",
                        "buy_max_threshold": "0.04",
                        "explicit_threshold": True,
                    },
                },
            }],
        )
        line = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]
        daily = line["daily"]
        self.assertEqual([row.get("action") for row in daily], [None, None, None, "BUY"])
        self.assertTrue(daily[2]["buy_blocked_by_gm_buy_max"])
        self.assertEqual(daily[2]["buy_blocked_message"], "Achat bloqué : GM au-dessus du seuil haut d’achat.")

    def test_gm_push_market_buy_without_explicit_threshold_does_not_block_backtest_or_kpi_path(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "101", "low": "99", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "99", "high": "100", "low": "98", "close": "99"},
                {"date": self.start + timedelta(days=2), "open": "101", "high": "102", "low": "100", "close": "101"},
            ],
        )
        symbol = self._fresh_symbol()
        signal_lines = [{
            "trading_model": "PROGRESSIVE_AUTO_SELL",
            "buy": ["Af"],
            "sell": [],
            "gm_push_buy_conditions": {
                "operator": "AND",
                "market": {"mode": "POS", "explicit_threshold": False},
            },
        }]
        bt = self._backtest_for_symbol(
            symbol=symbol,
            prices=["10", "10", "10.5"],
            alerts_by_offset={1: "Af"},
            signal_lines=signal_lines,
        )

        line = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]
        self.assertEqual([row.get("action") for row in line["daily"]], [None, None, "BUY"])

        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(int(kpi_final["N"]), 0)

    def test_gm_push_sell_market_exit_closes_open_position_and_matches_kpi_path(self):
        symbol = self._fresh_symbol()
        bt = self._backtest_for_symbol(
            symbol=symbol,
            prices=["10", "10.1", "10.2", "9"],
            alerts_by_offset={1: "Af"},
            signal_lines=[{
                "trading_model": "PROGRESSIVE_AUTO_SELL",
                "buy": ["Af"],
                "sell": [],
                "gm_push_sell_market_exit_conditions": {
                    "operator": "AND",
                    "current": {"mode": "NEG", "buy_threshold": "0.03", "sell_threshold": "-0.03", "explicit_threshold": True},
                },
            }],
        )
        line = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]
        actions = [row.get("action") for row in line["daily"]]
        self.assertEqual(actions, [None, "BUY", None, "SELL"])
        self.assertIn("GM_PUSH_MARKET_EXIT", line["daily"][3]["action_reason"])
        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(int(kpi_final["N"]), 1)

    def test_gm_push_sector_sell_market_exit_accepts_positive_negative_threshold_and_matches_kpi_path(self):
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "101", "low": "99", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "100", "high": "101", "low": "99", "close": "100"},
                {"date": self.start + timedelta(days=2), "open": "125", "high": "126", "low": "124", "close": "125"},
                {"date": self.start + timedelta(days=3), "open": "80", "high": "81", "low": "79", "close": "80"},
            ],
        )
        symbol = self._fresh_symbol(sector="Technology")
        signal_lines = [{
            "trading_model": "PROGRESSIVE_AUTO_SELL",
            "buy": ["Af"],
            "sell": [],
            "gm_push_sell_market_exit_conditions": {
                "operator": "AND",
                "sector": {"mode": "NEG", "threshold": "0.2", "explicit_threshold": True},
            },
        }]
        bt = self._backtest_for_symbol(
            symbol=symbol,
            prices=["10", "10.1", "10.2", "9"],
            alerts_by_offset={1: "Af"},
            signal_lines=signal_lines,
        )

        line = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]
        actions = [row.get("action") for row in line["daily"]]
        self.assertEqual(actions, [None, "BUY", None, "SELL"])
        self.assertIn("GM_PUSH_MARKET_EXIT", line["daily"][3]["action_reason"])

        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(int(kpi_final["N"]), 1)

    def test_gm_push_warmup_restores_state_before_backtest_start(self):
        symbol = self._fresh_symbol()
        bt = self._backtest_for_symbol(
            symbol=symbol,
            prices=["10", "10.1", "10.2", "10.6"],
            alerts_by_offset={3: "Af"},
            start_offset=3,
            warmup_days=3,
            signal_lines=[{
                "trading_model": "PROGRESSIVE_AUTO_SELL",
                "buy": ["Af"],
                "sell": [],
                "gm_push_buy_conditions": {
                    "operator": "AND",
                    "current": {"mode": "POS", "buy_threshold": "0.03", "sell_threshold": "-0.03", "explicit_threshold": True},
                },
            }],
        )
        daily = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]["daily"]
        self.assertEqual([row.get("action") for row in daily], ["BUY"])

    def test_legacy_buy_gm_filter_remains_unchanged(self):
        signal_lines = [{
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af", "SPVa_basse"],
            "buy_logic": "AND",
            "sell": [],
            "buy_gm_filter": "GM_POS",
        }]
        baseline = self._create_backtest(signal_lines=copy.deepcopy(signal_lines))
        with_trend_defaults = self._create_backtest(
            signal_lines=copy.deepcopy(signal_lines),
            settings={TREND_FILTER_OPERATOR_KEY: "AND"},
        )
        self.assertEqual(self._actions(baseline), self._actions(with_trend_defaults))

    def test_legacy_sell_gm_filter_remains_unchanged(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "102", "low": "98", "close": "101"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "92", "low": "88", "close": "89"},
            ],
        )
        signal_lines = [{
            "trading_model": "LEGACY_DAILY",
            "buy": ["Af"],
            "sell": ["Bf"],
            "buy_logic": "AND",
            "sell_logic": "OR",
            "sell_gm_filter": "GM_NEG",
        }]
        baseline = self._create_backtest(signal_lines=copy.deepcopy(signal_lines), alerts_by_offset={0: "Af", 1: "Bf"})
        same = self._create_backtest(
            signal_lines=copy.deepcopy(signal_lines),
            alerts_by_offset={0: "Af", 1: "Bf"},
            settings={TREND_FILTER_OPERATOR_KEY: "AND"},
        )
        self.assertEqual(self._actions(baseline), self._actions(same))

    def test_legacy_daily_mode_remains_unchanged_without_new_filters(self):
        signal_lines = [{
            "trading_model": "LEGACY_DAILY",
            "buy": ["Af"],
            "sell": ["Bf"],
            "buy_logic": "AND",
            "sell_logic": "OR",
        }]
        baseline = self._create_backtest(signal_lines=copy.deepcopy(signal_lines), alerts_by_offset={0: "Af", 2: "Bf"})
        same = self._create_backtest(
            signal_lines=copy.deepcopy(signal_lines),
            alerts_by_offset={0: "Af", 2: "Bf"},
            settings={TREND_FILTER_OPERATOR_KEY: "OR"},
        )
        self.assertEqual(self._actions(baseline), self._actions(same))

    def test_benchmark_formula_uses_same_p_convention_as_daily_metric(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "10", "high": "20", "low": "0", "close": "10"},
                {"date": self.start + timedelta(days=1), "open": "20", "high": "40", "low": "0", "close": "20"},
            ],
        )
        cache = preload_benchmark_price_cache(
            symbols=[self.spy],
            scenario=self.scenario,
            start_date=self.start,
            end_date=self.start + timedelta(days=1),
        )
        self.assertEqual(cache["SPY"]["values"], [Decimal("10"), Decimal("20")])

    def test_classification_uses_same_thresholds_as_existing_gm(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "10", "high": "10", "low": "10", "close": "10"},
                {"date": self.start + timedelta(days=1), "open": "10.005", "high": "10.005", "low": "10.005", "close": "10.005"},
                {"date": self.start + timedelta(days=2), "open": "9.99", "high": "9.99", "low": "9.99", "close": "9.99"},
            ],
        )
        cache = preload_benchmark_price_cache(
            symbols=[self.spy],
            scenario=self.scenario,
            start_date=self.start,
            end_date=self.start + timedelta(days=2),
        )
        neutral = evaluate_trend_filters_for_symbol(
            symbol=self.symbol,
            settings={TREND_FILTER_GM_MARKET_KEY: "GM_NEU"},
            as_of=self.start + timedelta(days=1),
            nglobal=1,
            gm_current_regime=None,
            benchmark_cache_by_ticker=cache,
        )
        negative = evaluate_trend_filters_for_symbol(
            symbol=self.symbol,
            settings={TREND_FILTER_GM_MARKET_KEY: "GM_NEG"},
            as_of=self.start + timedelta(days=2),
            nglobal=1,
            gm_current_regime=None,
            benchmark_cache_by_ticker=cache,
        )
        self.assertTrue(neutral["passed"])
        self.assertTrue(negative["passed"])

    def test_gm_market_passes_when_benchmark_trend_matches(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_line_market_conditions_require_matching_local_market_context(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_NEG",
                "buy_market_operator": "AND",
            }],
        )
        self.assertNotIn("BUY", {action for action in self._actions(bt) if action})

    def test_line_market_conditions_and_operator_requires_all_selected_conditions(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
                "buy_market_gm_sector": "GM_POS",
                "buy_market_operator": "AND",
            }],
        )
        self.assertNotIn("BUY", {action for action in self._actions(bt) if action})

    def test_line_market_conditions_or_operator_allows_one_matching_condition(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
                "buy_market_gm_sector": "GM_POS",
                "buy_market_operator": "OR",
            }],
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_line_market_gm_market_authorizes_buy_after_signal_is_latched(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
                {"date": self.start + timedelta(days=2), "open": "99", "high": "99", "low": "99", "close": "99"},
                {"date": self.start + timedelta(days=3), "open": "105", "high": "105", "low": "105", "close": "105"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
                "buy_market_operator": "AND",
            }],
            prices=["10", "20", "40", "80"],
            alerts_by_offset={1: "Af"},
        )

        daily = self._daily(bt)
        self.assertIsNone(daily[1]["action"])
        self.assertEqual(daily[2]["action"], "BUY")

    def test_line_market_gm_current_authorizes_buy_after_signal_is_latched(self):
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_current": "GM_POS",
                "buy_market_operator": "AND",
            }],
            prices=["10", "20", "40", "80"],
            alerts_by_offset={1: "Af"},
        )
        gm_regimes = {
            self.start + timedelta(days=1): "GM_NEG",
            self.start + timedelta(days=2): "GM_POS",
            self.start + timedelta(days=3): "GM_POS",
        }

        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            ticker = bt.universe_snapshot[0]
            daily = run_backtest(bt).results["tickers"][ticker]["lines"][0]["daily"]

        self.assertIsNone(daily[1]["action"])
        self.assertEqual(daily[2]["action"], "BUY")

    def test_line_market_gm_sector_authorizes_buy_after_signal_is_latched(self):
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
                {"date": self.start + timedelta(days=2), "open": "99", "high": "99", "low": "99", "close": "99"},
                {"date": self.start + timedelta(days=3), "open": "105", "high": "105", "low": "105", "close": "105"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_sector": "GM_POS",
                "buy_market_operator": "AND",
            }],
            prices=["10", "20", "40", "80"],
            alerts_by_offset={1: "Af"},
        )

        daily = self._daily(bt)
        self.assertIsNone(daily[1]["action"])
        self.assertEqual(daily[2]["action"], "BUY")

    def test_line_market_delayed_authorization_matches_kpi_only_path(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
                {"date": self.start + timedelta(days=2), "open": "99", "high": "99", "low": "99", "close": "99"},
                {"date": self.start + timedelta(days=3), "open": "105", "high": "105", "low": "105", "close": "105"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
                "buy_market_operator": "AND",
            }],
            prices=["10", "20", "40", "80"],
            alerts_by_offset={1: "Af"},
            close_positions_at_end=True,
        )

        final = self._kpi_final(bt)
        self.assertEqual(final["N"], 1)
        self.assertEqual(Decimal(final["BT"]), Decimal("1"))

    def test_line_market_conditions_do_not_trigger_sell_by_themselves(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
                "buy_market_operator": "AND",
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={1: "Af"},
        )
        actions = [action for action in self._actions(bt) if action]
        self.assertIn("BUY", actions)
        self.assertNotIn("SELL", actions)

    def test_gm_sell_market_exit_closes_open_position_without_ticker_sell(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG"},
                },
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={0: "Af"},
        )

        daily = self._daily(bt)
        self.assertEqual(daily[0]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertIn("Protection marché GM", daily[2]["action_reason"])
        self.assertEqual(self._kpi_trade_count(bt), 1)

    def test_gm_sell_market_exit_not_configured_keeps_existing_behavior(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={0: "Af"},
        )
        actions = [action for action in self._actions(bt) if action]
        self.assertEqual(actions, ["BUY"])

    def test_gm_sell_market_exit_configured_but_not_reached_does_not_sell(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "120", "high": "120", "low": "120", "close": "120"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG"},
                },
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={0: "Af"},
        )
        actions = [action for action in self._actions(bt) if action]
        self.assertEqual(actions, ["BUY"])

    def test_gm_sell_market_exit_or_operator_allows_one_matching_condition(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "120", "high": "120", "low": "120", "close": "120"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "OR",
                    "market": {"mode": "GM_NEG"},
                    "sector": {"mode": "GM_NEG"},
                },
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={0: "Af"},
        )
        self.assertEqual([action for action in self._actions(bt) if action], ["BUY", "SELL"])

    def test_gm_sell_market_exit_and_operator_requires_all_conditions(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "120", "high": "120", "low": "120", "close": "120"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG"},
                    "sector": {"mode": "GM_NEG"},
                },
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={0: "Af"},
        )
        self.assertEqual([action for action in self._actions(bt) if action], ["BUY"])

    def test_gm_sell_market_exit_never_sells_without_open_position(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG"},
                },
            }],
            prices=["10", "11"],
            alerts_by_offset={0: "SPVv"},
        )
        self.assertEqual([action for action in self._actions(bt) if action], [])

    def test_gm_sell_market_exit_explicit_threshold_uses_configured_value(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "105.60", "high": "105.60", "low": "105.60", "close": "105.60"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG", "threshold": "-0.03", "explicit_threshold": True},
                },
            }],
            prices=["10", "11", "12"],
            alerts_by_offset={0: "Af"},
        )
        self.assertEqual([action for action in self._actions(bt) if action], ["BUY", "SELL"])

    def test_gm_buy_explicit_threshold_uses_configured_value(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "103", "high": "103", "low": "103", "close": "103"},
                {"date": self.start + timedelta(days=2), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_buy_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_POS", "threshold": "0.05", "explicit_threshold": True},
                },
            }],
            prices=["10", "20", "40"],
            alerts_by_offset={1: "Af"},
        )
        daily = self._daily(bt)
        self.assertIsNone(daily[1]["action"])
        self.assertEqual(daily[2]["action"], "BUY")

    def test_gm_current_buy_max_threshold_blocks_then_reauthorizes_without_resetting_buy_memory(self):
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_buy_conditions": {
                    "operator": "AND",
                    "current": {"mode": "GM_POS", "threshold": "0.4", "buy_max_threshold": "0.6", "explicit_threshold": True},
                },
            }],
            prices=["10", "20", "40", "80"],
            alerts_by_offset={1: "Af"},
        )
        gm_values = {
            self.start + timedelta(days=1): Decimal("0.3"),
            self.start + timedelta(days=2): Decimal("0.65"),
            self.start + timedelta(days=3): Decimal("0.55"),
        }
        with patch("core.services.backtesting.engine._build_global_momentum_values_from_ticker_data", return_value=gm_values):
            ticker = bt.universe_snapshot[0]
            line = run_backtest(bt).results["tickers"][ticker]["lines"][0]
        daily = line["daily"]
        self.assertIsNone(daily[1]["action"])
        self.assertIsNone(daily[2]["action"])
        self.assertTrue(daily[2]["buy_blocked_by_gm_buy_max"])
        self.assertEqual(daily[2]["buy_blocked_message"], "Achat bloqué : GM au-dessus du seuil haut d’achat.")
        self.assertEqual(daily[3]["action"], "BUY")
        self.assertNotIn("SELL", daily[3]["action"])

        kpi_bt = self._create_backtest(
            signal_lines=copy.deepcopy(bt.signal_lines),
            prices=["10", "20", "40", "80"],
            alerts_by_offset={1: "Af"},
            close_positions_at_end=True,
        )
        with patch("core.services.backtesting.engine._build_global_momentum_values_from_ticker_data", return_value=gm_values):
            self.assertEqual(self._kpi_trade_count(kpi_bt), 1)

    def test_gm_market_buy_max_threshold_blocks_then_allows_again(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "165", "high": "165", "low": "165", "close": "165"},
                {"date": self.start + timedelta(days=2), "open": "255", "high": "255", "low": "255", "close": "255"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_buy_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_POS", "threshold": "0.4", "buy_max_threshold": "0.6", "explicit_threshold": True},
                },
            }],
            prices=["10", "20", "40"],
            alerts_by_offset={1: "Af"},
        )
        daily = self._daily(bt)
        self.assertIsNone(daily[1]["action"])
        self.assertTrue(daily[1]["buy_blocked_by_gm_buy_max"])
        self.assertEqual(daily[2]["action"], "BUY")

    def test_gm_sector_buy_max_threshold_blocks_then_allows_again(self):
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "165", "high": "165", "low": "165", "close": "165"},
                {"date": self.start + timedelta(days=2), "open": "255", "high": "255", "low": "255", "close": "255"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_buy_conditions": {
                    "operator": "AND",
                    "sector": {"mode": "GM_POS", "threshold": "0.4", "buy_max_threshold": "0.6", "explicit_threshold": True},
                },
            }],
            prices=["10", "20", "40"],
            alerts_by_offset={1: "Af"},
        )
        daily = self._daily(bt)
        self.assertIsNone(daily[1]["action"])
        self.assertTrue(daily[1]["buy_blocked_by_gm_buy_max"])
        self.assertEqual(daily[2]["action"], "BUY")

    def test_gm_buy_max_threshold_respects_or_and_operators(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "150", "high": "150", "low": "150", "close": "150"},
            ],
        )
        base_line = {
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af"],
            "buy_logic": "AND",
            "sell": [],
            "gm_buy_conditions": {
                "current": {"mode": "GM_POS", "threshold": "0.4", "buy_max_threshold": "0.6", "explicit_threshold": True},
                "market": {"mode": "GM_POS", "threshold": "0.4", "buy_max_threshold": "0.6", "explicit_threshold": True},
            },
        }
        gm_values = {self.start + timedelta(days=1): Decimal("0.65")}
        or_line = copy.deepcopy(base_line)
        or_line["gm_buy_conditions"]["operator"] = "OR"
        or_bt = self._create_backtest(signal_lines=[or_line], prices=["10", "20"], alerts_by_offset={1: "Af"})
        with patch("core.services.backtesting.engine._build_global_momentum_values_from_ticker_data", return_value=gm_values):
            self.assertEqual(self._daily(or_bt)[1]["action"], "BUY")

        and_line = copy.deepcopy(base_line)
        and_line["gm_buy_conditions"]["operator"] = "AND"
        and_bt = self._create_backtest(signal_lines=[and_line], prices=["10", "20"], alerts_by_offset={1: "Af"})
        with patch("core.services.backtesting.engine._build_global_momentum_values_from_ticker_data", return_value=gm_values):
            daily = self._daily(and_bt)
        self.assertIsNone(daily[1]["action"])
        self.assertTrue(daily[1]["buy_blocked_by_gm_buy_max"])

    def test_gm_sell_market_exit_ignores_buy_max_threshold(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_POS", "threshold": "0.05", "buy_max_threshold": "0.06", "explicit_threshold": True},
                },
            }],
            prices=["10", "11"],
            alerts_by_offset={0: "Af"},
        )
        daily = self._daily(bt)
        self.assertEqual(daily[0]["action"], "BUY")
        self.assertEqual(daily[1]["action"], "SELL")

    def test_legacy_line_market_condition_keeps_neutral_band_threshold(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "100.05", "high": "100.05", "low": "100.05", "close": "100.05"},
            ],
        )
        bt = self._create_backtest(
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
            }],
            prices=["10", "20"],
            alerts_by_offset={1: "Af"},
        )
        self.assertNotIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_global_filter_is_ignored_when_line_market_condition_passes(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(
            settings={TREND_FILTER_GM_SECTOR_KEY: "GM_POS"},
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "sell": [],
                "buy_market_gm_market": "GM_POS",
                "buy_market_operator": "AND",
            }],
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_gm_current_uses_existing_gm_regime_without_changing_legacy_computation(self):
        bt = self._create_backtest(settings={TREND_FILTER_GM_CURRENT_KEY: "GM_POS"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_trend_gm_current_gm_pos_does_not_block_buy_on_gm_neg_day(self):
        bt = self._create_backtest(
            settings={TREND_FILTER_GM_CURRENT_KEY: "GM_POS"},
            prices=["10", "9", "8"],
            alerts_by_offset={0: "Af", 1: "SPVa_basse"},
            close_positions_at_end=True,
        )

        actions = self._actions(bt)
        self.assertEqual(actions[1], "BUY")
        self.assertEqual(self._kpi_trade_count(bt), 1)

    def test_legacy_trend_gm_current_is_ignored_at_runtime(self):
        bt = self._create_backtest(settings={TREND_FILTER_GM_CURRENT_KEY: "GM_NEG"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_form_normalization_moves_legacy_buy_gm_filter_to_line_market_condition(self):
        legacy = self._create_backtest(
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "sell": ["Bf"],
                "buy_gm_filter": "GM_POS",
                "sell_gm_filter": "GM_NEG",
            }],
            settings={},
        )
        form = BacktestForm(
            data={
                "name": legacy.name,
                "description": legacy.description,
                "scenario": str(self.scenario.id),
                "start_date": legacy.start_date,
                "end_date": legacy.end_date,
                "capital_total": legacy.capital_total,
                "capital_per_ticker": legacy.capital_per_ticker,
                "capital_mode": legacy.capital_mode,
                "ratio_threshold": legacy.ratio_threshold,
                "include_all_tickers": "on",
                "signal_lines": json.dumps(legacy.signal_lines),
                "warmup_days": legacy.warmup_days,
                "close_positions_at_end": "",
            },
            instance=legacy,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertNotIn(TREND_FILTER_GM_CURRENT_KEY, saved.settings)
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")
        self.assertEqual(saved.signal_lines[0]["buy_market_gm_current"], "GM_POS")
        self.assertEqual(saved.signal_lines[0]["sell_gm_filter"], "IGNORE")
        self.assertIn("BUY", {action for action in self._actions(saved) if action})

    def test_legacy_buy_gm_filter_suppresses_duplicate_trend_gm_current_in_full_backtest(self):
        signal_lines = [{
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af", "SPVa_basse"],
            "buy_logic": "AND",
            "sell": [],
            "buy_gm_filter": "GM_POS",
        }]
        bt = self._create_backtest(
            signal_lines=signal_lines,
            settings={TREND_FILTER_GM_CURRENT_KEY: "GM_NEG"},
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_buy_gm_filter_suppresses_duplicate_trend_gm_current_in_kpi_only_path(self):
        signal_lines = [{
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af", "SPVa_basse"],
            "buy_logic": "AND",
            "sell": [],
            "buy_gm_filter": "GM_POS",
        }]
        bt = self._create_backtest(
            signal_lines=signal_lines,
            settings={TREND_FILTER_GM_CURRENT_KEY: "GM_NEG"},
            close_positions_at_end=True,
        )
        self.assertEqual(self._kpi_trade_count(bt), 1)

    def test_legacy_global_gm_market_is_ignored_with_legacy_buy_gm_filter(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        signal_lines = [{
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af", "SPVa_basse"],
            "buy_logic": "AND",
            "sell": [],
            "buy_gm_filter": "GM_POS",
        }]
        bt = self._create_backtest(
            signal_lines=signal_lines,
            settings={TREND_FILTER_GM_CURRENT_KEY: "GM_NEG", TREND_FILTER_GM_MARKET_KEY: "GM_POS"},
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_global_gm_sector_is_ignored_with_legacy_buy_gm_filter(self):
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        signal_lines = [{
            "trading_model": "LATCH_STATEFUL",
            "buy": ["Af", "SPVa_basse"],
            "buy_logic": "AND",
            "sell": [],
            "buy_gm_filter": "GM_POS",
        }]
        bt = self._create_backtest(
            signal_lines=signal_lines,
            settings={TREND_FILTER_GM_CURRENT_KEY: "GM_NEG", TREND_FILTER_GM_SECTOR_KEY: "GM_POS"},
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_gm_market_does_not_block_when_benchmark_trend_does_not_match(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_gm_sector_passes_when_sector_trend_matches(self):
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_SECTOR_KEY: "GM_POS"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_gm_sector_does_not_block_when_sector_trend_does_not_match(self):
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_SECTOR_KEY: "GM_POS"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_and_operator_does_not_block_buy(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(settings={
            TREND_FILTER_OPERATOR_KEY: "AND",
            TREND_FILTER_GM_MARKET_KEY: "GM_POS",
            TREND_FILTER_GM_SECTOR_KEY: "GM_POS",
        })
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_or_operator_allows_one_pass_one_fail(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
            ],
        )
        bt = self._create_backtest(settings={
            TREND_FILTER_OPERATOR_KEY: "OR",
            TREND_FILTER_GM_MARKET_KEY: "GM_POS",
            TREND_FILTER_GM_SECTOR_KEY: "GM_POS",
        })
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_or_operator_allows_one_pass_one_missing(self):
        symbol = Symbol.objects.create(ticker="NOUS", exchange="", country="", sector="Technology", active=True)
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            symbol=symbol,
            settings={
                TREND_FILTER_OPERATOR_KEY: "OR",
                TREND_FILTER_GM_MARKET_KEY: "GM_POS",
                TREND_FILTER_GM_SECTOR_KEY: "GM_POS",
            },
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_or_operator_does_not_block_when_all_filters_fail_or_missing(self):
        symbol = Symbol.objects.create(ticker="FAIL", exchange="", country="", sector="", active=True)
        bt = self._create_backtest(
            symbol=symbol,
            settings={
                TREND_FILTER_OPERATOR_KEY: "OR",
                TREND_FILTER_GM_MARKET_KEY: "GM_POS",
                TREND_FILTER_GM_SECTOR_KEY: "GM_POS",
            },
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_missing_market_mapping_does_not_block_buy(self):
        symbol = Symbol.objects.create(ticker="INTL", exchange="", country="", sector="Technology", active=True)
        self._add_benchmark_fixture(
            self.xlk,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            symbol=symbol,
            settings={
                TREND_FILTER_OPERATOR_KEY: "AND",
                TREND_FILTER_GM_MARKET_KEY: "GM_POS",
                TREND_FILTER_GM_SECTOR_KEY: "GM_POS",
            },
        )
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_missing_benchmark_data_and_insufficient_lookback_do_not_block(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"})
        self.assertIn("BUY", {action for action in self._actions(bt) if action})

    def test_uses_latest_available_observation_without_future_leakage(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=2), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"})
        actions = self._actions(bt)
        self.assertEqual(actions[1], "BUY")

    def test_trend_filters_are_buy_only_and_do_not_force_sell(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
                {"date": self.start + timedelta(days=2), "open": "90", "high": "90", "low": "90", "close": "90"},
                {"date": self.start + timedelta(days=3), "open": "80", "high": "80", "low": "80", "close": "80"},
            ],
        )
        bt = self._create_backtest(
            settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"},
            alerts_by_offset={0: "Af", 1: "SPVa_basse"},
            close_positions_at_end=False,
        )
        actions = self._actions(bt)
        self.assertIn("BUY", actions)
        self.assertNotIn("SELL", {action for action in actions if action})

    def test_ignored_legacy_trend_filters_do_not_reset_latch_state(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "90", "high": "90", "low": "90", "close": "90"},
                {"date": self.start + timedelta(days=2), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"},
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["A1", "C1"],
                "buy_logic": "AND",
                "sell": [],
            }],
            prices=["10", "10", "10"],
            alerts_by_offset={0: "A1", 1: "C1"},
            close_positions_at_end=False,
        )
        actions = self._actions(bt)
        self.assertEqual(actions[1], "BUY")

    def test_price_and_market_cap_filters_remain_and_gates_with_trend_filters(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            settings={
                TREND_FILTER_GM_MARKET_KEY: "GM_POS",
                "min_price": "100",
                "market_cap_min": "1000",
            },
        )
        HistoricalMarketCap.objects.create(
            symbol=bt._test_symbol,
            date=self.start + timedelta(days=1),
            market_cap=Decimal("2000"),
            provider="eodhd",
        )
        self.assertNotIn("BUY", {action for action in self._actions(bt) if action})

    def test_legacy_trend_filters_are_ignored_in_kpi_only_path(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(
            settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"},
            close_positions_at_end=True,
        )
        self.assertEqual(self._kpi_trade_count(bt), 1)

    @patch("core.services.provider_twelvedata.TwelveDataClient.time_series_daily")
    @patch("core.services.provider_eodhd.EODHDClient.fetch_historical_market_cap")
    def test_no_provider_calls_are_made_during_simulation(self, market_cap_mock, twelvedata_mock):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start, "open": "100", "high": "100", "low": "100", "close": "100"},
                {"date": self.start + timedelta(days=1), "open": "110", "high": "110", "low": "110", "close": "110"},
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"})
        run_backtest(bt)
        run_backtest_kpi_only(bt)
        market_cap_mock.assert_not_called()
        twelvedata_mock.assert_not_called()

    def test_legacy_trend_filter_does_not_preload_benchmark_bars(self):
        self._add_benchmark_fixture(
            self.spy,
            rows=[
                {"date": self.start + timedelta(days=i), "open": str(100 + i), "high": str(100 + i), "low": str(100 + i), "close": str(100 + i)}
                for i in range(5)
            ],
        )
        bt = self._create_backtest(settings={TREND_FILTER_GM_MARKET_KEY: "GM_POS"}, prices=["10", "11", "12", "13", "14"])
        query_count = self._count_dailybar_queries(lambda: run_backtest(bt))
        self.assertEqual(query_count, 1)

    def test_no_benchmark_preload_when_no_trend_filter_is_active(self):
        bt = self._create_backtest()
        query_count = self._count_dailybar_queries(lambda: run_backtest(bt))
        self.assertEqual(query_count, 1)
