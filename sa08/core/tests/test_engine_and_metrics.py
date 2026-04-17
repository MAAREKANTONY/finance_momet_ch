from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.db import connection
from django.test import TestCase
from django.test.utils import CaptureQueriesContext

from core.models import Alert, Backtest, DailyBar, DailyMetric, GameScenario, Scenario, Symbol
from core.services.backtesting.engine import (
    _apply_signal_state_transitions,
    _match_line_with_global_filter,
    _update_and_latched_states,
    run_backtest,
    run_backtest_kpi_only,
)
from core.services.calculations import compute_for_symbol_scenario
from core.services.calculations_fast import compute_full_for_symbol_scenario
from core.services.game_scenarios.runner import run_game_scenario_now
from core.services.global_momentum import (
    build_global_momentum_regime_by_date,
    compute_global_momentum_values_by_date,
)
from core.tasks import _enrich_alerts_with_global_momentum


class EngineAndMetricsRegressionTests(TestCase):
    def setUp(self):
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(
            name="Scenario Test",
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
            nglobal=2,
            history_years=2,
        )

    def _create_bars_for_symbol(self, symbol: Symbol, closes: list[str], *, start: date | None = None):
        start = start or date(2024, 1, 1)
        bars = []
        for idx, close in enumerate(closes):
            d = start + timedelta(days=idx)
            c = Decimal(close)
            bars.append(
                DailyBar(
                    symbol=symbol,
                    date=d,
                    open=c,
                    high=c + Decimal("1"),
                    low=c - Decimal("1"),
                    close=c,
                    volume=1000 + idx,
                )
            )
        DailyBar.objects.bulk_create(bars)
        return [b.date for b in bars]


    def test_backtest_computes_pnl_amount_kpis_globally_and_per_line(self):
        start = date(2024, 1, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "12", "15", "11"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "12", "15", "11"])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
        ])

        bt = Backtest.objects.create(
            name="PnL Test",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        final = result["tickers"][self.symbol.ticker]["lines"][0]["final"]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(Decimal(final["PNL_AMOUNT"]), Decimal("50"))
        self.assertEqual(Decimal(final["TOTAL_GAIN_AMOUNT"]), Decimal("50"))
        self.assertEqual(Decimal(final["TOTAL_LOSS_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(final["AVG_TRADE_AMOUNT"]), Decimal("50"))
        self.assertEqual(final["WIN_TRADES"], 1)
        self.assertEqual(final["LOSS_TRADES"], 0)
        self.assertEqual(Decimal(final["WIN_RATE_AMOUNT"]), Decimal("100"))
        self.assertEqual(Decimal(final["MAX_GAIN_AMOUNT"]), Decimal("50"))
        self.assertIsNone(final["MAX_LOSS_AMOUNT"])
        self.assertEqual(Decimal(final["FINAL_EQUITY"]), Decimal("150"))

        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal("50"))
        self.assertEqual(Decimal(portfolio["FINAL_EQUITY"]), Decimal("1050"))
        self.assertEqual(Decimal(portfolio["TOTAL_GAIN_AMOUNT"]), Decimal("50"))
        self.assertEqual(Decimal(portfolio["TOTAL_LOSS_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["AVG_TRADE_AMOUNT"]), Decimal("50"))
        self.assertEqual(portfolio["TOTAL_TRADES"], 1)
        self.assertEqual(Decimal(portfolio["WIN_RATE_AMOUNT"]), Decimal("100"))
        self.assertEqual(Decimal(portfolio["MAX_GAIN_AMOUNT"]), Decimal("50"))
        self.assertIsNone(portfolio["MAX_LOSS_AMOUNT"])
        self.assertEqual(Decimal(portfolio["max_drawdown_amount"]), Decimal("0"))


    def test_backtest_portfolio_counts_flat_trades_and_total_return_on_capital(self):
        start = date(2024, 2, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(3)
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")

        bt = Backtest.objects.create(
            name="Flat Trade Portfolio KPI",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        final = result["tickers"][self.symbol.ticker]["lines"][0]["final"]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(final["N"], 1)
        self.assertEqual(final["WIN_TRADES"], 0)
        self.assertEqual(final["LOSS_TRADES"], 0)
        self.assertEqual(final["FLAT_TRADES"], 1)
        self.assertEqual(Decimal(final["AVG_TRADE_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(final["FINAL_EQUITY"]), Decimal("100"))

        self.assertEqual(portfolio["TOTAL_TRADES"], 1)
        self.assertEqual(portfolio["WIN_TRADES"], 0)
        self.assertEqual(portfolio["LOSS_TRADES"], 0)
        self.assertEqual(portfolio["FLAT_TRADES"], 1)
        self.assertEqual(Decimal(portfolio["AVG_TRADE_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["TOTAL_RETURN_ON_CAPITAL"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["WIN_RATE_AMOUNT"]), Decimal("0"))

    def test_backtest_portfolio_counts_played_ticker_once_even_with_multiple_lines(self):
        start = date(2024, 3, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "11", "12", "13"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(str(10 + i)), ratio_P=Decimal("1"))
            for i in range(4)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1,C1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1,D1"),
        ])

        bt = Backtest.objects.create(
            name="Played Ticker Count",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[
                {"buy": ["A1"], "sell": ["B1"]},
                {"buy": ["C1"], "sell": ["D1"]},
            ],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(portfolio["NB_PLAYED_TICKERS"], 1)
        self.assertEqual(portfolio["TOTAL_TRADES"], 2)
        self.assertEqual(portfolio["FLAT_TRADES"], 0)

    def test_incremental_and_full_indicator_calculations_match(self):
        dates = self._create_bars_for_symbol(self.symbol, ["10", "11", "12", "11", "13", "15", "14"])

        for d in dates:
            compute_for_symbol_scenario(self.symbol, self.scenario, d)

        inc_metrics = {
            m.date: m
            for m in DailyMetric.objects.filter(symbol=self.symbol, scenario=self.scenario).order_by("date")
        }
        inc_alerts = {
            a.date: a.alerts
            for a in Alert.objects.filter(symbol=self.symbol, scenario=self.scenario).order_by("date")
        }

        DailyMetric.objects.all().delete()
        Alert.objects.all().delete()

        bars = DailyBar.objects.filter(symbol=self.symbol).order_by("date")
        compute_full_for_symbol_scenario(symbol=self.symbol, scenario=self.scenario, bars=bars)

        full_metrics = {
            m.date: m
            for m in DailyMetric.objects.filter(symbol=self.symbol, scenario=self.scenario).order_by("date")
        }
        full_alerts = {
            a.date: a.alerts
            for a in Alert.objects.filter(symbol=self.symbol, scenario=self.scenario).order_by("date")
        }

        self.assertEqual(set(inc_metrics.keys()), set(full_metrics.keys()))
        for d in inc_metrics:
            inc = inc_metrics[d]
            full = full_metrics[d]
            for field in [
                "P", "M", "M1", "X", "X1", "T", "Q", "S", "K1", "K2", "K3", "K4",
                "Kf2bis", "sum_slope", "slope_vrai", "sum_slope_basse", "slope_vrai_basse",
            ]:
                self.assertEqual(getattr(inc, field), getattr(full, field), f"Mismatch on {field} @ {d}")
        self.assertEqual(inc_alerts, full_alerts)

    def test_global_momentum_values_and_regimes_are_computed_per_date(self):
        metrics_by_ticker = {
            "AAA": {
                date(2024, 1, 1): Decimal("100"),
                date(2024, 1, 2): Decimal("110"),
                date(2024, 1, 3): Decimal("121"),
            },
            "BBB": {
                date(2024, 1, 1): Decimal("100"),
                date(2024, 1, 2): Decimal("90"),
                date(2024, 1, 3): Decimal("81"),
            },
        }
        values = compute_global_momentum_values_by_date(metrics_by_ticker, nglobal=1)
        self.assertEqual(values[date(2024, 1, 2)], Decimal("0"))
        self.assertEqual(values[date(2024, 1, 3)], Decimal("0"))

        regimes = build_global_momentum_regime_by_date(metrics_by_ticker, nglobal=1, neutral_band=Decimal("0.0001"))
        self.assertEqual(regimes[date(2024, 1, 2)], "GM_NEU")
        self.assertEqual(regimes[date(2024, 1, 3)], "GM_NEU")

    def test_alert_enrichment_appends_current_gm_and_removes_stale_code(self):
        other = Symbol.objects.create(ticker="BBB", exchange="NASDAQ", active=True)
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        dates = [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]
        for d, p1, p2 in zip(dates, [100, 110, 121], [100, 90, 81]):
            DailyMetric.objects.create(symbol=self.symbol, scenario=self.scenario, date=d, P=Decimal(str(p1)))
            DailyMetric.objects.create(symbol=other, scenario=self.scenario, date=d, P=Decimal(str(p2)))
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=date(2024, 1, 2), alerts="A1,GM_NEG")
        Alert.objects.create(symbol=other, scenario=self.scenario, date=date(2024, 1, 2), alerts="B1")

        updated = _enrich_alerts_with_global_momentum(scenario=self.scenario)
        self.assertEqual(updated, 2)
        a1 = Alert.objects.get(symbol=self.symbol, date=date(2024, 1, 2))
        a2 = Alert.objects.get(symbol=other, date=date(2024, 1, 2))
        self.assertEqual(a1.alerts, "A1,GM_NEU")
        self.assertEqual(a2.alerts, "B1,GM_NEU")

    def test_and_memory_and_gm_filters_behave_as_expected(self):
        active_states = {}
        day1 = _apply_signal_state_transitions(active_states, {"AF"})
        self.assertIn("AF", day1)
        day2 = _apply_signal_state_transitions(active_states, set())
        self.assertIn("AF", day2)
        day3 = _apply_signal_state_transitions(active_states, {"BF"})
        self.assertNotIn("AF", day3)
        self.assertIn("BF", day3)

        latched_states = {}
        latched1 = _update_and_latched_states(latched_states, {"A1"})
        self.assertIn("A1", latched1)
        latched2 = _update_and_latched_states(latched_states, {"C1"})
        self.assertIn("A1", latched2)
        self.assertIn("C1", latched2)
        self.assertTrue(
            _match_line_with_global_filter(
                day_alerts={"C1"},
                latched_alerts=latched2,
                codes=["A1", "C1"],
                logic="AND",
                gm_code="GM_POS",
                gm_filter="GM_POS",
                gm_operator="AND",
            )
        )
        self.assertFalse(
            _match_line_with_global_filter(
                day_alerts={"C1"},
                latched_alerts=latched2,
                codes=["A1", "C1"],
                logic="AND",
                gm_code="GM_NEG",
                gm_filter="GM_POS",
                gm_operator="AND",
            )
        )
        self.assertTrue(
            _match_line_with_global_filter(
                day_alerts=set(),
                latched_alerts=set(),
                codes=[],
                logic="AND",
                gm_code="GM_NEG",
                gm_filter="GM_NEG",
                gm_operator="AND",
            )
        )


    def test_normalize_codes_accepts_legacy_csv_for_sell_or_logic(self):
        self.assertTrue(
            _match_line_with_global_filter(
                day_alerts={"SPVV_BASSE"},
                latched_alerts=set(),
                codes="SPV,SPVV,SPV_BASSE,SPVV_BASSE",
                logic="OR",
                gm_code="GM_POS",
                gm_filter="IGNORE",
                gm_operator="AND",
            )
        )

    def test_normalize_codes_accepts_mixed_list_with_csv_and_deduplicates(self):
        self.assertTrue(
            _match_line_with_global_filter(
                day_alerts={"SPVA"},
                latched_alerts={"SPA"},
                codes=["SPA, SPVA", "SPVA"],
                logic="AND",
                gm_code="GM_POS",
                gm_filter="IGNORE",
                gm_operator="AND",
            )
        )


    def test_run_backtest_clears_signal_memory_after_sell_preventing_same_day_rebuy(self):
        start = date(2024, 1, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(4)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="C1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
        ])

        bt = Backtest.objects.create(
            name="Memory Reset Test",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1", "C1"], "buy_logic": "AND", "sell": ["B1"], "sell_logic": "OR"}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertNotIn("BUY", {row.get("action") for row in daily[2:]})
        self.assertEqual(Decimal(line["final"]["cash_ticker_end"]), Decimal("100"))
        self.assertEqual(Decimal(line["final"]["FINAL_EQUITY"]), Decimal("100"))

    def test_run_backtest_kpi_only_keeps_query_count_low_for_many_tickers(self):
        symbols = [self.symbol]
        for i in range(1, 26):
            symbols.append(Symbol.objects.create(ticker=f"T{i:03d}", exchange="NYSE", active=True))
        start = date(2024, 1, 1)
        for idx, sym in enumerate(symbols):
            bars = []
            metrics = []
            alerts = []
            for offset in range(6):
                d = start + timedelta(days=offset)
                close = Decimal("100") + Decimal(idx) + Decimal(offset)
                bars.append(DailyBar(symbol=sym, date=d, open=close, high=close + 1, low=close - 1, close=close, volume=1000))
                metrics.append(DailyMetric(symbol=sym, scenario=self.scenario, date=d, P=close, ratio_P=Decimal("1")))
                if offset == 2:
                    alerts.append(Alert(symbol=sym, scenario=self.scenario, date=d, alerts="A1"))
                elif offset == 4:
                    alerts.append(Alert(symbol=sym, scenario=self.scenario, date=d, alerts="B1"))
            DailyBar.objects.bulk_create(bars)
            DailyMetric.objects.bulk_create(metrics)
            Alert.objects.bulk_create(alerts)

        bt = Backtest(
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=5),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[s.ticker for s in symbols],
            warmup_days=0,
            close_positions_at_end=True,
        )
        with CaptureQueriesContext(connection) as ctx:
            out = run_backtest_kpi_only(bt)
        self.assertEqual(len(out), len(symbols))
        self.assertLessEqual(len(ctx.captured_queries), 5, [q["sql"] for q in ctx.captured_queries])

    def test_game_runner_updates_snapshot_and_interprets_thresholds_in_percent(self):
        game = GameScenario.objects.create(
            name="Daily Game",
            active=True,
            study_days=30,
            tradability_threshold=Decimal("0.3"),
            presence_threshold_pct=Decimal("30"),
            npente=100,
            slope_threshold=Decimal("0.1"),
            npente_basse=20,
            slope_threshold_basse=Decimal("0.02"),
            nglobal=20,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
        )
        # The runner derives end_d from DailyBar.
        self._create_bars_for_symbol(self.symbol, ["10", "11", "12"])

        fake_depth = SimpleNamespace(needs_full_recompute=lambda: False, missing_symbol_ids=[], total_symbols=1)
        fake_out = {
            self.symbol.ticker: {
                "best_bmd": "0.004",  # 0.4%
                "lines": [
                    {"final": {"BMD": "0.004", "TRADABLE_DAYS": 10, "TRADABLE_DAYS_IN_POSITION_CLOSED": 4}}
                ],
            }
        }
        with patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 1, "bars": 0}), \
             patch("core.tasks._compute_metrics_for_scenario", return_value={"symbols": 1, "rows": 0}), \
             patch("core.services.game_scenarios.runner.check_metrics_depth", return_value=fake_depth), \
             patch("core.services.game_scenarios.runner.run_backtest_kpi_only", return_value=fake_out), \
             patch("core.services.game_scenarios.runner._compute_avg_slope_for_ticker", return_value="0.2"):
            result = run_game_scenario_now(game.id)

        game.refresh_from_db()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(game.last_run_status, "ok")
        self.assertEqual(len(game.today_results.get("rows") or []), 1)
        row = game.today_results["rows"][0]
        self.assertEqual(row["ticker"], self.symbol.ticker)
        self.assertTrue(row["ok"], row)
        self.assertIn(row["RATIO_IN_POSITION"], {"40", "40.0"})
