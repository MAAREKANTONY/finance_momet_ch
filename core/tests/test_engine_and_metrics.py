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


def apply_signal_events_to_latch_state(state: dict[str, bool], events: set[str]) -> dict[str, bool]:
    """Pure spec helper for the target latch model used by tests only."""
    updated = dict(state)
    grouped: dict[str, set[str]] = {}
    for raw_event in events:
        event = str(raw_event or "").strip().upper()
        if len(event) < 2 or event[-1] not in {"+", "-"}:
            continue
        grouped.setdefault(event[:-1], set()).add(event[-1])

    for signal_id, polarities in grouped.items():
        if "+" in polarities and "-" in polarities:
            updated[signal_id] = False
        elif "+" in polarities:
            updated[signal_id] = True
        elif "-" in polarities:
            updated[signal_id] = False
    return updated


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

    def _create_latch_routing_fixture(self, *, start: date | None = None) -> date:
        start = start or date(2024, 8, 1)
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
        return start

    def _create_progressive_af_spva_basse_fixture(
        self,
        *,
        start: date | None = None,
        ratio_p: Decimal | None = Decimal("1"),
        closes: list[str] | None = None,
    ) -> date:
        start = start or date(2024, 10, 1)
        closes = closes or ["10", "10", "12"]
        self._create_bars_for_symbol(self.symbol, closes, start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=i),
                P=Decimal(close),
                ratio_P=ratio_p,
            )
            for i, close in enumerate(closes)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="Af"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="SPVa_basse"),
        ])
        return start

    def _create_price_range_backtest(
        self,
        *,
        settings: dict | None = None,
        closes: list[str] | None = None,
        ratio_p: Decimal | None = None,
        close_positions_at_end: bool = False,
        signal_lines: list[dict] | None = None,
    ) -> tuple[Backtest, date]:
        start = self._create_progressive_af_spva_basse_fixture(
            ratio_p=ratio_p,
            closes=closes or ["50", "50", "50"],
        )
        bt = Backtest.objects.create(
            name="Price Range Tradability",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=signal_lines or [{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=close_positions_at_end,
            settings=settings or {},
        )
        return bt, start


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

    def test_backtest_golden_line_kpis_single_profitable_trade(self):
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
            name="Golden KPI Profitable Trade",
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
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        final = line["final"]
        daily_last = line["daily"][-1]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(Decimal(final["BT"]), Decimal("0.5"))
        self.assertEqual(final["TRADABLE_DAYS"], 4)
        self.assertEqual(final["TRADABLE_DAYS_NOT_IN_POSITION"], 2)
        self.assertEqual(final["TRADABLE_DAYS_IN_POSITION_CLOSED"], 2)
        self.assertEqual(Decimal(final["RATIO_NOT_IN_POSITION"]), Decimal("50"))
        self.assertEqual(Decimal(final["RATIO_IN_POSITION"]), Decimal("50"))
        self.assertEqual(Decimal(final["BMJ"]), Decimal("0.25"))
        self.assertEqual(Decimal(final["BMD"]), Decimal("0.25"))

        self.assertEqual(Decimal(daily_last["BT"]), Decimal("0.5"))
        self.assertEqual(daily_last["TRADABLE_DAYS"], 4)
        self.assertEqual(daily_last["TRADABLE_DAYS_NOT_IN_POSITION"], 2)
        self.assertEqual(daily_last["TRADABLE_DAYS_IN_POSITION_CLOSED"], 2)
        self.assertEqual(Decimal(daily_last["RATIO_NOT_IN_POSITION"]), Decimal("50"))
        self.assertEqual(Decimal(daily_last["RATIO_IN_POSITION"]), Decimal("50"))
        self.assertEqual(Decimal(daily_last["BMJ"]), Decimal("0.25"))
        self.assertEqual(Decimal(daily_last["BMD"]), Decimal("0.25"))

        self.assertEqual(Decimal(portfolio["invested_end"]), Decimal("100"))
        self.assertEqual(Decimal(portfolio["equity_end"]), Decimal("1050"))
        self.assertEqual(portfolio["NB_DAYS"], 4)
        self.assertEqual(portfolio["N"], 1)
        self.assertEqual(Decimal(portfolio["S_G_N"]), Decimal("0.5"))
        self.assertEqual(Decimal(portfolio["BT"]), Decimal("0.5"))
        self.assertEqual(portfolio["TRADABLE_DAYS_NOT_IN_POSITION"], 2)
        self.assertEqual(portfolio["TRADABLE_DAYS_IN_POSITION_CLOSED"], 2)
        self.assertEqual(Decimal(portfolio["BMJ"]), Decimal("0.25"))
        self.assertEqual(Decimal(portfolio["BMD"]), Decimal("0.25"))

    def test_backtest_golden_portfolio_kpis_no_trade_all_days_tradable(self):
        start = date(2024, 2, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(3)
        ])

        bt = Backtest.objects.create(
            name="Golden KPI No Trade",
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

        self.assertEqual(Decimal(final["BT"]), Decimal("0"))
        self.assertEqual(final["TRADABLE_DAYS"], 3)
        self.assertEqual(final["TRADABLE_DAYS_NOT_IN_POSITION"], 3)
        self.assertEqual(final["TRADABLE_DAYS_IN_POSITION_CLOSED"], 0)
        self.assertEqual(Decimal(final["RATIO_NOT_IN_POSITION"]), Decimal("100"))
        self.assertEqual(Decimal(final["RATIO_IN_POSITION"]), Decimal("0"))
        self.assertEqual(Decimal(final["BMJ"]), Decimal("0"))
        self.assertIsNone(final["BMD"])

        self.assertEqual(Decimal(portfolio["invested_end"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["equity_end"]), Decimal("1000"))
        self.assertEqual(portfolio["NB_DAYS"], 0)
        self.assertEqual(portfolio["N"], 0)
        self.assertIsNone(portfolio["S_G_N"])
        self.assertEqual(Decimal(portfolio["BT"]), Decimal("0"))
        self.assertEqual(portfolio["TRADABLE_DAYS_NOT_IN_POSITION"], 3)
        self.assertEqual(portfolio["TRADABLE_DAYS_IN_POSITION_CLOSED"], 0)
        self.assertEqual(Decimal(portfolio["BMJ"]), Decimal("0"))
        self.assertIsNone(portfolio["BMD"])

    def test_backtest_portfolio_kpis_match_line_kpis_for_single_closed_trade(self):
        start = date(2024, 2, 10)
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
            name="Portfolio KPI Single Trade",
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
        line_final = result["tickers"][self.symbol.ticker]["lines"][0]["final"]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(portfolio["N"], line_final["N"])
        self.assertEqual(Decimal(portfolio["BT"]), Decimal(line_final["BT"]))
        self.assertEqual(Decimal(portfolio["BMJ"]), Decimal(line_final["BMJ"]))
        self.assertEqual(Decimal(portfolio["BMD"]), Decimal(line_final["BMD"]))

    def test_backtest_portfolio_kpis_aggregate_multiple_closed_trades_on_one_line(self):
        start = date(2024, 2, 20)
        self._create_bars_for_symbol(self.symbol, ["10", "11", "10", "12"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "11", "10", "12"])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="B1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=3), alerts="B1"),
        ])

        bt = Backtest.objects.create(
            name="Portfolio KPI Multi Trade One Line",
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
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line_final = result["tickers"][self.symbol.ticker]["lines"][0]["final"]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(line_final["N"], 2)
        self.assertEqual(portfolio["N"], 2)
        self.assertEqual(Decimal(portfolio["BT"]), Decimal("0.3"))
        self.assertEqual(Decimal(portfolio["BT"]), Decimal(line_final["BT"]))
        self.assertEqual(Decimal(portfolio["S_G_N"]), Decimal(portfolio["BT"]) / Decimal(portfolio["N"]))

    def test_backtest_portfolio_kpis_aggregate_across_multiple_tickers(self):
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        start = date(2024, 3, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "12", "15", "11"], start=start)
        self._create_bars_for_symbol(other, ["20", "20", "24", "24"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "12", "15", "11"])
        ] + [
            DailyMetric(symbol=other, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["20", "20", "24", "24"])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
            Alert(symbol=other, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=other, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
        ])

        bt = Backtest.objects.create(
            name="Portfolio KPI Multi Ticker",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker, other.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        lines = [
            result["tickers"][self.symbol.ticker]["lines"][0]["final"],
            result["tickers"][other.ticker]["lines"][0]["final"],
        ]
        portfolio = result["portfolio"]["kpi"]
        expected_n = sum(int(line["N"]) for line in lines)
        expected_bt = sum(Decimal(line["BT"]) for line in lines)
        expected_not_in = sum(int(line["TRADABLE_DAYS_NOT_IN_POSITION"]) for line in lines)
        expected_in = sum(int(line["TRADABLE_DAYS_IN_POSITION_CLOSED"]) for line in lines)

        self.assertEqual(portfolio["N"], expected_n)
        self.assertEqual(Decimal(portfolio["BT"]), expected_bt)
        self.assertEqual(portfolio["TRADABLE_DAYS_NOT_IN_POSITION"], expected_not_in)
        self.assertEqual(portfolio["TRADABLE_DAYS_IN_POSITION_CLOSED"], expected_in)
        self.assertEqual(Decimal(portfolio["BMJ"]), expected_bt / Decimal(expected_not_in))
        self.assertEqual(Decimal(portfolio["BMD"]), expected_bt / Decimal(expected_in))

    def test_backtest_portfolio_kpis_do_not_become_null_in_reinvest_with_unlimited_capital(self):
        start = date(2024, 3, 10)
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
            name="Portfolio KPI Reinvest Unlimited",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("0"),
            capital_per_ticker=Decimal("100"),
            capital_mode="REINVEST",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        line_final = result["tickers"][self.symbol.ticker]["lines"][0]["final"]
        portfolio = result["portfolio"]["kpi"]

        self.assertIsNotNone(portfolio["BT"])
        self.assertIsNotNone(portfolio["BMJ"])
        self.assertIsNotNone(portfolio["BMD"])
        self.assertEqual(Decimal(portfolio["BT"]), Decimal(line_final["BT"]))

    def test_backtest_portfolio_kpis_include_forced_sell_in_trade_count_and_bt(self):
        start = date(2024, 3, 20)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "13"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "10", "13"])
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")

        bt = Backtest.objects.create(
            name="Portfolio KPI Forced Sell",
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
        line_final = result["tickers"][self.symbol.ticker]["lines"][0]["final"]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(portfolio["N"], 1)
        self.assertEqual(Decimal(portfolio["BT"]), Decimal(line_final["BT"]))

    def test_backtest_portfolio_kpis_no_trade_uses_zero_bt_and_null_s_g_n(self):
        start = date(2024, 4, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(3)
        ])

        bt = Backtest.objects.create(
            name="Portfolio KPI No Trade Zero BT",
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
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(portfolio["N"], 0)
        self.assertEqual(Decimal(portfolio["BT"]), Decimal("0"))
        self.assertIsNone(portfolio["S_G_N"])


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

    def test_forced_sell_at_backtest_end_updates_trade_counters_and_daily_row(self):
        start = date(2024, 3, 10)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "13"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "10", "13"])
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")

        bt = Backtest.objects.create(
            name="Forced Sell End Sync",
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
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        final = line["final"]
        daily_last = line["daily"][-1]
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(final["N"], 1)
        self.assertEqual(Decimal(final["BT"]), Decimal("0.3"))
        self.assertEqual(Decimal(final["S_G_N"]), Decimal("0.3"))
        self.assertEqual(Decimal(final["PNL_AMOUNT"]), Decimal("30"))
        self.assertEqual(final["WIN_TRADES"], 1)
        self.assertEqual(final["LOSS_TRADES"], 0)
        self.assertEqual(final["FLAT_TRADES"], 0)
        self.assertEqual(daily_last["action"], "FORCED_SELL")
        self.assertTrue(daily_last["forced_close"])
        self.assertEqual(daily_last["shares"], 0)
        self.assertEqual(daily_last["N"], 1)
        self.assertEqual(Decimal(daily_last["BT"]), Decimal("0.3"))
        self.assertEqual(Decimal(daily_last["action_PNL_AMOUNT"]), Decimal("30"))
        self.assertEqual(portfolio["TOTAL_TRADES"], 1)
        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal("30"))
        self.assertEqual(Decimal(portfolio["WIN_RATE_AMOUNT"]), Decimal("100"))

    def test_forced_sell_at_backtest_end_updates_loss_side_kpis(self):
        start = date(2024, 3, 20)
        self._create_bars_for_symbol(self.symbol, ["10", "9", "8"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "9", "8"])
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")

        bt = Backtest.objects.create(
            name="Forced Sell End Loss",
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
        self.assertEqual(final["LOSS_TRADES"], 1)
        self.assertEqual(final["FLAT_TRADES"], 0)
        self.assertEqual(Decimal(final["PNL_AMOUNT"]), Decimal("-20"))
        self.assertEqual(Decimal(final["TOTAL_LOSS_AMOUNT"]), Decimal("-20"))
        self.assertEqual(Decimal(final["MAX_LOSS_AMOUNT"]), Decimal("-20"))
        self.assertEqual(portfolio["TOTAL_TRADES"], 1)
        self.assertEqual(portfolio["LOSS_TRADES"], 1)
        self.assertEqual(Decimal(portfolio["TOTAL_LOSS_AMOUNT"]), Decimal("-20"))

    def test_run_backtest_kpi_only_counts_forced_sell_as_completed_trade(self):
        start = date(2024, 4, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "11", "12"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "11", "12"])
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")

        bt = Backtest.objects.create(
            name="KPI Only Forced Sell",
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

        result = run_backtest_kpi_only(bt)
        final = result[self.symbol.ticker]["lines"][0]["final"]

        self.assertEqual(final["N"], 1)
        self.assertEqual(Decimal(final["BT"]), Decimal("0.2"))
        self.assertEqual(Decimal(result[self.symbol.ticker]["best_bmd"]), Decimal(final["BMD"]))

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

    def test_signal_latch_progressively_activates_required_codes_across_days(self):
        state = {}
        state = apply_signal_events_to_latch_state(state, {"S1+"})
        self.assertEqual(state, {"S1": True})
        self.assertFalse(all(state.get(signal, False) for signal in ("S1", "S2")))

        state = apply_signal_events_to_latch_state(state, set())
        self.assertEqual(state, {"S1": True})
        self.assertFalse(all(state.get(signal, False) for signal in ("S1", "S2")))

        state = apply_signal_events_to_latch_state(state, {"S2+"})
        self.assertEqual(state, {"S1": True, "S2": True})
        self.assertTrue(all(state.get(signal, False) for signal in ("S1", "S2")))

    def test_signal_latch_selectively_invalidates_only_matching_pair_before_buy(self):
        state = {"S1": True, "S2": True}
        state = apply_signal_events_to_latch_state(state, {"S1-"})
        self.assertEqual(state, {"S1": False, "S2": True})
        self.assertFalse(all(state.get(signal, False) for signal in ("S1", "S2")))

    def test_signal_latch_handles_same_day_activation_and_invalidation_conservatively(self):
        state = {"S1": True, "S2": True}
        state = apply_signal_events_to_latch_state(state, {"S1+", "S1-"})
        self.assertEqual(state, {"S1": False, "S2": True})
        self.assertFalse(all(state.get(signal, False) for signal in ("S1", "S2")))


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

    def test_run_backtest_warmup_carries_latched_setup_into_first_live_day(self):
        start = date(2024, 5, 3)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10", "10"], start=start - timedelta(days=2))
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start - timedelta(days=2) + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(4)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start - timedelta(days=2), alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start - timedelta(days=1), alerts="C1"),
        ])

        bt = Backtest.objects.create(
            name="Warmup Latch Carryover",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=1),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1", "C1"], "buy_logic": "AND", "sell": ["B1"], "sell_logic": "OR"}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=2,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        self.assertEqual(line["daily"][0]["action"], "BUY")

    def test_run_backtest_target_model_sells_on_first_invalidated_latch_without_explicit_sell_codes(self):
        start = date(2024, 6, 1)
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
            name="Latch Sell Without Sell Codes",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertEqual(line["final"]["N"], 1)

    def test_run_backtest_target_model_resets_after_sell_and_blocks_same_day_reentry(self):
        start = date(2024, 7, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(4)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="C1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1,A1,C1"),
        ])

        bt = Backtest.objects.create(
            name="Latch Reset Same Day Reentry",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertNotIn("BUY", str(daily[2]["action"]))
        self.assertEqual(line["final"]["N"], 1)

    def test_explicit_legacy_daily_forces_legacy_routing_for_supported_latch_codes(self):
        start = self._create_latch_routing_fixture()
        bt = Backtest.objects.create(
            name="Explicit Legacy Routing",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LEGACY_DAILY", "buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "LEGACY_DAILY")
        self.assertEqual(daily[1]["action"], "BUY")
        self.assertNotIn("SELL", {row.get("action") for row in daily})
        self.assertEqual(line["final"]["N"], 0)

    def test_explicit_latch_stateful_forces_latch_routing(self):
        start = self._create_latch_routing_fixture()
        bt = Backtest.objects.create(
            name="Explicit Latch Routing",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "LATCH_STATEFUL")
        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertEqual(line["final"]["N"], 1)

    def test_missing_trading_model_preserves_current_implicit_latch_routing(self):
        start = self._create_latch_routing_fixture()
        bt = Backtest.objects.create(
            name="Implicit Latch Routing",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "LATCH_STATEFUL")
        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertEqual(line["final"]["N"], 1)

    def test_invalid_explicit_latch_config_raises(self):
        invalid_lines = [
            {"trading_model": "LATCH_STATEFUL", "buy": ["A1"], "buy_logic": "OR", "sell": []},
            {"trading_model": "LATCH_STATEFUL", "buy": ["B1"], "buy_logic": "AND", "sell": []},
            {"trading_model": "LATCH_STATEFUL", "buy": ["A1"], "buy_logic": "AND", "sell": ["B1"]},
            {"trading_model": "LATCH_STATEFUL", "buy": ["A1"], "buy_logic": "AND", "sell": [], "sell_gm_filter": "GM_POS"},
        ]
        for line in invalid_lines:
            with self.subTest(line=line):
                bt = Backtest(
                    scenario=self.scenario,
                    start_date=date(2024, 9, 1),
                    end_date=date(2024, 9, 2),
                    capital_total=Decimal("1000"),
                    capital_per_ticker=Decimal("100"),
                    capital_mode="FIXED",
                    include_all_tickers=True,
                    signal_lines=[line],
                    universe_snapshot=[self.symbol.ticker],
                    warmup_days=0,
                    close_positions_at_end=False,
                )
                with self.assertRaises(ValueError):
                    run_backtest(bt)

    def test_explicit_latch_routing_matches_kpi_only_path(self):
        start = self._create_latch_routing_fixture()
        bt = Backtest.objects.create(
            name="Latch Full KPI Parity",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        full_final = run_backtest(bt).results["tickers"][self.symbol.ticker]["lines"][0]["final"]
        kpi_final = run_backtest_kpi_only(bt)[self.symbol.ticker]["lines"][0]["final"]

        self.assertEqual(full_final["N"], 1)
        self.assertEqual(kpi_final["N"], 1)
        self.assertEqual(Decimal(full_final["BT"]), Decimal(kpi_final["BT"]))

    def test_default_tradability_allows_progressive_buy_when_ratio_p_missing(self):
        start = self._create_progressive_af_spva_basse_fixture(ratio_p=None)
        bt = Backtest.objects.create(
            name="Default Tradability Progressive",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertIsNone(daily[0]["action"])
        self.assertEqual(daily[1]["action"], "BUY")

    def test_price_range_default_bounds_preserve_progressive_buy_with_missing_ratio_p(self):
        bt, _start = self._create_price_range_backtest(settings={"min_price": None, "max_price": None}, ratio_p=None)

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertTrue(daily[1]["tradable"])

    def test_price_range_below_min_blocks_buy_and_marks_day_not_tradable(self):
        bt, _start = self._create_price_range_backtest(settings={"min_price": "100"}, closes=["90", "90", "90"], ratio_p=None)

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertIsNone(daily[1]["action"])
        self.assertFalse(daily[1]["tradable"])
        self.assertEqual(line["final"]["N"], 0)

    def test_price_range_above_max_blocks_buy_and_marks_day_not_tradable(self):
        bt, _start = self._create_price_range_backtest(settings={"max_price": "50"}, closes=["60", "60", "60"], ratio_p=None)

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertIsNone(daily[1]["action"])
        self.assertFalse(daily[1]["tradable"])
        self.assertEqual(line["final"]["N"], 0)

    def test_price_range_inside_bounds_allows_buy(self):
        bt, _start = self._create_price_range_backtest(settings={"min_price": "10", "max_price": "100"}, closes=["50", "50", "50"], ratio_p=None)

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertTrue(daily[1]["tradable"])

    def test_price_range_missing_trading_day_price_blocks_buy_and_marks_day_not_tradable(self):
        start = date(2024, 10, 1)
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        self._create_bars_for_symbol(self.symbol, ["50"], start=start)
        self._create_bars_for_symbol(other, ["50", "50", "50"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("50"), ratio_P=None)
            for i in range(3)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="Af"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="SPVa_basse"),
        ])
        bt = Backtest.objects.create(
            name="Missing Price Tradability",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
            settings={"min_price": "10", "max_price": "100"},
        )

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]
        by_date = {row["date"]: row for row in daily}
        missing_price_day = str(start + timedelta(days=1))

        self.assertIn(missing_price_day, by_date)
        self.assertIsNone(by_date[missing_price_day]["action"])
        self.assertFalse(by_date[missing_price_day]["tradable"])

    def test_price_range_does_not_block_forced_sell_at_end(self):
        bt, _start = self._create_price_range_backtest(
            settings={"max_price": "50"},
            closes=["40", "40", "60"],
            ratio_p=None,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertGreater(Decimal(daily[-1]["price_close"]), Decimal("50"))
        self.assertIn("FORCED_SELL", daily[-1]["action"])
        self.assertTrue(daily[-1]["forced_close"])
        self.assertEqual(daily[-1]["shares"], 0)

    def test_price_range_does_not_block_explicit_sell_signal(self):
        start = date(2024, 11, 1)
        self._create_bars_for_symbol(self.symbol, ["40", "60", "60"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("60"), ratio_P=None)
            for i in range(3)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="B1"),
        ])
        bt = Backtest.objects.create(
            name="Price Range Sell Not Blocked",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LEGACY_DAILY", "buy": ["A1"], "buy_logic": "AND", "sell": ["B1"], "sell_logic": "OR"}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
            settings={"max_price": "50"},
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[0]["action"], "BUY")
        self.assertGreater(Decimal(daily[1]["price_close"]), Decimal("50"))
        self.assertEqual(daily[1]["action"], "SELL")
        self.assertEqual(daily[1]["shares"], 0)
        self.assertEqual(line["final"]["N"], 1)

    def test_price_range_does_not_block_progressive_invalidation_sell(self):
        start = date(2024, 12, 1)
        self._create_bars_for_symbol(self.symbol, ["40", "40", "60"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("60"), ratio_P=None)
            for i in range(3)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="Af"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="SPVa_basse"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="Bf"),
        ])
        bt = Backtest.objects.create(
            name="Price Range Progressive Sell Not Blocked",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
            settings={"max_price": "50"},
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertGreater(Decimal(daily[2]["price_close"]), Decimal("50"))
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertEqual(daily[2]["shares"], 0)
        self.assertEqual(line["final"]["N"], 1)

    def test_price_range_kpi_only_matches_full_backtest_final_state(self):
        bt, _start = self._create_price_range_backtest(settings={"min_price": "10", "max_price": "100"}, closes=["50", "50", "50"], ratio_p=None, close_positions_at_end=True)

        full_final = run_backtest(bt).results["tickers"][self.symbol.ticker]["lines"][0]["final"]
        kpi_final = run_backtest_kpi_only(bt)[self.symbol.ticker]["lines"][0]["final"]

        self.assertEqual(full_final["N"], kpi_final["N"])
        self.assertEqual(Decimal(full_final["BT"]), Decimal(kpi_final["BT"]))
        self.assertEqual(full_final["TRADABLE_DAYS"], kpi_final["TRADABLE_DAYS"])
        self.assertEqual(full_final["TRADABLE_DAYS_IN_POSITION_CLOSED"], kpi_final["TRADABLE_DAYS_IN_POSITION_CLOSED"])

    def test_price_range_game_runner_uses_same_backtest_logic(self):
        self._create_progressive_af_spva_basse_fixture(ratio_p=None, closes=["90", "90", "90"])
        game = GameScenario.objects.create(
            name="Price Range Game",
            active=True,
            study_days=3,
            tradability_threshold=Decimal("0"),
            presence_threshold_pct=Decimal("0"),
            npente=2,
            slope_threshold=Decimal("0.01"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.005"),
            nglobal=2,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            close_positions_at_end=True,
            settings={"min_price": "100"},
        )
        fake_depth = SimpleNamespace(needs_full_recompute=lambda: False, missing_symbol_ids=[], total_symbols=1)

        with patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 1, "bars": 0}), \
             patch("core.tasks._compute_metrics_for_scenario", return_value={"symbols": 1, "rows": 0}), \
             patch("core.services.game_scenarios.runner._sync_engine_scenario", return_value=self.scenario), \
             patch("core.services.game_scenarios.runner.check_metrics_depth", return_value=fake_depth), \
             patch("core.services.game_scenarios.runner._compute_avg_slope_for_ticker", return_value="0.2"):
            run_game_scenario_now(game.id, skip_metrics=True)

        game.refresh_from_db()
        rows = game.today_results.get("rows") or []
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ticker"], self.symbol.ticker)
        self.assertIsNone(rows[0]["bmd"])
        self.assertEqual(rows[0]["TRADABLE_DAYS"], 0)

    def test_price_range_applies_to_legacy_mode_buy_eligibility(self):
        start = self._create_latch_routing_fixture()
        bt = Backtest.objects.create(
            name="Legacy Price Range Tradability",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LEGACY_DAILY", "buy": ["A1"], "buy_logic": "AND", "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
            settings={"min_price": "100"},
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]

        self.assertEqual(line["final"]["N"], 0)
        self.assertNotIn("BUY", {row.get("action") for row in line["daily"]})

    def test_explicit_eligibility_filter_blocks_progressive_buy_when_ratio_p_missing(self):
        start = self._create_progressive_af_spva_basse_fixture(ratio_p=None)
        bt = Backtest.objects.create(
            name="Explicit Eligibility Progressive",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            ratio_threshold=Decimal("1"),
            include_all_tickers=False,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]

        self.assertNotIn("BUY", {row.get("action") for row in line["daily"]})
        self.assertEqual(line["final"]["N"], 0)

    def test_supported_progressive_signals_use_progressive_model_by_default(self):
        start = self._create_progressive_af_spva_basse_fixture(ratio_p=Decimal("1"))
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="Bf")
        bt = Backtest.objects.create(
            name="Implicit Progressive AF SPVA Basse",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "LATCH_STATEFUL")
        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")

    def test_end_of_backtest_forced_sell_closes_default_progressive_position(self):
        start = self._create_progressive_af_spva_basse_fixture(ratio_p=Decimal("1"))
        bt = Backtest.objects.create(
            name="Default Progressive Forced Sell",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[-1]["action"], "FORCED_SELL")
        self.assertTrue(daily[-1]["forced_close"])
        self.assertEqual(daily[-1]["shares"], 0)
        self.assertEqual(line["final"]["N"], 1)
        self.assertEqual(Decimal(line["final"]["cash_ticker_end"]), Decimal("100"))
        self.assertEqual(Decimal(line["final"]["PNL_AMOUNT"]), Decimal("20"))
        self.assertEqual(Decimal(line["final"]["FINAL_EQUITY"]), Decimal("120"))

    def test_end_of_backtest_forced_sell_closes_explicit_progressive_position(self):
        start = self._create_progressive_af_spva_basse_fixture(ratio_p=Decimal("1"))
        bt = Backtest.objects.create(
            name="Explicit Progressive Forced Sell",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[-1]["action"], "FORCED_SELL")
        self.assertTrue(daily[-1]["forced_close"])
        self.assertEqual(daily[-1]["shares"], 0)
        self.assertEqual(line["final"]["N"], 1)
        self.assertEqual(Decimal(line["final"]["cash_ticker_end"]), Decimal("100"))
        self.assertEqual(Decimal(line["final"]["PNL_AMOUNT"]), Decimal("20"))
        self.assertEqual(Decimal(line["final"]["FINAL_EQUITY"]), Decimal("120"))

    def test_forced_sell_final_state_matches_kpi_only_for_progressive_model(self):
        start = self._create_progressive_af_spva_basse_fixture(ratio_p=Decimal("1"))
        bt = Backtest.objects.create(
            name="Progressive Forced Sell KPI Parity",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af", "SPVa_basse"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=True,
        )

        full_final = run_backtest(bt).results["tickers"][self.symbol.ticker]["lines"][0]["final"]
        kpi_final = run_backtest_kpi_only(bt)[self.symbol.ticker]["lines"][0]["final"]

        self.assertEqual(full_final["N"], 1)
        self.assertEqual(kpi_final["N"], 1)
        self.assertEqual(Decimal(full_final["BT"]), Decimal(kpi_final["BT"]))
        self.assertEqual(full_final["TRADABLE_DAYS"], kpi_final["TRADABLE_DAYS"])
        self.assertEqual(full_final["TRADABLE_DAYS_IN_POSITION_CLOSED"], kpi_final["TRADABLE_DAYS_IN_POSITION_CLOSED"])

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
