from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

from django.db import connection
from django.test import TestCase, override_settings
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
from core.services.derived_data import game_impactful_changes, scenario_impactful_changes
from core.services.game_scenarios.runner import run_game_scenario_now
from core.services.game_scenarios.runner import _sync_engine_scenario
from core.services.game_scenarios.sync import GAME_RUNTIME_SCENARIO_FIELDS, sync_game_engine_scenario
from core.services.global_momentum import (
    build_global_momentum_regime_by_date,
    compute_global_momentum_values_by_date,
)
from core.services.recent_high_drawdown import (
    compute_recent_high_drawdown_alerts_for_series,
    compute_recent_high_drawdown_condition,
)
from core.tasks import (
    _enrich_alerts_with_global_momentum,
    _ensure_game_engine_scenario,
    determine_backtest_result_mode,
    estimate_backtest_daily_result_rows,
    indicator_signature,
    run_backtest_task,
)


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

    def _compute_alerts_with_slow_path(self, scenario: Scenario, closes: list[str]) -> list[str]:
        symbol = Symbol.objects.create(ticker=f"SLOW{Symbol.objects.count():04d}", exchange="NYSE", active=True)
        dates = self._create_bars_for_symbol(symbol, closes)
        for trading_date in dates:
            compute_for_symbol_scenario(symbol, scenario, trading_date)
        return list(
            Alert.objects.filter(symbol=symbol, scenario=scenario)
            .order_by("date")
            .values_list("alerts", flat=True)
        )

    def _compute_alert_map_with_slow_path(self, scenario: Scenario, closes: list[str]) -> dict[date, str]:
        symbol = Symbol.objects.create(ticker=f"SLOMAP{Symbol.objects.count():04d}", exchange="NYSE", active=True)
        dates = self._create_bars_for_symbol(symbol, closes)
        for trading_date in dates:
            compute_for_symbol_scenario(symbol, scenario, trading_date)
        return {
            row.date: row.alerts
            for row in Alert.objects.filter(symbol=symbol, scenario=scenario).order_by("date")
        }

    def _compute_alerts_with_fast_path(self, scenario: Scenario, closes: list[str]) -> list[str]:
        symbol = Symbol.objects.create(ticker=f"FAST{Symbol.objects.count():04d}", exchange="NYSE", active=True)
        self._create_bars_for_symbol(symbol, closes)
        bars = list(DailyBar.objects.filter(symbol=symbol).order_by("date"))
        compute_full_for_symbol_scenario(symbol=symbol, scenario=scenario, bars=bars)
        return list(
            Alert.objects.filter(symbol=symbol, scenario=scenario)
            .order_by("date")
            .values_list("alerts", flat=True)
        )

    def _compute_alert_map_with_fast_path(self, scenario: Scenario, closes: list[str]) -> dict[date, str]:
        symbol = Symbol.objects.create(ticker=f"FSTMAP{Symbol.objects.count():04d}", exchange="NYSE", active=True)
        self._create_bars_for_symbol(symbol, closes)
        bars = list(DailyBar.objects.filter(symbol=symbol).order_by("date"))
        compute_full_for_symbol_scenario(symbol=symbol, scenario=scenario, bars=bars)
        return {
            row.date: row.alerts
            for row in Alert.objects.filter(symbol=symbol, scenario=scenario).order_by("date")
        }

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

    def _create_selective_latch_retention_fixture(
        self,
        *,
        start: date | None = None,
        prices: list[str] | None = None,
        alerts_by_offset: dict[int, str] | None = None,
        k1_by_offset: dict[int, str] | None = None,
        slope_vrai_by_offset: dict[int, str] | None = None,
    ) -> tuple[date, Backtest]:
        start = start or date(2024, 12, 16)
        prices = prices or ["10", "11", "13", "12.5", "11.5", "13"]
        alerts_by_offset = alerts_by_offset or {
            1: "A1",
            2: "C1",
            4: "B1",
            5: "A1",
        }
        k1_by_offset = k1_by_offset or {
            0: "-1",
            1: "1",
            2: "1",
            3: "1",
            4: "-1",
            5: "1",
        }
        slope_vrai_by_offset = slope_vrai_by_offset or {
            0: "0.10",
            1: "0.15",
            2: "0.25",
            3: "0.24",
            4: "0.23",
            5: "0.24",
        }

        self.scenario.nglobal = 1
        self.scenario.slope_threshold = Decimal("0.2")
        self.scenario.save(update_fields=["nglobal", "slope_threshold"])

        self._create_bars_for_symbol(self.symbol, prices, start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=i),
                P=Decimal(prices[i]),
                ratio_P=Decimal("1"),
                K1=Decimal(k1_by_offset[i]),
                slope_vrai=Decimal(slope_vrai_by_offset[i]),
            )
            for i in range(len(prices))
        ])
        Alert.objects.bulk_create([
            Alert(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=offset),
                alerts=alerts,
            )
            for offset, alerts in sorted(alerts_by_offset.items())
        ])

        bt = Backtest.objects.create(
            name="Selective Latch Retention",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=len(prices) - 1),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["A1", "C1"],
                "buy_logic": "AND",
                "sell": [],
                "buy_gm_filter": "GM_POS",
            }],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )
        return start, bt

    def _gm_regime_map(self, start: date, mapping: dict[int, str]) -> dict[date, str]:
        return {start + timedelta(days=offset): regime for offset, regime in mapping.items()}

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
        portfolio_daily = result["portfolio"]["daily"]

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
        self.assertEqual(Decimal(portfolio_daily[-1]["equity"]), Decimal("1050"))
        self.assertEqual(Decimal(portfolio_daily[-1]["pnl_global"]), Decimal("50"))
        self.assertEqual(
            Decimal(portfolio_daily[-1]["pnl_global"]),
            Decimal(portfolio["TOTAL_PNL_AMOUNT"]),
        )

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
        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal(portfolio["TOTAL_GAIN_AMOUNT"]) + Decimal(portfolio["TOTAL_LOSS_AMOUNT"]))
        self.assertEqual(Decimal(portfolio["BT"]), (Decimal(portfolio["equity_end"]) - Decimal(portfolio["invested_end"])) / Decimal(portfolio["invested_end"]))
        self.assertEqual(Decimal(portfolio["BMJ"]), Decimal(portfolio["BT"]) / Decimal(portfolio["NB_DAYS"]))

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
        portfolio_daily = result["portfolio"]["daily"]

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
        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal("0"))
        self.assertIsNone(portfolio["BT"])
        self.assertIsNone(portfolio["BMJ"])
        self.assertTrue(portfolio_daily)
        for row in portfolio_daily:
            self.assertEqual(Decimal(row["equity"]), Decimal("1000"))
            self.assertEqual(Decimal(row["pnl_global"]), Decimal("0"))
            self.assertEqual(Decimal(row["drawdown"]), Decimal("0"))
        self.assertEqual(
            Decimal(portfolio_daily[-1]["pnl_global"]),
            Decimal(portfolio["TOTAL_PNL_AMOUNT"]),
        )

        large_result = run_backtest(bt, large_result_mode=True).results
        self.assertTrue(large_result["meta"]["detailed_daily_rows_omitted"])
        self.assertTrue(large_result["portfolio"]["daily"])
        self.assertTrue(
            all(Decimal(row["pnl_global"]) == Decimal("0") for row in large_result["portfolio"]["daily"])
        )

    def test_backtest_portfolio_daily_pnl_includes_unrealized_gain_without_realized_gain(self):
        start = date(2024, 2, 5)
        self._create_bars_for_symbol(self.symbol, ["10", "12", "15"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=i),
                P=Decimal(value),
                ratio_P=Decimal("1"),
            )
            for i, value in enumerate(["10", "12", "15"])
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")
        bt = Backtest.objects.create(
            name="Portfolio latent PnL",
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
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        portfolio = result["portfolio"]["kpi"]
        portfolio_daily = result["portfolio"]["daily"]

        self.assertEqual(
            [Decimal(row["pnl_global"]) for row in portfolio_daily],
            [Decimal("0"), Decimal("20"), Decimal("50")],
        )
        for row in portfolio_daily:
            self.assertEqual(
                Decimal(row["pnl_global"]),
                Decimal(row["equity"]) - Decimal("1000"),
            )
        self.assertEqual(Decimal(portfolio["TOTAL_GAIN_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["TOTAL_LOSS_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal("0"))
        self.assertEqual(Decimal(portfolio_daily[-1]["equity"]), Decimal("1050"))

    def test_backtest_portfolio_bt_uses_equity_and_invested_for_single_closed_trade(self):
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
        portfolio = result["portfolio"]["kpi"]

        expected_bt = (Decimal(portfolio["equity_end"]) - Decimal(portfolio["invested_end"])) / Decimal(portfolio["invested_end"])
        self.assertEqual(Decimal(portfolio["BT"]), expected_bt)
        self.assertEqual(Decimal(portfolio["BMJ"]), expected_bt / Decimal(portfolio["NB_DAYS"]))

    def test_backtest_portfolio_pnl_matches_gain_plus_loss_for_multiple_closed_trades(self):
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
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal(portfolio["TOTAL_GAIN_AMOUNT"]) + Decimal(portfolio["TOTAL_LOSS_AMOUNT"]))

    def test_backtest_portfolio_bt_and_bmj_use_equity_model_across_multiple_tickers(self):
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
        portfolio = result["portfolio"]["kpi"]
        expected_bt = (Decimal(portfolio["equity_end"]) - Decimal(portfolio["invested_end"])) / Decimal(portfolio["invested_end"])
        self.assertEqual(Decimal(portfolio["BT"]), expected_bt)
        self.assertEqual(Decimal(portfolio["BMJ"]), expected_bt / Decimal(portfolio["NB_DAYS"]))

    def test_backtest_portfolio_total_pnl_is_not_final_equity_when_capital_total_is_zero(self):
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
        portfolio = result["portfolio"]["kpi"]

        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal(portfolio["TOTAL_GAIN_AMOUNT"]) + Decimal(portfolio["TOTAL_LOSS_AMOUNT"]))
        self.assertNotEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal(portfolio["FINAL_EQUITY"]))
        expected_bt = (Decimal(portfolio["equity_end"]) - Decimal(portfolio["invested_end"])) / Decimal(portfolio["invested_end"])
        self.assertEqual(Decimal(portfolio["BT"]), expected_bt)
        self.assertEqual(Decimal(portfolio["BMJ"]), expected_bt / Decimal(portfolio["NB_DAYS"]))
        for row in result["portfolio"]["daily"]:
            self.assertEqual(
                Decimal(row["pnl_global"]),
                Decimal(row["equity"]) - Decimal(row["invested"]),
            )

    def test_buy_candidate_with_zero_effective_capital_records_blocker(self):
        start = date(2024, 3, 14)
        self._create_bars_for_symbol(self.symbol, ["10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(2)
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")
        bt = Backtest.objects.create(
            name="Zero Effective Capital",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=1),
            capital_total=Decimal("0"),
            capital_per_ticker=Decimal("0"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        line = run_backtest(bt).results["tickers"][self.symbol.ticker]["lines"][0]
        explain = line["explain"]

        self.assertGreaterEqual(explain["buy_candidates"], 1)
        self.assertEqual(explain["buy_executed"], 0)
        self.assertIn("ZERO_EFFECTIVE_CAPITAL", explain["blocked_counts"])
        blocker = explain["last_blockers"][-1]
        self.assertEqual(blocker["code"], "ZERO_EFFECTIVE_CAPITAL")
        self.assertEqual(blocker["cash"], "0")
        self.assertEqual(blocker["quantity"], 0)

    def test_buy_candidate_with_budget_below_price_records_order_quantity_zero_blocker(self):
        start = date(2024, 3, 15)
        self._create_bars_for_symbol(self.symbol, ["10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(2)
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")
        bt = Backtest.objects.create(
            name="Quantity Zero",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=1),
            capital_total=Decimal("0"),
            capital_per_ticker=Decimal("5"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        line = run_backtest(bt).results["tickers"][self.symbol.ticker]["lines"][0]
        explain = line["explain"]

        self.assertGreaterEqual(explain["buy_candidates"], 1)
        self.assertEqual(explain["buy_executed"], 0)
        self.assertIn("ORDER_QUANTITY_ZERO", explain["blocked_counts"])
        blocker = explain["last_blockers"][-1]
        self.assertEqual(blocker["code"], "ORDER_QUANTITY_ZERO")
        self.assertEqual(blocker["cash"], "5")
        self.assertEqual(blocker["price"], "10.000000")
        self.assertEqual(blocker["quantity"], 0)

    def test_buy_candidate_with_invalid_execution_price_records_blocker(self):
        start = date(2024, 3, 18)
        self._create_bars_for_symbol(self.symbol, ["0", "0"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("0"), ratio_P=Decimal("1"))
            for i in range(2)
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")
        bt = Backtest.objects.create(
            name="Invalid Execution Price",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=1),
            capital_total=Decimal("0"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        line = run_backtest(bt).results["tickers"][self.symbol.ticker]["lines"][0]
        explain = line["explain"]

        self.assertGreaterEqual(explain["buy_candidates"], 1)
        self.assertEqual(explain["buy_executed"], 0)
        self.assertIn("INVALID_EXECUTION_PRICE", explain["blocked_counts"])
        blocker = explain["last_blockers"][-1]
        self.assertEqual(blocker["code"], "INVALID_EXECUTION_PRICE")
        self.assertEqual(blocker["price"], "0.000000")

    def test_buy_candidate_without_remaining_global_cash_records_insufficient_cash_blocker(self):
        start = date(2024, 3, 19)
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        self._create_bars_for_symbol(self.symbol, ["10", "10"], start=start)
        self._create_bars_for_symbol(other, ["10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("2"))
            for i in range(2)
        ] + [
            DailyMetric(symbol=other, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(2)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=other, scenario=self.scenario, date=start, alerts="A1"),
        ])
        bt = Backtest.objects.create(
            name="Insufficient Global Cash",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=1),
            capital_total=Decimal("100"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker, other.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        results = run_backtest(bt).results
        first_explain = results["tickers"][self.symbol.ticker]["lines"][0]["explain"]
        second_explain = results["tickers"][other.ticker]["lines"][0]["explain"]

        self.assertEqual(first_explain["buy_executed"], 1)
        self.assertGreater(second_explain["buy_candidates"], 0)
        self.assertEqual(second_explain["buy_executed"], 0)
        self.assertGreater(second_explain["blocked_counts"].get("INSUFFICIENT_CASH", 0), 0)
        self.assertTrue(
            any(blocker["code"] == "INSUFFICIENT_CASH" for blocker in second_explain["last_blockers"])
        )

    def test_backtest_portfolio_bt_uses_equity_model_with_forced_sell(self):
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
        portfolio = result["portfolio"]["kpi"]

        expected_bt = (Decimal(portfolio["equity_end"]) - Decimal(portfolio["invested_end"])) / Decimal(portfolio["invested_end"])
        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal(portfolio["TOTAL_GAIN_AMOUNT"]) + Decimal(portfolio["TOTAL_LOSS_AMOUNT"]))
        self.assertEqual(Decimal(portfolio["BT"]), expected_bt)
        self.assertEqual(Decimal(portfolio["BMJ"]), expected_bt / Decimal(portfolio["NB_DAYS"]))

    def test_backtest_portfolio_no_trade_uses_null_bt_and_bmj(self):
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

        self.assertEqual(Decimal(portfolio["TOTAL_PNL_AMOUNT"]), Decimal("0"))
        self.assertIsNone(portfolio["BT"])
        self.assertIsNone(portfolio["BMJ"])


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
        self.assertEqual(Decimal(result["portfolio"]["daily"][-1]["equity"]), Decimal("980"))
        self.assertEqual(Decimal(result["portfolio"]["daily"][-1]["pnl_global"]), Decimal("-20"))
        self.assertEqual(
            Decimal(result["portfolio"]["daily"][-1]["pnl_global"]),
            Decimal(portfolio["TOTAL_PNL_AMOUNT"]),
        )

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

    def test_null_sell_threshold_preserves_historical_slope_alerts(self):
        scenario = Scenario.objects.create(
            name="Slope Null Sell",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            nglobal=2,
            history_years=2,
        )

        slow_alerts = self._compute_alerts_with_slow_path(scenario, ["100", "112", "120.96"])
        fast_alerts = self._compute_alerts_with_fast_path(scenario, ["100", "112", "120.96"])

        self.assertEqual(slow_alerts, ["SPv,SPVv,SPv_basse,SPVv_basse"])
        self.assertEqual(fast_alerts, slow_alerts)

    def test_explicit_sell_threshold_changes_spv_and_spvv_only(self):
        scenario = Scenario.objects.create(
            name="Slope Explicit Sell",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            slope_sell_threshold=Decimal("0.05"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            nglobal=2,
            history_years=2,
        )

        slow_alerts = self._compute_alerts_with_slow_path(scenario, ["100", "112", "120.96"])
        fast_alerts = self._compute_alerts_with_fast_path(scenario, ["100", "112", "120.96"])

        self.assertEqual(slow_alerts, ["SPv_basse,SPVv_basse"])
        self.assertEqual(fast_alerts, slow_alerts)

    def test_explicit_low_sell_threshold_changes_spv_basse_and_spvv_basse(self):
        scenario = Scenario.objects.create(
            name="Slope Explicit Low Sell",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            slope_sell_threshold_basse=Decimal("0.05"),
            nglobal=2,
            history_years=2,
        )

        slow_alerts = self._compute_alerts_with_slow_path(scenario, ["100", "112", "120.96"])
        fast_alerts = self._compute_alerts_with_fast_path(scenario, ["100", "112", "120.96"])

        self.assertEqual(slow_alerts, ["SPv,SPVv"])
        self.assertEqual(fast_alerts, slow_alerts)

    def test_scenario_impactful_changes_detect_sell_threshold_updates(self):
        diff = scenario_impactful_changes(
            instance=self.scenario,
            cleaned_data={
                "slope_sell_threshold": Decimal("0.03"),
                "slope_sell_threshold_basse": Decimal("0.01"),
            },
        )
        self.assertIn("slope_sell_threshold", diff)
        self.assertIn("slope_sell_threshold_basse", diff)

    def test_game_impactful_changes_detect_sell_threshold_updates(self):
        game = GameScenario.objects.create(
            name="Game impact",
            active=True,
            study_days=1000,
            tradability_threshold=Decimal("0"),
            npente=100,
            slope_threshold=Decimal("0.1"),
            npente_basse=20,
            slope_threshold_basse=Decimal("0.02"),
            nglobal=20,
            presence_threshold_pct=Decimal("30"),
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
        diff = game_impactful_changes(
            instance=game,
            cleaned_data={
                "slope_sell_threshold": Decimal("0.03"),
                "slope_sell_threshold_basse": Decimal("0.01"),
            },
        )
        self.assertIn("slope_sell_threshold", diff)
        self.assertIn("slope_sell_threshold_basse", diff)

    def test_indicator_signature_changes_when_sell_thresholds_change(self):
        base = indicator_signature(self.scenario)
        self.scenario.slope_sell_threshold = Decimal("0.05")
        sell_main = indicator_signature(self.scenario)
        self.scenario.slope_sell_threshold_basse = Decimal("0.01")
        sell_low = indicator_signature(self.scenario)

        self.assertNotEqual(base, sell_main)
        self.assertNotEqual(sell_main, sell_low)

    def test_recent_high_drawdown_helper_true_case(self):
        state = compute_recent_high_drawdown_condition(
            previous_prices=["95", "100", "92", "91", "90", "88", "87", "86", "85", "84"],
            current_price="91",
            lookback_days=10,
            max_drop_pct=Decimal("-0.10"),
        )
        self.assertTrue(state["enabled"])
        self.assertTrue(state["sufficient_history"])
        self.assertTrue(state["passed"])
        self.assertEqual(state["recent_high"], Decimal("100"))
        self.assertEqual(state["threshold_price"], Decimal("90.00"))

    def test_recent_high_drawdown_helper_false_case(self):
        state = compute_recent_high_drawdown_condition(
            previous_prices=["95", "100", "92", "91", "90", "88", "87", "86", "85", "84"],
            current_price="89",
            lookback_days=10,
            max_drop_pct=Decimal("-0.10"),
        )
        self.assertTrue(state["enabled"])
        self.assertTrue(state["sufficient_history"])
        self.assertFalse(state["passed"])
        self.assertEqual(state["recent_high"], Decimal("100"))
        self.assertEqual(state["threshold_price"], Decimal("90.00"))

    def test_recent_high_drawdown_excludes_current_day_from_recent_high(self):
        state = compute_recent_high_drawdown_condition(
            previous_prices=["95", "100", "92", "91", "90", "88", "87", "86", "85", "84"],
            current_price="120",
            lookback_days=10,
            max_drop_pct=Decimal("-0.10"),
        )
        self.assertEqual(state["recent_high"], Decimal("100"))
        self.assertEqual(state["threshold_price"], Decimal("90.00"))
        self.assertTrue(state["passed"])

    def test_recent_high_drawdown_returns_false_when_history_is_insufficient(self):
        state = compute_recent_high_drawdown_condition(
            previous_prices=["95", "100"],
            current_price="99",
            lookback_days=3,
            max_drop_pct=Decimal("-0.10"),
        )
        self.assertTrue(state["enabled"])
        self.assertFalse(state["sufficient_history"])
        self.assertFalse(state["passed"])
        self.assertIsNone(state["recent_high"])
        self.assertIsNone(state["threshold_price"])

    def test_recent_high_drawdown_classic_threshold_boundaries_are_unchanged(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "80", "79.99", "80"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
        )
        self.assertEqual(alerts[3], ["RHD_OK"])
        self.assertEqual(alerts[4], ["RHD_FAIL"])
        self.assertEqual(alerts[5], ["RHD_OK"])

    def test_recent_high_drawdown_rebound_mode_requires_prior_fail(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "100", "108", "108"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.40"),
        )
        self.assertEqual(sum(1 for day_alerts in alerts if "RHD_OK" in day_alerts), 1)
        self.assertFalse(any("RHD_FAIL" in day_alerts for day_alerts in alerts))

    def test_recent_high_drawdown_rebound_mode_waits_for_sufficient_rebound(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "100", "79", "60", "63", "63"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.40"),
        )
        self.assertIn("RHD_FAIL", alerts[4])
        self.assertFalse(any("RHD_OK" in day_alerts for day_alerts in alerts[5:]))

    def test_recent_high_drawdown_rebound_mode_confirms_rebound(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "100", "79", "60", "65", "65"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.40"),
        )
        self.assertEqual(alerts[4], ["RHD_FAIL"])
        self.assertEqual(alerts[6], [])
        self.assertEqual(alerts[7], ["RHD_OK"])

    def test_recent_high_drawdown_rebound_mode_resets_confirmation_when_lost(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "100", "79", "60", "65", "63"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.40"),
        )
        self.assertEqual(alerts[6], [])
        self.assertEqual(alerts[7], [])

    def test_recent_high_drawdown_rebound_mode_respects_reentry_drawdown(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "100", "79", "60", "65", "65"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.30"),
        )
        self.assertEqual(alerts[4], ["RHD_FAIL"])
        self.assertFalse(any("RHD_OK" in day_alerts for day_alerts in alerts[5:]))

    def test_recent_high_drawdown_rebound_rearms_reference_after_ok(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            ["100", "100", "100", "100", "79", "60", "65", "65", "64"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.40"),
        )
        self.assertEqual(alerts[7], ["RHD_OK"])
        self.assertNotIn("RHD_FAIL", alerts[8])

    def test_recent_high_drawdown_rebound_handles_missing_prices(self):
        alerts = compute_recent_high_drawdown_alerts_for_series(
            [None, "100", None, "99"],
            lookback_days=3,
            max_drop_pct=Decimal("-0.20"),
            mode="rebound_confirmed",
            rebound_threshold=Decimal("0.08"),
            confirmation_days=2,
            reentry_max_drawdown=Decimal("0.40"),
        )
        self.assertEqual(len(alerts), 4)

    def test_recent_high_drawdown_disabled_by_default_produces_no_alerts(self):
        scenario = Scenario.objects.create(
            name="RHD Disabled",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            nglobal=2,
            history_years=2,
        )
        slow_map = self._compute_alert_map_with_slow_path(scenario, ["100", "100", "110", "105", "95", "108"])
        fast_map = self._compute_alert_map_with_fast_path(scenario, ["100", "100", "110", "105", "95", "108"])
        for alerts in list(slow_map.values()) + list(fast_map.values()):
            self.assertNotIn("RHD_OK", alerts or "")
            self.assertNotIn("RHD_FAIL", alerts or "")

    def test_recent_high_drawdown_alerts_are_generated_in_slow_and_fast_paths(self):
        scenario = Scenario.objects.create(
            name="RHD Alerts",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=2,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            nglobal=2,
            history_years=2,
        )
        closes = ["100", "100", "110", "105", "95", "108"]
        slow_map = self._compute_alert_map_with_slow_path(scenario, closes)
        fast_map = self._compute_alert_map_with_fast_path(scenario, closes)

        self.assertIn("RHD_OK", slow_map[date(2024, 1, 3)])
        self.assertNotIn("RHD_FAIL", slow_map.get(date(2024, 1, 4), ""))
        self.assertIn("RHD_FAIL", slow_map[date(2024, 1, 5)])
        self.assertIn("RHD_OK", slow_map[date(2024, 1, 6)])
        self.assertEqual(slow_map, fast_map)

    def test_recent_high_drawdown_rebound_alerts_match_slow_and_fast_paths(self):
        scenario = Scenario.objects.create(
            name="RHD Rebound Alerts",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=3,
            recent_high_drawdown_max_drop_pct=Decimal("-0.20"),
            rhd_ok_reactivation_mode="rebound_confirmed",
            rhd_ok_rebound_threshold=Decimal("0.08"),
            rhd_ok_confirmation_days=2,
            rhd_ok_reentry_max_drawdown=Decimal("0.40"),
            nglobal=2,
            history_years=2,
        )
        closes = ["100", "100", "100", "100", "79", "60", "65", "65", "64"]
        slow_map = self._compute_alert_map_with_slow_path(scenario, closes)
        fast_map = self._compute_alert_map_with_fast_path(scenario, closes)

        self.assertIn("RHD_OK", slow_map[date(2024, 1, 4)])
        self.assertIn("RHD_FAIL", slow_map[date(2024, 1, 5)])
        self.assertNotIn("RHD_OK", slow_map.get(date(2024, 1, 7), ""))
        self.assertIn("RHD_OK", slow_map[date(2024, 1, 8)])
        self.assertNotIn("RHD_FAIL", slow_map.get(date(2024, 1, 9), ""))
        self.assertEqual(slow_map, fast_map)

    def test_scenario_impactful_changes_detect_recent_high_drawdown_updates(self):
        diff = scenario_impactful_changes(
            instance=self.scenario,
            cleaned_data={
                "recent_high_drawdown_lookback_days": 10,
                "recent_high_drawdown_max_drop_pct": Decimal("-0.10"),
                "rhd_ok_reactivation_mode": "rebound_confirmed",
                "rhd_ok_rebound_threshold": Decimal("0.12"),
            },
        )
        self.assertIn("recent_high_drawdown_lookback_days", diff)
        self.assertIn("recent_high_drawdown_max_drop_pct", diff)
        self.assertIn("rhd_ok_reactivation_mode", diff)
        self.assertIn("rhd_ok_rebound_threshold", diff)

    def test_game_impactful_changes_detect_recent_high_drawdown_updates(self):
        game = GameScenario.objects.create(
            name="Game impact RHD",
            active=True,
            study_days=1000,
            tradability_threshold=Decimal("0"),
            npente=100,
            slope_threshold=Decimal("0.1"),
            npente_basse=20,
            slope_threshold_basse=Decimal("0.02"),
            nglobal=20,
            presence_threshold_pct=Decimal("30"),
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
        diff = game_impactful_changes(
            instance=game,
            cleaned_data={
                "recent_high_drawdown_lookback_days": 10,
                "recent_high_drawdown_max_drop_pct": Decimal("-0.10"),
                "rhd_ok_reactivation_mode": "rebound_confirmed",
                "rhd_ok_rebound_threshold": Decimal("0.12"),
            },
        )
        self.assertIn("recent_high_drawdown_lookback_days", diff)
        self.assertIn("recent_high_drawdown_max_drop_pct", diff)
        self.assertIn("rhd_ok_reactivation_mode", diff)
        self.assertIn("rhd_ok_rebound_threshold", diff)

    def test_indicator_signature_changes_when_recent_high_drawdown_changes(self):
        base = indicator_signature(self.scenario)
        self.scenario.recent_high_drawdown_lookback_days = 10
        lookback_signature = indicator_signature(self.scenario)
        self.scenario.recent_high_drawdown_max_drop_pct = Decimal("-0.10")
        threshold_signature = indicator_signature(self.scenario)
        self.scenario.rhd_ok_reactivation_mode = "rebound_confirmed"
        mode_signature = indicator_signature(self.scenario)
        self.scenario.rhd_ok_rebound_threshold = Decimal("0.12")
        rebound_signature = indicator_signature(self.scenario)

        self.assertNotEqual(base, lookback_signature)
        self.assertNotEqual(lookback_signature, threshold_signature)
        self.assertNotEqual(threshold_signature, mode_signature)
        self.assertNotEqual(mode_signature, rebound_signature)

    def test_explicit_sell_threshold_full_cycle_buy_then_sell(self):
        scenario = Scenario.objects.create(
            name="Slope Full Cycle",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            slope_sell_threshold=Decimal("0.05"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            slope_sell_threshold_basse=Decimal("0.05"),
            nglobal=2,
            history_years=2,
        )

        closes = ["100", "104", "116.48", "125.7984", "130.830336"]
        slow_map = self._compute_alert_map_with_slow_path(scenario, closes)
        fast_map = self._compute_alert_map_with_fast_path(scenario, closes)

        day_buy = date(2024, 1, 3)
        day_middle = date(2024, 1, 4)
        day_sell = date(2024, 1, 5)

        for alerts in (slow_map.get(day_buy, ""), fast_map.get(day_buy, "")):
            self.assertIn("SPa", alerts)
            self.assertIn("SPVa", alerts)
            self.assertIn("SPa_basse", alerts)
            self.assertIn("SPVa_basse", alerts)
            self.assertNotIn("SPv", alerts)
            self.assertNotIn("SPVv", alerts)

        for alerts in (slow_map.get(day_middle, ""), fast_map.get(day_middle, "")):
            self.assertNotIn("SPv", alerts)
            self.assertNotIn("SPVv", alerts)
            self.assertNotIn("SPv_basse", alerts)
            self.assertNotIn("SPVv_basse", alerts)

        for alerts in (slow_map.get(day_sell, ""), fast_map.get(day_sell, "")):
            self.assertIn("SPv", alerts)
            self.assertIn("SPVv", alerts)
            self.assertIn("SPv_basse", alerts)
            self.assertIn("SPVv_basse", alerts)

    def test_backtest_with_recent_high_drawdown_alone_enters_and_exits(self):
        scenario = Scenario.objects.create(
            name="BT RHD Alone",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=2,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            nglobal=2,
            history_years=2,
        )
        start = date(2024, 1, 1)
        symbol = Symbol.objects.create(ticker="RHDALONE", exchange="NYSE", active=True)
        self._create_bars_for_symbol(symbol, ["100", "100", "110", "105", "95", "108"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=symbol,
                scenario=scenario,
                date=start + timedelta(days=i),
                P=Decimal(v),
                ratio_P=Decimal("1"),
            )
            for i, v in enumerate(["100", "100", "110", "105", "95", "108"])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=2), alerts="RHD_OK"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=4), alerts="RHD_FAIL"),
        ])
        bt = Backtest.objects.create(
            name="RHD Alone",
            scenario=scenario,
            start_date=start,
            end_date=start + timedelta(days=5),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LEGACY_DAILY", "buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
            universe_snapshot=[symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )
        results = run_backtest(bt).results
        daily = results["tickers"][symbol.ticker]["lines"][0]["daily"]
        actions = [row["action"] for row in daily if row.get("action")]
        self.assertIn("BUY", actions, daily)
        self.assertIn("SELL", actions, daily)
        self.assertLess(actions.index("BUY"), actions.index("SELL"))
        self.assertEqual(results["tickers"][symbol.ticker]["lines"][0]["final"]["N"], 1)

        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(kpi_final["N"], 1)
        self.assertEqual(Decimal(kpi_final["BT"]), Decimal(results["tickers"][symbol.ticker]["lines"][0]["final"]["BT"]))

        bt_stateful = Backtest.objects.create(
            name="RHD Stateful Alone",
            scenario=scenario,
            start_date=start,
            end_date=start + timedelta(days=5),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
            universe_snapshot=[symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )
        stateful_results = run_backtest(bt_stateful).results
        stateful_daily = stateful_results["tickers"][symbol.ticker]["lines"][0]["daily"]
        stateful_actions = [row["action"] for row in stateful_daily if row.get("action")]
        self.assertIn("BUY", stateful_actions, stateful_daily)
        self.assertIn("SELL", stateful_actions, stateful_daily)
        self.assertLess(stateful_actions.index("BUY"), stateful_actions.index("SELL"))
        self.assertEqual(stateful_results["tickers"][symbol.ticker]["lines"][0]["final"]["N"], 1)

        stateful_kpi_final = run_backtest_kpi_only(bt_stateful)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(stateful_kpi_final["N"], 1)
        self.assertEqual(
            Decimal(stateful_kpi_final["BT"]),
            Decimal(stateful_results["tickers"][symbol.ticker]["lines"][0]["final"]["BT"]),
        )

    def test_recent_high_drawdown_latched_state_survives_warmup(self):
        scenario = Scenario.objects.create(
            name="BT RHD Warmup",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=2,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            nglobal=2,
            history_years=2,
        )
        first_day = date(2024, 3, 1)
        start = first_day + timedelta(days=2)
        symbol = Symbol.objects.create(ticker="RHDWARM", exchange="NYSE", active=True)
        closes = ["100", "110", "108", "109", "111"]
        self._create_bars_for_symbol(symbol, closes, start=first_day)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=symbol,
                scenario=scenario,
                date=first_day + timedelta(days=i),
                P=Decimal(v),
                ratio_P=Decimal("1"),
            )
            for i, v in enumerate(closes)
        ])
        Alert.objects.create(symbol=symbol, scenario=scenario, date=first_day + timedelta(days=1), alerts="RHD_OK")
        bt = Backtest.objects.create(
            name="RHD Warmup",
            scenario=scenario,
            start_date=start,
            end_date=first_day + timedelta(days=4),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
            universe_snapshot=[symbol.ticker],
            warmup_days=2,
            close_positions_at_end=True,
        )

        results = run_backtest(bt).results
        daily = results["tickers"][symbol.ticker]["lines"][0]["daily"]
        buy_rows = [row for row in daily if row.get("action") in {"BUY", "SELL+BUY"}]
        self.assertTrue(buy_rows, daily)
        self.assertEqual(buy_rows[0]["date"], str(start))

        full_final = results["tickers"][symbol.ticker]["lines"][0]["final"]
        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(full_final["N"], 1)
        self.assertEqual(kpi_final["N"], 1)
        self.assertEqual(Decimal(full_final["BT"]), Decimal(kpi_final["BT"]))

    def test_backtest_with_recent_high_drawdown_and_existing_state_condition_requires_both(self):
        scenario = Scenario.objects.create(
            name="BT RHD Combined",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=2,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            nglobal=2,
            history_years=2,
        )
        start = date(2024, 2, 1)
        symbol = Symbol.objects.create(ticker="RHDCOMB", exchange="NYSE", active=True)
        self._create_bars_for_symbol(symbol, ["100", "100", "110", "109", "95", "108"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=symbol,
                scenario=scenario,
                date=start + timedelta(days=i),
                P=Decimal(v),
                ratio_P=Decimal("1"),
            )
            for i, v in enumerate(["100", "100", "110", "109", "95", "108"])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=2), alerts="RHD_OK,A1"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=4), alerts="RHD_FAIL"),
        ])
        bt = Backtest.objects.create(
            name="RHD Combined",
            scenario=scenario,
            start_date=start,
            end_date=start + timedelta(days=5),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["RHD_OK", "A1"],
                "buy_logic": "AND",
                "sell": ["RHD_FAIL", "B1"],
                "sell_logic": "OR",
            }],
            universe_snapshot=[symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )
        results = run_backtest(bt).results
        daily = results["tickers"][symbol.ticker]["lines"][0]["daily"]
        actions = [row["action"] for row in daily if row.get("action")]
        self.assertIn("BUY", actions, daily)
        self.assertIn("SELL", actions, daily)
        self.assertLess(actions.index("BUY"), actions.index("SELL"))

    def test_rhd_ok_reactivates_after_sell_and_allows_later_spa_buy(self):
        scenario = Scenario.objects.create(
            name="BT RHD Reactivation",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=2,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            nglobal=2,
            history_years=2,
        )
        start = date(2024, 3, 1)
        symbol = Symbol.objects.create(ticker="RHDREACT", exchange="NYSE", active=True)
        closes = ["100", "101", "102", "103", "104", "105", "106", "107"]
        self._create_bars_for_symbol(symbol, closes, start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=symbol,
                scenario=scenario,
                date=start + timedelta(days=i),
                P=Decimal(v),
                ratio_P=Decimal("1"),
            )
            for i, v in enumerate(closes)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=symbol, scenario=scenario, date=start, alerts="RHD_OK"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=1), alerts="SPa"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=2), alerts="SPVv"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=4), alerts="RHD_FAIL"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=5), alerts="RHD_OK"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=6), alerts="SPa"),
        ])
        bt = Backtest.objects.create(
            name="RHD Reactivation",
            scenario=scenario,
            start_date=start,
            end_date=start + timedelta(days=7),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["SPa", "RHD_OK"],
                "buy_logic": "AND",
                "sell": ["SPVv", "RHD_FAIL"],
                "sell_logic": "OR",
            }],
            universe_snapshot=[symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        results = run_backtest(bt).results
        daily = results["tickers"][symbol.ticker]["lines"][0]["daily"]
        buy_dates = [row["date"] for row in daily if row.get("action") in {"BUY", "SELL+BUY"}]
        sell_dates = [row["date"] for row in daily if row.get("action") in {"SELL", "SELL+BUY"}]
        self.assertEqual(buy_dates, [str(start + timedelta(days=1)), str(start + timedelta(days=6))], daily)
        self.assertEqual(sell_dates, [str(start + timedelta(days=2))], daily)

        kpi_daily_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(kpi_daily_final["N"], 1)
        self.assertEqual(
            Decimal(kpi_daily_final["BT"]),
            Decimal(results["tickers"][symbol.ticker]["lines"][0]["final"]["BT"]),
        )

    def test_implicit_stateful_rhd_ok_reactivation_allows_later_spa_buy_after_spv_invalidations(self):
        scenario = Scenario.objects.create(
            name="BT RHD Implicit Reactivation",
            active=True,
            a=1,
            b=0,
            c=0,
            d=0,
            e=1,
            n1=2,
            n2=2,
            npente=1,
            slope_threshold=Decimal("0.10"),
            npente_basse=1,
            slope_threshold_basse=Decimal("0.10"),
            recent_high_drawdown_lookback_days=2,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            nglobal=2,
            history_years=2,
        )
        start = date(2024, 4, 1)
        symbol = Symbol.objects.create(ticker="RHDIMPL", exchange="NYSE", active=True)
        closes = ["100", "101", "102", "103", "104", "105", "106", "107", "108"]
        self._create_bars_for_symbol(symbol, closes, start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=symbol,
                scenario=scenario,
                date=start + timedelta(days=i),
                P=Decimal(v),
                ratio_P=Decimal("1"),
            )
            for i, v in enumerate(closes)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=symbol, scenario=scenario, date=start, alerts="RHD_OK"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=1), alerts="SPa"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=2), alerts="SPVv,SPv"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=4), alerts="SPVv,SPv"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=5), alerts="RHD_FAIL,SPv"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=6), alerts="RHD_OK"),
            Alert(symbol=symbol, scenario=scenario, date=start + timedelta(days=7), alerts="SPa"),
        ])
        bt = Backtest.objects.create(
            name="RHD Implicit Reactivation",
            scenario=scenario,
            start_date=start,
            end_date=start + timedelta(days=8),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "buy": ["SPa", "RHD_OK"],
                "buy_logic": "AND",
                "sell": ["SPVv", "RHD_FAIL"],
                "sell_logic": "OR",
            }],
            universe_snapshot=[symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        results = run_backtest(bt).results
        daily = results["tickers"][symbol.ticker]["lines"][0]["daily"]
        buy_dates = [row["date"] for row in daily if row.get("action") in {"BUY", "SELL+BUY"}]
        sell_dates = [row["date"] for row in daily if row.get("action") in {"SELL", "SELL+BUY"}]
        self.assertEqual(buy_dates, [str(start + timedelta(days=1)), str(start + timedelta(days=7))], daily)
        self.assertEqual(sell_dates, [str(start + timedelta(days=2))], daily)

        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(kpi_final["N"], 1)
        self.assertEqual(
            Decimal(kpi_final["BT"]),
            Decimal(results["tickers"][symbol.ticker]["lines"][0]["final"]["BT"]),
        )

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

    def test_global_momentum_uses_bounded_relative_returns_and_regime_sign(self):
        metrics_by_ticker = {
            "AAA": {
                date(2024, 1, 1): Decimal("100"),
                date(2024, 1, 2): Decimal("110"),
                date(2024, 1, 3): Decimal("99"),
            },
            "BBB": {
                date(2024, 1, 1): Decimal("200"),
                date(2024, 1, 2): Decimal("220"),
                date(2024, 1, 3): Decimal("198"),
            },
        }
        values = compute_global_momentum_values_by_date(metrics_by_ticker, nglobal=1)
        self.assertEqual(values[date(2024, 1, 2)], Decimal("0.1"))
        self.assertEqual(values[date(2024, 1, 3)], Decimal("-0.1"))

        regimes = build_global_momentum_regime_by_date(metrics_by_ticker, nglobal=1, neutral_band=Decimal("0.001"))
        self.assertEqual(regimes[date(2024, 1, 2)], "GM_POS")
        self.assertEqual(regimes[date(2024, 1, 3)], "GM_NEG")

    def test_global_momentum_clamps_extreme_positive_return_when_base_price_is_near_zero(self):
        metrics_by_ticker = {
            "AAA": {
                date(2024, 1, 1): Decimal("0.0001"),
                date(2024, 1, 2): Decimal("10.0001"),
            },
            "BBB": {
                date(2024, 1, 1): Decimal("5"),
                date(2024, 1, 2): Decimal("5"),
            },
        }
        values = compute_global_momentum_values_by_date(metrics_by_ticker, nglobal=1)
        self.assertEqual(values[date(2024, 1, 2)], Decimal("0.5"))

    def test_global_momentum_clamps_extreme_negative_return_to_minus_one(self):
        metrics_by_ticker = {
            "AAA": {
                date(2024, 1, 1): Decimal("10"),
                date(2024, 1, 2): Decimal("0.0001"),
            },
            "BBB": {
                date(2024, 1, 1): Decimal("5"),
                date(2024, 1, 2): Decimal("5"),
            },
        }
        values = compute_global_momentum_values_by_date(metrics_by_ticker, nglobal=1)
        self.assertEqual(values[date(2024, 1, 2)], Decimal("-0.499995"))

    def test_global_momentum_regime_is_neutral_within_default_band(self):
        metrics_by_ticker = {
            "AAA": {
                date(2024, 1, 1): Decimal("1000"),
                date(2024, 1, 2): Decimal("1000.5"),
            },
            "BBB": {
                date(2024, 1, 1): Decimal("1000"),
                date(2024, 1, 2): Decimal("999.5"),
            },
        }
        regimes = build_global_momentum_regime_by_date(metrics_by_ticker, nglobal=1)
        self.assertEqual(regimes[date(2024, 1, 2)], "GM_NEU")

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


    def test_run_backtest_blocks_rebuy_after_sell_without_fresh_signal_reactivation(self):
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
            name="No Immediate Rebuy After Sell",
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

    def test_run_backtest_target_model_blocks_same_day_reentry_after_sell(self):
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
            name="Latch Same Day Reentry Blocked",
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

    def test_latch_sell_keeps_non_invalidated_buy_signal_after_sell(self):
        start, bt = self._create_selective_latch_retention_fixture()
        gm_regimes = self._gm_regime_map(start, {2: "GM_POS", 5: "GM_POS"})

        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[2]["action"], "BUY")
        self.assertEqual(daily[4]["action"], "SELL")
        self.assertNotIn("C1", daily[5]["alerts"])
        self.assertEqual(daily[5]["action"], "BUY")

    def test_latch_rebuy_occurs_when_only_invalidated_signal_reactivates_after_sell(self):
        start, bt = self._create_selective_latch_retention_fixture()
        gm_regimes = self._gm_regime_map(start, {2: "GM_POS", 5: "GM_POS"})

        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]
        buy_days = [row["date"] for row in daily if row.get("action") == "BUY"]

        self.assertEqual(buy_days, [str(bt.start_date + timedelta(days=2)), str(bt.start_date + timedelta(days=5))])
        self.assertEqual(line["final"]["N"], 1)

    def test_gm_never_triggers_sell_in_latch_mode(self):
        start, bt = self._create_selective_latch_retention_fixture(
            prices=["10", "11", "13", "12", "11.9"],
            alerts_by_offset={
                1: "A1",
                2: "C1",
            },
            k1_by_offset={
                0: "-1",
                1: "1",
                2: "1",
                3: "1",
                4: "1",
            },
            slope_vrai_by_offset={
                0: "0.10",
                1: "0.15",
                2: "0.25",
                3: "0.24",
                4: "0.23",
            },
        )
        gm_regimes = self._gm_regime_map(start, {2: "GM_POS", 3: "GM_NEG", 4: "GM_NEG"})

        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        actions = [row.get("action") for row in line["daily"]]

        self.assertIn("BUY", actions)
        self.assertNotIn("SELL", {action for action in actions if action})
        self.assertEqual(line["final"]["N"], 0)

    def test_latch_rebuy_still_blocked_when_gm_filter_invalid(self):
        start, bt = self._create_selective_latch_retention_fixture(
            prices=["10", "11", "13", "12.5", "11.5", "10.5", "12"],
            alerts_by_offset={
                1: "A1",
                2: "C1",
                4: "B1",
                5: "A1",
            },
            k1_by_offset={
                0: "-1",
                1: "1",
                2: "1",
                3: "1",
                4: "-1",
                5: "1",
                6: "1",
            },
            slope_vrai_by_offset={
                0: "0.10",
                1: "0.15",
                2: "0.25",
                3: "0.24",
                4: "0.23",
                5: "0.24",
                6: "0.24",
            },
        )
        gm_regimes = self._gm_regime_map(start, {2: "GM_POS", 5: "GM_NEG", 6: "GM_POS"})

        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[2]["action"], "BUY")
        self.assertEqual(daily[4]["action"], "SELL")
        self.assertIsNone(daily[5]["action"])
        self.assertEqual(daily[6]["action"], "BUY")

    def test_latch_real_case_a1_spva_gmpos_rebuys_without_fresh_spva_after_b1_sell(self):
        start = date(2024, 12, 16)
        self.scenario.nglobal = 1
        self.scenario.slope_threshold = Decimal("0.2")
        self.scenario.save(update_fields=["nglobal", "slope_threshold"])
        self._create_bars_for_symbol(self.symbol, ["10", "11", "13", "12.5", "11.5", "13"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(
                symbol=self.symbol,
                scenario=self.scenario,
                date=start + timedelta(days=i),
                P=Decimal(price),
                ratio_P=Decimal("1"),
                K1=Decimal(k1),
                slope_vrai=Decimal(slope),
            )
            for i, (price, k1, slope) in enumerate([
                ("10", "-1", "0.10"),
                ("11", "1", "0.15"),
                ("13", "1", "0.25"),
                ("12.5", "1", "0.24"),
                ("11.5", "-1", "0.23"),
                ("13", "1", "0.24"),
            ])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="SPVa"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=4), alerts="B1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=5), alerts="A1"),
        ])

        bt = Backtest.objects.create(
            name="A1 SPVa GM_POS Selective Rebuy",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=5),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LATCH_STATEFUL",
                "buy": ["A1", "SPVa"],
                "buy_logic": "AND",
                "sell": [],
                "buy_gm_filter": "GM_POS",
            }],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        gm_regimes = self._gm_regime_map(start, {2: "GM_POS", 5: "GM_POS"})
        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(daily[2]["action"], "BUY")
        self.assertEqual(daily[4]["action"], "SELL")
        self.assertEqual(daily[5]["action"], "BUY")

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

    def test_missing_trading_model_uses_progressive_auto_sell_routing(self):
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

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "PROGRESSIVE_AUTO_SELL")
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

    def test_progressive_auto_sell_accumulates_buy_and_sells_on_invalidation(self):
        start = self._create_latch_routing_fixture()
        bt = Backtest.objects.create(
            name="Progressive Auto Sell",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "PROGRESSIVE_AUTO_SELL", "buy": ["A1", "C1"], "buy_logic": "AND", "sell": []}],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "PROGRESSIVE_AUTO_SELL")
        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")

    def test_progressive_explicit_sell_accumulates_sell_codes_with_and_logic(self):
        start = date(2024, 11, 1)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(4)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="B1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="D1"),
        ])
        bt = Backtest.objects.create(
            name="Progressive Explicit Sell AND",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=3),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "PROGRESSIVE_EXPLICIT_SELL",
                "buy": ["A1"],
                "buy_logic": "AND",
                "sell": ["B1", "D1"],
                "sell_logic": "AND",
            }],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        daily = result["tickers"][self.symbol.ticker]["lines"][0]["daily"]

        self.assertEqual(daily[0]["action"], "BUY")
        self.assertIsNone(daily[1]["action"])
        self.assertEqual(daily[2]["action"], "SELL")

    def test_progressive_explicit_sell_retains_buy_memory_consumes_sell_memory_and_warns_on_reentry(self):
        start = date(2024, 11, 10)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(3)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="D1"),
        ])
        bt = Backtest.objects.create(
            name="Progressive Explicit Sell Reentry Warning",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "PROGRESSIVE_EXPLICIT_SELL",
                "buy": ["A1"],
                "buy_logic": "AND",
                "sell": ["D1"],
                "sell_logic": "OR",
            }],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]

        self.assertEqual(daily[0]["action"], "BUY")
        self.assertEqual(daily[1]["action"], "SELL+BUY")
        self.assertIsNone(daily[2]["action"])
        self.assertEqual(line["final"]["N"], 1)
        self.assertEqual(line["warning_count"], 1)
        self.assertEqual(result["meta"]["warning_count"], 1)
        self.assertEqual(line["warnings"][0]["code"], "IMMEDIATE_REENTRY")

    def test_progressive_explicit_sell_kpi_only_matches_reentry_warning_behavior(self):
        start = date(2024, 11, 20)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(3)
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=1), alerts="D1"),
        ])
        bt = Backtest.objects.create(
            name="Progressive Explicit Sell KPI",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "PROGRESSIVE_EXPLICIT_SELL",
                "buy": ["A1"],
                "buy_logic": "AND",
                "sell": ["D1"],
                "sell_logic": "OR",
            }],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest_kpi_only(bt)
        line = result[self.symbol.ticker]["lines"][0]

        self.assertEqual(line["final"]["N"], 1)
        self.assertEqual(line["warning_count"], 1)
        self.assertEqual(line["warnings"][0]["code"], "IMMEDIATE_REENTRY")

    def test_progressive_explicit_sell_gm_market_exit_warns_on_immediate_reentry(self):
        start = date(2024, 11, 30)
        self._create_bars_for_symbol(self.symbol, ["10", "10", "10"], start=start)
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", active=True)
        self._create_bars_for_symbol(spy, ["100", "110", "90"], start=start)
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal("10"), ratio_P=Decimal("1"))
            for i in range(3)
        ])
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1")
        bt = Backtest.objects.create(
            name="Progressive Explicit Sell GM Reentry Warning",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "PROGRESSIVE_EXPLICIT_SELL",
                "buy": ["A1"],
                "buy_logic": "AND",
                "sell": [],
                "sell_logic": "OR",
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG"},
                },
            }],
            universe_snapshot=[self.symbol.ticker],
            warmup_days=0,
            close_positions_at_end=False,
        )

        result = run_backtest(bt).results
        line = result["tickers"][self.symbol.ticker]["lines"][0]
        daily = line["daily"]
        events = line["events"]

        self.assertEqual(daily[0]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL+BUY")
        self.assertIn("Protection marché GM", daily[2]["action_reason"])
        self.assertEqual(events[1]["action"], "SELL")
        self.assertIn("Protection marché GM", events[1]["action_reason"])
        self.assertEqual(line["warning_count"], 1)
        self.assertEqual(result["meta"]["warning_count"], 1)
        self.assertEqual(line["warnings"][0]["code"], "IMMEDIATE_REENTRY")

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

    def test_supported_progressive_signals_use_progressive_auto_sell_by_default(self):
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

        self.assertEqual(result["meta"]["signal_lines"][0]["trading_model"], "PROGRESSIVE_AUTO_SELL")
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

    def test_game_runner_keeps_tradability_semantics_on_buy_threshold_only(self):
        game = GameScenario.objects.create(
            name="Daily Game Sell Threshold",
            active=True,
            study_days=30,
            tradability_threshold=Decimal("0.3"),
            presence_threshold_pct=Decimal("30"),
            npente=100,
            slope_threshold=Decimal("0.1"),
            slope_sell_threshold=Decimal("0.5"),
            npente_basse=20,
            slope_threshold_basse=Decimal("0.02"),
            slope_sell_threshold_basse=Decimal("0.01"),
            recent_high_drawdown_lookback_days=10,
            recent_high_drawdown_max_drop_pct=Decimal("-0.20"),
            rhd_ok_reactivation_mode="rebound_confirmed",
            rhd_ok_rebound_threshold=Decimal("0.08"),
            rhd_ok_confirmation_days=2,
            rhd_ok_reentry_max_drawdown=Decimal("0.40"),
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
        self._create_bars_for_symbol(self.symbol, ["10", "11", "12"])

        fake_depth = SimpleNamespace(needs_full_recompute=lambda: False, missing_symbol_ids=[], total_symbols=1)
        fake_out = {
            self.symbol.ticker: {
                "best_bmd": "0.004",
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
            run_game_scenario_now(game.id)

        game.refresh_from_db()
        row = game.today_results["rows"][0]
        self.assertTrue(row["ok"], row)
        self.assertEqual(game.engine_scenario.slope_sell_threshold, Decimal("0.50000000"))
        self.assertEqual(game.engine_scenario.slope_sell_threshold_basse, Decimal("0.01000000"))

    def test_game_runtime_scenario_sync_helper_is_authoritative_for_both_call_paths(self):
        common = dict(
            name="Game Sync Equivalence",
            active=True,
            study_days=1000,
            tradability_threshold=Decimal("0"),
            presence_threshold_pct=Decimal("30"),
            npente=100,
            slope_threshold=Decimal("0.10"),
            slope_sell_threshold=Decimal("0.05"),
            npente_basse=20,
            slope_threshold_basse=Decimal("0.02"),
            slope_sell_threshold_basse=Decimal("0.01"),
            nglobal=20,
            a=Decimal("1"),
            b=Decimal("2"),
            c=Decimal("3"),
            d=Decimal("4"),
            e=Decimal("5"),
            vc=Decimal("0.40"),
            fl=Decimal("0.60"),
            n1=5,
            n2=3,
            n3=7,
            n4=9,
            n5=100,
            k2j=10,
            cr=Decimal("10"),
            n5f3=80,
            crf3=Decimal("12"),
            nampL3=70,
            baseL3=Decimal("0.03"),
            periodeL3=90,
            m_v=Decimal("1.25"),
            capital_total=Decimal("10000"),
            capital_per_ticker=Decimal("1000"),
            capital_mode="FIXED",
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
        )
        game_runner = GameScenario.objects.create(**common)
        game_task = GameScenario.objects.create(**common)

        sc_from_helper = sync_game_engine_scenario(game_runner)
        sc_from_runner_wrapper = _sync_engine_scenario(game_runner)
        sc_from_task_wrapper = _ensure_game_engine_scenario(game_task)

        for field_name in GAME_RUNTIME_SCENARIO_FIELDS:
            self.assertEqual(getattr(sc_from_helper, field_name), getattr(game_runner, field_name), field_name)
            self.assertEqual(getattr(sc_from_runner_wrapper, field_name), getattr(sc_from_helper, field_name), field_name)
            self.assertEqual(getattr(sc_from_task_wrapper, field_name), getattr(sc_from_helper, field_name), field_name)

        self.assertEqual(sc_from_runner_wrapper.name, f"[GAME] {game_runner.name}")
        self.assertEqual(sc_from_task_wrapper.name, f"[GAME] {game_task.name}")
        self.assertEqual(sc_from_runner_wrapper.description, f"Auto-generated scenario for GameScenario #{game_runner.id}")
        self.assertEqual(sc_from_task_wrapper.description, f"Auto-generated scenario for GameScenario #{game_task.id}")


class CouloirStatefulSignalTests(TestCase):
    def _make_scenario(self, name="Couloir Scenario"):
        return Scenario.objects.create(
            name=name,
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=2,
            n2=2,
            nglobal=2,
        )

    def _make_symbol_with_bars(self, closes, *, ticker="COUL"):
        symbol = Symbol.objects.create(ticker=f"{ticker}{Symbol.objects.count()}", exchange="NYSE", active=True)
        start = date(2024, 1, 1)
        bars = []
        for idx, close in enumerate(closes):
            close_dec = Decimal(str(close))
            bars.append(DailyBar(
                symbol=symbol,
                date=start + timedelta(days=idx),
                open=close_dec,
                high=close_dec,
                low=close_dec,
                close=close_dec,
                volume=1000 + idx,
            ))
        DailyBar.objects.bulk_create(bars)
        return symbol, [bar.date for bar in bars]

    def _couloir_line(self, **overrides):
        line = {
            "buy": ["COULOIR"],
            "sell": [],
            "couloir_initial_low_lookback_days": 3,
            "couloir_buy_rebound_threshold": "0.10",
            "couloir_sell_drawdown_threshold": "0.10",
            "couloir_buy_confirmation_days": 1,
            "couloir_sell_confirmation_days": 1,
        }
        line.update(overrides)
        return line

    def _make_backtest(self, closes, *, line=None, start_index=0, close_positions_at_end=False):
        scenario = self._make_scenario()
        symbol, dates = self._make_symbol_with_bars(closes)
        start_date = dates[start_index]
        warmup_days = (start_date - dates[0]).days
        bt = Backtest(
            name="Couloir BT",
            scenario=scenario,
            start_date=start_date,
            end_date=dates[-1],
            capital_total=0,
            capital_per_ticker=Decimal("10000"),
            include_all_tickers=True,
            signal_lines=[line or self._couloir_line()],
            warmup_days=warmup_days,
            close_positions_at_end=close_positions_at_end,
            universe_snapshot=[symbol.ticker],
        )
        return bt, symbol

    def _actions(self, bt, symbol):
        result = run_backtest(bt).results
        daily = result["tickers"][symbol.ticker]["lines"][0]["daily"]
        return [(row["date"], row.get("action"), row.get("price_close"), row.get("action_reason")) for row in daily if row.get("action")]

    def test_couloir_first_buy_requires_rebound_and_confirmation(self):
        bt, symbol = self._make_backtest(
            ["50", "52", "54", "54", "55", "55"],
            line=self._couloir_line(couloir_buy_confirmation_days=2),
        )
        actions = self._actions(bt, symbol)
        self.assertEqual(actions[0][1], "BUY")
        self.assertEqual(actions[0][2], "55.000000")

    def test_couloir_does_not_buy_when_rebound_is_insufficient(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "54", "54"])
        self.assertEqual(self._actions(bt, symbol), [])

    def test_couloir_buy_confirmation_resets_when_price_falls_back(self):
        bt, symbol = self._make_backtest(
            ["50", "52", "54", "55", "54", "55"],
            line=self._couloir_line(couloir_buy_confirmation_days=2),
        )
        self.assertEqual(self._actions(bt, symbol), [])

    def test_couloir_buys_when_threshold_already_holds_after_history(self):
        bt, symbol = self._make_backtest(
            ["50", "61", "62", "63"],
            line=self._couloir_line(couloir_buy_rebound_threshold="0.20"),
        )

        actions = self._actions(bt, symbol)

        self.assertTrue(actions)
        self.assertEqual(actions[0][1:3], ("BUY", "62.000000"))

    def test_couloir_lower_threshold_is_not_less_permissive_than_higher_threshold(self):
        closes = ["50", "61", "62", "65"]
        lower_bt, lower_symbol = self._make_backtest(
            closes,
            line=self._couloir_line(couloir_buy_rebound_threshold="0.20"),
        )
        higher_bt, higher_symbol = self._make_backtest(
            closes,
            line=self._couloir_line(couloir_buy_rebound_threshold="0.30"),
        )

        lower_actions = self._actions(lower_bt, lower_symbol)
        higher_actions = self._actions(higher_bt, higher_symbol)

        self.assertTrue(lower_actions)
        self.assertTrue(higher_actions)
        self.assertEqual(lower_actions[0][1], "BUY")
        self.assertEqual(higher_actions[0][1], "BUY")
        self.assertLessEqual(lower_actions[0][0], higher_actions[0][0])

    def test_couloir_retries_gm_blocked_buy_when_condition_stays_true(self):
        line = self._couloir_line(
            couloir_buy_rebound_threshold="0.20",
            gm_buy_conditions={
                "operator": "AND",
                "current": {"mode": "GM_POS"},
                "market": {"mode": "IGNORE"},
                "sector": {"mode": "IGNORE"},
            },
        )
        bt, symbol = self._make_backtest(["50", "61", "62", "62", "62"], line=line)
        start = date(2024, 1, 1)
        gm_regimes = {
            start + timedelta(days=2): "GM_NEG",
            start + timedelta(days=3): "GM_NEG",
            start + timedelta(days=4): "GM_POS",
        }

        with patch("core.services.backtesting.engine._build_global_momentum_regime_from_values", return_value=gm_regimes):
            daily = self._daily_rows(bt, symbol)

        blocked = [row for row in daily if row.get("couloir_blocked_reason") == "GM"]
        buy_row = next(row for row in daily if row.get("action") == "BUY")

        self.assertTrue(blocked)
        self.assertTrue(all(row["couloir_buy_candidate"] for row in blocked))
        self.assertTrue(all(not row["couloir_buy_executed"] for row in blocked))
        self.assertTrue(all(row["couloir_state"] == "OUT" for row in blocked))
        self.assertEqual(buy_row["date"], str(start + timedelta(days=4)))
        self.assertTrue(buy_row["couloir_buy_candidate"])
        self.assertTrue(buy_row["couloir_buy_executed"])

    def test_couloir_sell_after_high_and_sell_without_higher_high(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "100", "120", "108"])
        actions = self._actions(bt, symbol)
        self.assertEqual([(a[1], a[2]) for a in actions], [("BUY", "55.000000"), ("SELL", "108.000000")])

        bt2, symbol2 = self._make_backtest(["50", "52", "54", "55", "100", "90"])
        actions2 = self._actions(bt2, symbol2)
        self.assertEqual([(a[1], a[2]) for a in actions2], [("BUY", "55.000000"), ("SELL", "90.000000")])

    def test_couloir_rebuys_after_real_sell_from_new_low(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "100", "120", "108", "50", "55"])
        actions = self._actions(bt, symbol)
        self.assertEqual([(a[1], a[2]) for a in actions], [("BUY", "55.000000"), ("SELL", "108.000000"), ("BUY", "55.000000")])

    def test_couloir_gm_blocked_buy_keeps_state_out(self):
        line = self._couloir_line(gm_buy_conditions={
            "operator": "AND",
            "current": {"mode": "GM_POS", "threshold": "0.50", "explicit_threshold": True},
            "market": {"mode": "IGNORE"},
            "sector": {"mode": "IGNORE"},
        })
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "55", "54", "55"], line=line)
        self.assertEqual(self._actions(bt, symbol), [])

    def test_couloir_gm_exit_resets_low_since_real_sell(self):
        line = self._couloir_line(gm_sell_market_exit_conditions={
            "operator": "AND",
            "current": {"mode": "IGNORE"},
            "market": {"mode": "GM_NEG"},
            "sector": {"mode": "IGNORE"},
        })
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "100", "95", "50", "55"], line=line)
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", active=True)
        start = date(2024, 1, 1)
        DailyBar.objects.bulk_create([
            DailyBar(
                symbol=spy,
                date=start + timedelta(days=idx),
                open=Decimal(close),
                high=Decimal(close),
                low=Decimal(close),
                close=Decimal(close),
                volume=1000 + idx,
            )
            for idx, close in enumerate(["100", "100", "100", "100", "100", "90", "90", "90"])
        ])
        actions = self._actions(bt, symbol)
        self.assertEqual(actions[0][1:3], ("BUY", "55.000000"))
        self.assertEqual(actions[1][1], "SELL")
        self.assertIn("Protection marché GM", actions[1][3])
        self.assertEqual(actions[2][1:3], ("BUY", "55.000000"))

    def _daily_rows(self, bt, symbol, *, large_result_mode=False):
        result = run_backtest(bt, large_result_mode=large_result_mode).results
        return result["tickers"][symbol.ticker]["lines"][0]["daily"]

    def test_couloir_debug_trace_records_refs_thresholds_and_executions(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "100", "120", "108"])
        daily = self._daily_rows(bt, symbol)

        first = daily[0]
        self.assertEqual(first["couloir_state"], "OUT")
        self.assertEqual(Decimal(first["couloir_low_ref"]), Decimal("50"))
        self.assertEqual(Decimal(first["couloir_buy_threshold_price"]), Decimal("55"))

        buy_row = next(row for row in daily if row.get("action") == "BUY")
        self.assertTrue(buy_row["couloir_buy_candidate"])
        self.assertTrue(buy_row["couloir_buy_executed"])
        self.assertEqual(buy_row["couloir_state"], "IN")
        self.assertEqual(Decimal(buy_row["couloir_high_ref"]), Decimal("55"))

        sell_row = next(row for row in daily if row.get("action") == "SELL")
        self.assertTrue(sell_row["couloir_sell_candidate"])
        self.assertTrue(sell_row["couloir_sell_executed"])
        self.assertEqual(sell_row["couloir_sell_source"], "COULOIR")
        self.assertTrue(sell_row["couloir_reset_after_sell"])
        self.assertEqual(sell_row["couloir_state"], "OUT")
        self.assertEqual(Decimal(sell_row["couloir_sell_threshold_price"]), Decimal("108"))

    def test_couloir_debug_trace_marks_gm_blocked_buy(self):
        line = self._couloir_line(gm_buy_conditions={
            "operator": "AND",
            "current": {"mode": "GM_POS", "threshold": "0.50", "explicit_threshold": True},
            "market": {"mode": "IGNORE"},
            "sector": {"mode": "IGNORE"},
        })
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "55", "54", "55"], line=line)
        daily = self._daily_rows(bt, symbol)

        blocked = [row for row in daily if row.get("couloir_blocked_reason") == "GM"]
        self.assertTrue(blocked)
        self.assertTrue(blocked[0]["couloir_buy_candidate"])
        self.assertFalse(blocked[0]["couloir_buy_executed"])
        self.assertEqual(blocked[0]["couloir_state"], "OUT")

    def test_couloir_debug_trace_is_absent_for_classic_and_large_results(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "55"], line={"buy": ["A1"], "sell": ["B1"]})
        classic_daily = self._daily_rows(bt, symbol)
        self.assertTrue(classic_daily)
        self.assertNotIn("couloir_state", classic_daily[0])

        couloir_bt, couloir_symbol = self._make_backtest(["50", "52", "54", "55"])
        large_daily = self._daily_rows(couloir_bt, couloir_symbol, large_result_mode=True)
        self.assertEqual(large_daily, [])

    def test_couloir_debug_trace_marks_gm_exit_reset(self):
        line = self._couloir_line(gm_sell_market_exit_conditions={
            "operator": "AND",
            "current": {"mode": "IGNORE"},
            "market": {"mode": "GM_NEG"},
            "sector": {"mode": "IGNORE"},
        })
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "100", "95", "50", "55"], line=line)
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", active=True)
        start = date(2024, 1, 1)
        DailyBar.objects.bulk_create([
            DailyBar(
                symbol=spy,
                date=start + timedelta(days=idx),
                open=Decimal(close),
                high=Decimal(close),
                low=Decimal(close),
                close=Decimal(close),
                volume=1000 + idx,
            )
            for idx, close in enumerate(["100", "100", "100", "100", "100", "90", "90", "90"])
        ])

        daily = self._daily_rows(bt, symbol)
        sell_row = next(row for row in daily if "Protection marché GM" in str(row.get("action_reason") or ""))
        self.assertEqual(sell_row["couloir_sell_source"], "GM")
        self.assertTrue(sell_row["couloir_sell_executed"])
        self.assertTrue(sell_row["couloir_reset_after_sell"])
        self.assertEqual(sell_row["couloir_state"], "OUT")

    def test_couloir_detailed_and_kpi_only_are_consistent(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "55", "100", "120", "108", "50", "55"])
        detailed_final = run_backtest(bt).results["tickers"][symbol.ticker]["lines"][0]["final"]
        kpi_final = run_backtest_kpi_only(bt)[symbol.ticker]["lines"][0]["final"]
        self.assertEqual(kpi_final["N"], detailed_final["N"])
        self.assertEqual(kpi_final["BT"], detailed_final["BT"])

    def test_couloir_ignores_missing_or_invalid_prices(self):
        bt, symbol = self._make_backtest(["50", "52", "54", "55"])
        DailyBar.objects.filter(symbol=symbol, date=bt.end_date).update(close=Decimal("0"))
        self.assertEqual(self._actions(bt, symbol), [])

    def test_couloir_warmup_does_not_buy_artificially_on_first_real_day(self):
        bt, symbol = self._make_backtest(
            ["50", "50", "50", "56", "54", "55", "55"],
            line=self._couloir_line(couloir_buy_confirmation_days=2),
            start_index=3,
        )
        actions = self._actions(bt, symbol)
        self.assertEqual(len(actions), 1)
        self.assertEqual(actions[0][2], "55.000000")
        self.assertNotEqual(actions[0][0], str(bt.start_date))


class BacktestLargeResultModeTests(TestCase):
    def setUp(self):
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(name="Scenario Guard", active=True)

    def test_backtest_daily_result_rows_estimate_uses_universe_lines_and_weekdays(self):
        start = date(2024, 1, 1)  # Monday
        end = date(2024, 1, 7)    # Sunday => 5 weekdays
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        bt = Backtest.objects.create(
            name="Estimate Rows",
            scenario=self.scenario,
            start_date=start,
            end_date=end,
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}, {"buy": ["C1"], "sell": ["D1"]}],
            universe_snapshot=[self.symbol.ticker, other.ticker],
        )

        stats = estimate_backtest_daily_result_rows(bt)

        self.assertEqual(stats["symbols_count"], 2)
        self.assertEqual(stats["signal_line_count"], 2)
        self.assertEqual(stats["date_count"], 5)
        self.assertEqual(stats["estimated_daily_rows"], 20)

    @override_settings(BACKTEST_DETAILED_DAILY_ROWS_MAX=9)
    def test_large_backtest_mode_activates_and_avoids_daily_rows(self):
        start = date(2024, 1, 1)
        end = date(2024, 1, 31)
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        for symbol in (self.symbol, other):
            prices = ["10", "11", "12", "13", "14"]
            DailyBar.objects.bulk_create([
                DailyBar(
                    symbol=symbol,
                    date=start + timedelta(days=idx),
                    open=Decimal(price),
                    high=Decimal(price),
                    low=Decimal(price),
                    close=Decimal(price),
                    volume=1000,
                )
                for idx, price in enumerate(prices)
            ])
            DailyMetric.objects.bulk_create([
                DailyMetric(
                    symbol=symbol,
                    scenario=self.scenario,
                    date=start + timedelta(days=idx),
                    P=Decimal(price),
                    ratio_P=Decimal("1"),
                )
                for idx, price in enumerate(prices)
            ])
        bt = Backtest.objects.create(
            name="Large Result Backtest",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=4),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker, other.ticker],
            close_positions_at_end=True,
        )
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
            Alert(symbol=other, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=other, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
        ])

        prep_report = SimpleNamespace(did_fetch_bars=False, did_compute_metrics=False, notes=[])
        with patch("core.services.backtesting.prep.prepare_backtest_data", return_value=prep_report) as prep_mock:
            run_backtest_task(bt.id)

        prep_mock.assert_called_once()
        bt.refresh_from_db()
        self.assertEqual(bt.status, Backtest.Status.DONE)
        meta = bt.results.get("meta") or {}
        self.assertTrue(meta.get("large_result_mode"))
        self.assertTrue(meta.get("detailed_daily_rows_omitted"))
        self.assertEqual(meta.get("estimated_daily_rows"), 10)
        ticker_lines = bt.results["tickers"][self.symbol.ticker]["lines"][0]
        self.assertEqual(ticker_lines["daily"], [])
        self.assertTrue(ticker_lines["daily_rows_omitted"])
        self.assertEqual(
            ticker_lines["events"],
            [
                {"date": str(start), "action": "BUY", "price_close": "10.000000"},
                {
                    "date": str(start + timedelta(days=2)),
                    "action": "SELL",
                    "action_reason": "signal invalidation A1",
                    "price_close": "12.000000",
                    "action_G": "0.2",
                    "action_PNL_AMOUNT": "20.000000",
                },
            ],
        )
        self.assertEqual(int(ticker_lines["final"]["N"]), 1)
        self.assertEqual(Decimal(ticker_lines["final"]["PNL_AMOUNT"]), Decimal("20"))
        self.assertEqual(bt.results["portfolio"]["daily"][-1]["date"], str(start + timedelta(days=4)))
        self.assertEqual(
            Decimal(bt.results["portfolio"]["daily"][-1]["pnl_global"]),
            Decimal(bt.results["portfolio"]["kpi"]["TOTAL_PNL_AMOUNT"]),
        )

    @override_settings(BACKTEST_DETAILED_DAILY_ROWS_MAX=1000000)
    def test_small_backtest_keeps_detailed_rows(self):
        start = date(2024, 1, 1)
        DailyBar.objects.bulk_create([
            DailyBar(
                symbol=self.symbol,
                date=start + timedelta(days=i),
                open=Decimal(v),
                high=Decimal(v),
                low=Decimal(v),
                close=Decimal(v),
                volume=1000,
            )
            for i, v in enumerate(["10", "11", "12"])
        ])
        DailyMetric.objects.bulk_create([
            DailyMetric(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=i), P=Decimal(v), ratio_P=Decimal("1"))
            for i, v in enumerate(["10", "11", "12"])
        ])
        Alert.objects.bulk_create([
            Alert(symbol=self.symbol, scenario=self.scenario, date=start, alerts="A1"),
            Alert(symbol=self.symbol, scenario=self.scenario, date=start + timedelta(days=2), alerts="B1"),
        ])
        bt = Backtest.objects.create(
            name="Small Backtest",
            scenario=self.scenario,
            start_date=start,
            end_date=start + timedelta(days=2),
            capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"),
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            close_positions_at_end=True,
        )

        result = determine_backtest_result_mode(bt)

        self.assertEqual(result["symbols_count"], 1)
        self.assertEqual(result["signal_line_count"], 1)
        self.assertEqual(result["date_count"], 3)
        self.assertLess(result["estimated_daily_rows"], result["detailed_daily_rows_max"])
        self.assertFalse(result["large_result_mode"])

        engine_result = run_backtest(bt, large_result_mode=False, estimated_daily_rows=result["estimated_daily_rows"]).results
        line = engine_result["tickers"][self.symbol.ticker]["lines"][0]
        self.assertFalse(engine_result["meta"]["large_result_mode"])
        self.assertFalse(engine_result["meta"]["detailed_daily_rows_omitted"])
        self.assertEqual(len(line["daily"]), 3)
