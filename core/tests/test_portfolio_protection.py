from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from core.forms import BacktestForm
from core.models import Alert, Backtest, DailyBar, DailyMetric, GameScenario, Scenario, Symbol
from core.services.backtesting.engine import run_backtest, run_backtest_kpi_only
from core.services.backtesting.portfolio_protection import PortfolioProtection, protection_threshold
from core.services.game_scenarios.runner import run_game_scenario_now


class ProtectionIndexTests(SimpleTestCase):
    def test_recurring_unit_fractions_and_exact_threshold_after_apports(self):
        tracker = PortfolioProtection(Decimal("10"))
        tracker.fund("a", Decimal("0"), Decimal("1000"))
        tracker.check("b", Decimal("1200"))
        before = tracker.index, tracker.peak, tracker.drawdown
        tracker.fund("b", Decimal("1200"), Decimal("1"))
        tracker.observe("b", Decimal("1201"))
        self.assertEqual((tracker.index, tracker.peak, tracker.drawdown), before)
        tracker.check("c", Decimal("1080.9"))
        self.assertEqual(tracker.drawdown, Decimal("0.1"))
        self.assertFalse(tracker.pending)
        tracker.check("d", Decimal("1080.899999"))
        self.assertTrue(tracker.pending)

    def test_apport_preserves_index_peak_and_existing_drawdown(self):
        tracker = PortfolioProtection(Decimal("30"))
        tracker.fund("2024-01-01", Decimal("0"), Decimal("1000"))
        tracker.check("2024-01-02", Decimal("1200"))
        tracker.check("2024-01-03", Decimal("960"))
        before = tracker.index, tracker.peak, tracker.drawdown
        tracker.fund("2024-01-03", Decimal("960"), Decimal("2400"))
        tracker.observe("2024-01-03", Decimal("3360"))
        self.assertEqual((tracker.index, tracker.peak, tracker.drawdown), before)
        self.assertEqual(before, (Decimal("96"), Decimal("120"), Decimal("0.2")))

    def test_invalid_saved_thresholds_are_rejected_by_engine_configuration(self):
        for value in (None, "bad", "NaN", "Infinity", "0", "100", "-1", "101"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                protection_threshold({"portfolio_protection": {"enabled": True, "threshold_pct": value}})
        self.assertIsNone(protection_threshold({}))
        self.assertIsNone(protection_threshold({"portfolio_protection": {"enabled": False, "threshold_pct": "bad"}}))


class ProtectedKpiOnlyContractTests(SimpleTestCase):
    def test_best_bmd_is_the_decimal_maximum_of_non_null_line_values(self):
        cases = {
            "NUMERIC": (["2", None, "10", "9"], "10"),
            "PRECISE": (["0.10000000000000000001", "0.10000000000000000002"], "0.10000000000000000002"),
            "ZERO": ([None, "-0.1", "0"], "0"),
            "NEGATIVE": ([None, "-0.2", "-0.1"], "-0.1"),
            "NULL": ([None, None], None),
            "EMPTY": ([], None),
        }
        tickers = {
            ticker: {"lines": [{"final": {"BMD": value}} for value in values]}
            for ticker, (values, _expected) in cases.items()
        }
        tickers["NULL"]["lines"].append({"final": {}})
        backtest = Backtest(settings={"portfolio_protection": {"enabled": True, "threshold_pct": "10"}})
        checkpoint = lambda: None
        with patch("core.services.backtesting.engine.run_backtest", return_value=SimpleNamespace(results={"tickers": tickers})) as detailed:
            result = run_backtest_kpi_only(backtest, checkpoint=checkpoint, max_days=5)

        detailed.assert_called_once_with(backtest, checkpoint=checkpoint, large_result_mode=True, max_days=5)
        self.assertEqual(set(result), set(cases))
        for ticker, (_values, expected) in cases.items():
            with self.subTest(ticker=ticker):
                self.assertIn("best_bmd", result[ticker])
                self.assertEqual(result[ticker]["best_bmd"], expected)


class PortfolioProtectionTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.a = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.b = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(name="Protection", a=1, b=1, c=1, d=1)
        self.scenario.symbols.set([self.a, self.b])

    def day(self, offset):
        return self.start + timedelta(days=offset)

    def prices(self, symbol, values):
        for offset, price in enumerate(values):
            if price is None:
                continue
            value = Decimal(price)
            DailyBar.objects.create(symbol=symbol, date=self.day(offset), open=value, high=value, low=value, close=value)
            DailyMetric.objects.create(symbol=symbol, scenario=self.scenario, date=self.day(offset), P=value, ratio_P=1)

    def signal(self, symbol, offset, code):
        Alert.objects.update_or_create(symbol=symbol, scenario=self.scenario, date=self.day(offset), defaults={"alerts": code})

    def backtest(self, days, **kwargs):
        options = dict(name="Protection", scenario=self.scenario, start_date=self.start, end_date=self.day(days - 1),
                       capital_total=1000, capital_per_ticker=1000, capital_mode="REINVEST", include_all_tickers=True,
                       universe_snapshot=["AAA"], signal_lines=[{"buy": ["A1"], "sell": ["B1"], "trading_model": "LEGACY_DAILY"}],
                       close_positions_at_end=False, settings={"portfolio_protection": {"enabled": True, "threshold_pct": "10"}})
        options.update(kwargs)
        return Backtest.objects.create(**options)

    @staticmethod
    def financial_payload(result):
        return {"tickers": {tk: [{"events": line["events"], "final": line["final"]} for line in entry["lines"]]
                            for tk, entry in result["tickers"].items()}, "portfolio": result["portfolio"]}

    def golden_setup(self):
        self.prices(self.a, ["33.33", "40", "20", "25", "30", "15"])
        self.prices(self.b, ["50", "60", "65", "40", "45", "50"])
        for symbol in (self.a, self.b):
            self.signal(symbol, 0, "A1")
            self.signal(symbol, 2, "B1")
            self.signal(symbol, 3, "A1")
            self.signal(symbol, 5, "B1")

    def test_disabled_matches_base_commit_golden_transactions_pnl_and_all_kpis(self):
        self.golden_setup()
        golden = json.loads((Path(__file__).parent / "fixtures/portfolio_protection_disabled.json").read_text())
        for capital in (0, 1000):
            for mode in ("FIXED", "REINVEST"):
                bt = self.backtest(6, capital_total=capital, capital_per_ticker=100, capital_mode=mode,
                                   universe_snapshot=["AAA", "BBB"], settings={})
                baseline = run_backtest(bt).results
                self.assertEqual(self.financial_payload(baseline), golden[f"{capital}-{mode}"])
                bt.settings = {"portfolio_protection": {"enabled": False, "threshold_pct": "1"}}
                self.assertEqual(run_backtest(bt).results, baseline)

    def test_immediate_drop_exact_threshold_then_strict_breach_in_every_capital_mode(self):
        self.prices(self.a, ["100", "90", "89", "89"])
        for day in (0, 2, 3):
            self.signal(self.a, day, "A1")
        for capital in (0, 1000):
            for mode in ("FIXED", "REINVEST"):
                for large in (False, True):
                    with self.subTest(capital=capital, mode=mode, large=large):
                        bt = self.backtest(4, capital_total=capital, capital_mode=mode)
                        result = run_backtest(bt, large_result_mode=large).results
                        rows = result["portfolio"]["daily"]
                        self.assertFalse(rows[1]["protection_liquidation"])
                        self.assertEqual(Decimal(rows[1]["protection_drawdown"]), Decimal(".1"))
                        self.assertTrue(rows[2]["protection_liquidation"])
                        self.assertEqual(Decimal(rows[2]["positions_value"]), 0)
                        self.assertGreater(Decimal(rows[3]["positions_value"]), 0)
                        self.assertEqual(Decimal(rows[3]["positions_cost"]), 979 if mode == "FIXED" else 890)
                        self.assertEqual(Decimal(rows[3]["bank_total"]), -110 if mode == "FIXED" else 0)
                        events = result["tickers"]["AAA"]["lines"][0]["events"]
                        self.assertEqual([(e["date"], e["action"]) for e in events],
                                         [(str(self.day(0)), "BUY"), (str(self.day(2)), "SELL"), (str(self.day(3)), "BUY")])
                        cycle = result["portfolio"]["protection"]["cycles"][0]
                        self.assertEqual(Decimal(cycle["pnl"]), -110)
                        self.assertEqual(Decimal(cycle["peak_index"]), 100)
                        self.assertEqual(Decimal(cycle["trigger_drawdown"]), Decimal(".11"))

    def test_new_high_and_multiple_cycles_keep_realized_gains_and_losses(self):
        self.prices(self.a, ["100", "120", "108", "107", "100", "80", "80"])
        for offset in range(7):
            self.signal(self.a, offset, "A1")
        for capital in (0, 1000):
            for mode in ("FIXED", "REINVEST"):
                with self.subTest(capital=capital, mode=mode):
                    result = run_backtest(self.backtest(7, capital_total=capital, capital_mode=mode)).results
                    cycles = result["portfolio"]["protection"]["cycles"]
                    self.assertEqual([c["close_reason"] for c in cycles], ["PORTFOLIO_DRAWDOWN", "PORTFOLIO_DRAWDOWN", "OPEN_END"])
                    self.assertEqual(Decimal(cycles[0]["peak_index"]), 120)
                    self.assertEqual(cycles[0]["end_date"], str(self.day(3)))
                    self.assertEqual(Decimal(cycles[0]["pnl"]), 70)
                    self.assertEqual(Decimal(cycles[1]["peak_index"]), 100)
                    self.assertEqual(Decimal(cycles[1]["pnl"]), -200)
                    final = result["tickers"]["AAA"]["lines"][0]["final"]
                    self.assertEqual(Decimal(final["PNL_AMOUNT"]), -130)
                    self.assertEqual(Decimal(result["portfolio"]["kpi"]["TOTAL_PNL_AMOUNT"]), -130)
                    if mode == "FIXED":
                        self.assertEqual(Decimal(result["portfolio"]["daily"][-1]["bank_total"]), -130)

    def test_unlimited_new_allocations_neutralize_flows_at_current_marks(self):
        self.prices(self.a, ["100", "120", "110", "100"])
        self.prices(self.b, ["100"] * 4)
        self.signal(self.a, 0, "A1")
        self.signal(self.b, 2, "A1")
        for mode in ("FIXED", "REINVEST"):
            bt = self.backtest(4, capital_total=0, capital_mode=mode, universe_snapshot=["AAA", "BBB"])
            result = run_backtest(bt).results
            rows = result["portfolio"]["daily"]
            self.assertEqual(Decimal(rows[2]["equity"]), 2100)
            self.assertEqual(Decimal(rows[2]["protection_index"]), 110)
            self.assertEqual(Decimal(rows[2]["protection_peak"]), 120)
            self.assertAlmostEqual(Decimal(rows[2]["protection_drawdown"]), Decimal(1) / 12)
            self.assertTrue(rows[3]["protection_liquidation"])
            cycle = result["portfolio"]["protection"]["cycles"][0]
            self.assertEqual(Decimal(cycle["external_funding"]), 1000)
            self.assertEqual(Decimal(cycle["pnl"]), 0)

    def test_finite_allocations_are_internal_transfers(self):
        self.prices(self.a, ["100", "120", "110"])
        self.prices(self.b, ["100"] * 3)
        self.signal(self.a, 0, "A1")
        self.signal(self.b, 2, "A1")
        bt = self.backtest(3, capital_total=2000, universe_snapshot=["AAA", "BBB"])
        rows = run_backtest(bt).results["portfolio"]["daily"]
        self.assertEqual([Decimal(r["protection_index"]) for r in rows], [100, 110, 105])

    def test_breach_blocks_new_external_allocation_before_it_can_mask_the_drop(self):
        self.prices(self.a, ["100", "80"])
        self.prices(self.b, ["100", "100"])
        self.signal(self.a, 0, "A1")
        self.signal(self.b, 1, "A1")
        result = run_backtest(self.backtest(2, capital_total=0, universe_snapshot=["AAA", "BBB"])).results
        self.assertTrue(result["portfolio"]["daily"][-1]["protection_liquidation"])
        self.assertEqual(Decimal(result["portfolio"]["daily"][-1]["invested"]), 1000)
        self.assertEqual(result["tickers"]["BBB"]["lines"][0]["events"], [])

    def test_missing_quotes_keep_liquidation_pending_and_block_all_new_buys(self):
        self.prices(self.a, ["100", None, None, "70", "70"])
        self.prices(self.b, ["100", "70", "70", "70", "70"])
        self.signal(self.a, 0, "A1")
        self.signal(self.b, 0, "A1")
        for offset in range(1, 5):
            self.signal(self.b, offset, "A1,B1")
        for large in (False, True):
            result = run_backtest(self.backtest(5, capital_total=2000, universe_snapshot=["AAA", "BBB"]), large_result_mode=large).results
            a_events = result["tickers"]["AAA"]["lines"][0]["events"]
            b_events = result["tickers"]["BBB"]["lines"][0]["events"]
            self.assertEqual([e["date"] for e in a_events if e["action"] == "SELL"], [str(self.day(3))])
            self.assertEqual([e["date"] for e in b_events if e["action"] == "BUY"], [str(self.day(0)), str(self.day(4))])
            self.assertEqual(b_events[1]["action_reason"], "PORTFOLIO_DRAWDOWN")
            rows = result["portfolio"]["daily"]
            self.assertTrue(rows[2]["protection_pending"])
            self.assertFalse(result["portfolio"]["protection"]["pending_liquidation"])
            self.assertEqual(result["portfolio"]["protection"]["cycles"][0]["end_date"], str(self.day(3)))
            self.assertEqual(Decimal(result["portfolio"]["kpi"]["TOTAL_PNL_AMOUNT"]), -600)

    def test_end_of_backtest_does_not_backdate_a_pending_sale(self):
        self.prices(self.a, ["100", None, None])
        self.prices(self.b, ["100", "70", "70"])
        self.signal(self.a, 0, "A1")
        self.signal(self.b, 0, "A1")
        result = run_backtest(self.backtest(3, capital_total=2000, universe_snapshot=["AAA", "BBB"], close_positions_at_end=True)).results
        self.assertTrue(result["portfolio"]["protection"]["pending_liquidation"])
        self.assertEqual(result["portfolio"]["protection"]["cycles"][0]["close_reason"], "PENDING_DRAWDOWN")
        self.assertEqual([e["action"] for e in result["tickers"]["AAA"]["lines"][0]["events"]], ["BUY"])

    def test_zero_quote_is_not_an_executable_liquidation_price(self):
        self.prices(self.a, ["100", "0", "80"])
        self.signal(self.a, 0, "A1")
        result = run_backtest(self.backtest(3)).results
        events = result["tickers"]["AAA"]["lines"][0]["events"]
        self.assertEqual([(e["date"], e["action"]) for e in events],
                         [(str(self.day(0)), "BUY"), (str(self.day(2)), "SELL")])
        self.assertTrue(result["portfolio"]["daily"][1]["protection_pending"])

    def test_exhausted_fixed_reserve_keeps_accounts_and_waits_for_positive_funding(self):
        self.prices(self.a, ["100", "1", "100", "1", "100", "100"])
        self.prices(self.b, ["100"] * 6)
        for offset in range(6):
            self.signal(self.a, offset, "A1")
        self.signal(self.b, 5, "A1")
        for capital in (0, 1000):
            bt = self.backtest(6, capital_total=capital, capital_mode="FIXED", universe_snapshot=["AAA", "BBB"])
            result = run_backtest(bt).results
            rows = result["portfolio"]["daily"]
            self.assertEqual(Decimal(rows[4]["bank_total"]), -1980)
            self.assertEqual(Decimal(rows[4]["equity"]), -980)
            self.assertEqual(Decimal(rows[4]["positions_value"]), 0)
            self.assertEqual(Decimal(result["portfolio"]["kpi"]["TOTAL_PNL_AMOUNT"]), -1980)
            self.assertEqual(Decimal(rows[5]["positions_value"]), 2000 if capital == 0 else 0)
            if capital == 0:
                self.assertEqual(Decimal(rows[5]["equity"]), 20)
                self.assertEqual(Decimal(rows[5]["protection_index"]), 100)

    def test_future_prices_do_not_change_earlier_indices_or_trades(self):
        self.prices(self.a, ["100", "120", "110", "1"])
        self.prices(self.b, ["100", "100", "100", "1000"])
        self.signal(self.a, 0, "A1")
        self.signal(self.b, 2, "A1")
        bt = self.backtest(4, capital_total=0, universe_snapshot=["AAA", "BBB"])
        full = run_backtest(bt).results
        bt.end_date = self.day(2)
        prefix = run_backtest(bt).results
        self.assertEqual(prefix["portfolio"]["daily"], full["portfolio"]["daily"][:3])
        for ticker in ("AAA", "BBB"):
            earlier = [e for e in full["tickers"][ticker]["lines"][0]["events"] if e["date"] <= str(self.day(2))]
            self.assertEqual(prefix["tickers"][ticker]["lines"][0]["events"], earlier)

    def test_couloir_is_overridden_once_then_resumes_using_its_own_rules(self):
        self.prices(self.a, ["50", "61", "62", "70", "55", "56", "63"])
        line = {"buy": ["COULOIR"], "sell": [], "couloir_initial_low_lookback_days": 3,
                "couloir_buy_rebound_threshold": "0.10", "couloir_sell_drawdown_threshold": "0.10",
                "couloir_buy_confirmation_days": 1, "couloir_sell_confirmation_days": 1}
        result = run_backtest(self.backtest(7, signal_lines=[line])).results
        events = result["tickers"]["AAA"]["lines"][0]["events"]
        self.assertEqual([e["action"] for e in events], ["BUY", "SELL", "BUY"])
        self.assertEqual(events[1]["action_reason"], "PORTFOLIO_DRAWDOWN")
        self.assertEqual(events[1]["date"], str(self.day(4)))
        self.assertEqual(events[2]["date"], str(self.day(6)))

    def test_multiple_lines_liquidate_once_without_concurrent_end_sale(self):
        self.prices(self.a, ["100", "80"])
        self.signal(self.a, 0, "A1")
        self.signal(self.a, 1, "A1,B1")
        line = {"buy": ["A1"], "sell": ["B1"], "trading_model": "LEGACY_DAILY"}
        result = run_backtest(self.backtest(2, capital_total=2000, signal_lines=[line, line], close_positions_at_end=True)).results
        for line in result["tickers"]["AAA"]["lines"]:
            self.assertEqual([e["action"] for e in line["events"]], ["BUY", "SELL"])
            self.assertEqual(line["final"]["N"], 1)
        self.assertEqual(Decimal(result["portfolio"]["kpi"]["TOTAL_PNL_AMOUNT"]), -400)

    def test_zero_initial_capital_waits_for_first_actual_allocation_after_warmup(self):
        self.prices(self.a, ["120", "100", "100", "100", "89"])
        self.signal(self.a, 0, "A1")
        self.signal(self.a, 1, "B1")
        self.signal(self.a, 3, "A1")
        bt = self.backtest(5, capital_total=0, start_date=self.day(1), warmup_days=1)
        result = run_backtest(bt).results
        rows = result["portfolio"]["daily"]
        self.assertIsNone(rows[0]["protection_index"])
        self.assertIsNone(rows[1]["protection_index"])
        self.assertEqual(Decimal(rows[2]["protection_index"]), 100)
        self.assertEqual(result["portfolio"]["protection"]["cycles"][0]["start_date"], str(self.day(3)))
        self.assertTrue(rows[3]["protection_liquidation"])

    def test_offloading_keeps_cycles_markers_and_sale_events(self):
        from core.services.backtesting.results_offload import offload_daily_series_if_needed, load_daily_from_line
        self.prices(self.a, ["100", "80"])
        self.signal(self.a, 0, "A1")
        bt = self.backtest(2)
        result = run_backtest(bt).results
        portfolio = result["portfolio"]
        events = result["tickers"]["AAA"]["lines"][0]["events"]
        with TemporaryDirectory() as directory, patch.dict("os.environ", {"BACKTEST_DATA_DIR": directory}), \
             patch("core.services.backtesting.results_offload.estimate_json_bytes", return_value=2_000_000):
            offloaded = offload_daily_series_if_needed(bt, result, max_mb=1)
            self.assertEqual(offloaded["portfolio"], portfolio)
            line = offloaded["tickers"]["AAA"]["lines"][0]
            self.assertTrue(line["daily_offloaded"])
            self.assertEqual(line["events"], events)
            self.assertEqual(load_daily_from_line(line)[-1]["action_reason"], "PORTFOLIO_DRAWDOWN")

    def test_large_and_kpi_only_paths_preserve_protected_financial_results(self):
        self.prices(self.a, ["100", "80", "100", "70"])
        for offset in (0, 2):
            self.signal(self.a, offset, "A1")
        bt = self.backtest(4)
        full = run_backtest(bt).results
        large = run_backtest(bt, large_result_mode=True).results
        self.assertEqual(full["portfolio"], large["portfolio"])
        self.assertEqual(full["tickers"]["AAA"]["lines"][0]["events"], large["tickers"]["AAA"]["lines"][0]["events"])
        self.assertEqual(full["tickers"]["AAA"]["lines"][0]["final"], run_backtest_kpi_only(bt)["AAA"]["lines"][0]["final"])

    def best_bmd_backtest(self):
        self.prices(self.a, ["100", "100", "110", "150", "150"])
        self.signal(self.a, 0, "A1")
        self.signal(self.a, 1, "A2")
        self.signal(self.a, 3, "B1,B2")
        return self.backtest(5, capital_total=0, signal_lines=[
            {"buy": ["A3"], "sell": ["B3"], "trading_model": "LEGACY_DAILY"},
            {"buy": ["A1"], "sell": ["B1"], "trading_model": "LEGACY_DAILY"},
            {"buy": ["A2"], "sell": ["B2"], "trading_model": "LEGACY_DAILY"},
        ])

    def test_kpi_only_best_bmd_with_multiple_lines_with_and_without_protection(self):
        bt = self.best_bmd_backtest()
        for settings in ({}, {"portfolio_protection": {"enabled": False}}, bt.settings):
            with self.subTest(settings=settings):
                bt.settings = settings
                entry = run_backtest_kpi_only(bt)["AAA"]
                finals = [line["final"] for line in entry["lines"]]
                self.assertIsNone(finals[0]["BMD"])
                self.assertEqual(Decimal(finals[1]["BMD"]), Decimal("0.5") / 3)
                self.assertEqual(finals[1]["TRADABLE_DAYS_IN_POSITION_CLOSED"], 3)
                self.assertEqual(finals[2]["BMD"], "0.25")
                self.assertEqual(finals[2]["TRADABLE_DAYS_IN_POSITION_CLOSED"], 2)
                self.assertEqual(entry.get("best_bmd"), "0.25")

    def test_protected_game_persists_best_bmd_and_presence_from_the_best_line(self):
        bt = self.best_bmd_backtest()
        self.b.active = False
        self.b.save(update_fields=["active"])
        DailyMetric.objects.filter(symbol=self.a, scenario=self.scenario).update(sum_slope=Decimal("0.2"))
        game = GameScenario.objects.create(
            name="Protected KPI game", engine_scenario=self.scenario, study_days=5,
            capital_total=0, capital_per_ticker=1000, capital_mode="REINVEST",
            signal_lines=bt.signal_lines, settings=bt.settings, close_positions_at_end=False,
            tradability_threshold=20, slope_threshold=Decimal("0.1"), presence_threshold_pct=35,
            today_results={"rows": [{"ticker": "STALE", "ok": False}]},
        )

        # Only the provider boundary is stubbed; scenario sync, depth/slope queries,
        # the KPI-only engine and result persistence all use the local fixtures.
        with patch("core.tasks._fetch_daily_bars_for_symbols", return_value={"symbols": 1, "bars": 0}), \
             patch("core.tasks._compute_metrics_for_scenario") as compute_metrics:
            result = run_game_scenario_now(game.id, skip_metrics=True)
        compute_metrics.assert_not_called()

        self.assertEqual(result, {"status": "ok", "date": str(self.day(4)), "count": 1})
        game.refresh_from_db()
        self.assertEqual(game.last_run_status, "ok")
        self.assertIsNotNone(game.last_run_at)
        self.assertEqual(game.last_run_message, f"Computed 1 tickers for {self.day(4)}")
        self.assertEqual(game.today_results["date"], str(self.day(4)))
        rows = game.today_results["rows"]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["ticker"], "AAA")
        self.assertEqual(row["bmd"], "0.25")
        self.assertEqual(Decimal(row["avg_slope"]), Decimal("0.2"))
        self.assertEqual(row["TRADABLE_DAYS"], 5)
        self.assertEqual(row["TRADABLE_DAYS_IN_POSITION_CLOSED"], 2)
        self.assertEqual(Decimal(row["RATIO_IN_POSITION"]), Decimal("40"))
        self.assertIs(row["ok"], True)

    def test_protection_adds_no_database_queries_or_indicator_recalculation(self):
        self.prices(self.a, ["100", "95", "80"])
        self.signal(self.a, 0, "A1")
        bt = self.backtest(3, settings={})
        with CaptureQueriesContext(connection) as old:
            run_backtest(bt)
        bt.settings = {"portfolio_protection": {"enabled": True, "threshold_pct": "10"}}
        with CaptureQueriesContext(connection) as new:
            run_backtest(bt)
        self.assertEqual(len(new), len(old))

    def form_data(self, threshold="10", enabled=True):
        data = dict(name="Saved protection", scenario=self.scenario.pk, start_date=str(self.start), end_date=str(self.day(4)),
                    capital_total="1000", capital_per_ticker="1000", capital_mode="FIXED", ratio_threshold="0",
                    signal_lines=json.dumps([{"buy": ["A1"], "sell": ["B1"], "trading_model": "LEGACY_DAILY"}]), warmup_days="0",
                    portfolio_drawdown_threshold=threshold)
        if enabled:
            data["portfolio_protection_enabled"] = "on"
        return data

    def test_form_validation_defaults_and_snapshot_restore(self):
        self.assertFalse(BacktestForm().fields["portfolio_protection_enabled"].initial)
        for threshold in ("0", "100", "-1", "101", "", "NaN"):
            form = BacktestForm(data=self.form_data(threshold))
            self.assertFalse(form.is_valid(), threshold)
            self.assertIn("portfolio_drawdown_threshold", form.errors)
        form = BacktestForm(data=self.form_data("10.1234"))
        self.assertTrue(form.is_valid(), form.errors)
        bt = form.save()
        self.assertEqual(bt.settings["portfolio_protection"]["threshold_pct"], "10.1234")
        self.assertTrue(BacktestForm(instance=bt).fields["portfolio_protection_enabled"].initial)
        from core.services.run_configuration_snapshots import capture_backtest_configuration, restore_backtest_snapshot
        restored = restore_backtest_snapshot(capture_backtest_configuration(bt))
        self.assertEqual(restored.settings, bt.settings)

    def test_editing_protection_invalidates_saved_results(self):
        self.client.force_login(get_user_model().objects.create_user(username="protection-editor"))
        bt = self.backtest(5, settings={}, results={"stale": True}, status="DONE")
        response = self.client.post(reverse("backtest_update", args=[bt.pk]), self.form_data())
        self.assertEqual(response.status_code, 302)
        bt.refresh_from_db()
        self.assertEqual(bt.results, {})
        self.assertTrue(bt.settings["portfolio_protection"]["enabled"])

    def test_results_keep_liquidation_markers_in_long_charts_and_show_cycles(self):
        self.client.force_login(get_user_model().objects.create_user(username="protection-reader"))
        bt = self.backtest(405)
        daily = [{"date": str(self.day(i)), "equity": "1000", "invested": "1000", "positions_cost": "0", "available_cash": "1000",
                  "pnl_global": "0", "protection_liquidation": i == 51} for i in range(405)]
        # Offset 40 is removed by the normal 400-point downsampling.
        for row in daily:
            row["protection_liquidation"] = False
        daily[40]["protection_liquidation"] = True
        bt.results = {"tickers": {"AAA": {"lines": [{"line_index": 1, "daily": [], "final": {}}]}},
                      "portfolio": {"daily": daily, "kpi": {"capital_total": "1000"}, "protection": {"enabled": True, "threshold_pct": "10", "cycles": [
                          {"cycle": 1, "start_date": str(self.start), "end_date": str(self.day(50)), "peak_index": "120", "trigger_drawdown": ".11", "pnl": "70", "close_reason": "PORTFOLIO_DRAWDOWN"}]}}}
        bt.save(update_fields=["results"])
        with patch("core.views.build_diagnostic_chart_payload", return_value=None):
            response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Protection du portefeuille — cycles")
        self.assertContains(response, "Liquidation sur drawdown")
        self.assertEqual(len(response.context["portfolio_daily"]), 401)
        self.assertTrue(any(row.get("protection_liquidation") for row in response.context["portfolio_daily"]))
