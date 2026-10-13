from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from core.models import Alert, Backtest, DailyBar, DailyMetric, Scenario, Symbol
from core.services.backtesting.engine import run_backtest


class PortfolioCapitalTests(TestCase):
    def setUp(self):
        self.start = date(2024, 1, 1)
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(name="Capital curves", a=1, b=1, c=1, d=1)

    def _prices(self, symbol, prices):
        for offset, price in enumerate(prices):
            day = self.start + timedelta(days=offset)
            value = Decimal(price)
            DailyBar.objects.create(symbol=symbol, date=day, open=value, high=value, low=value, close=value)
            DailyMetric.objects.create(symbol=symbol, scenario=self.scenario, date=day, P=value, ratio_P=Decimal("1"))

    def _signal(self, symbol, offset, code):
        Alert.objects.create(symbol=symbol, scenario=self.scenario, date=self.start + timedelta(days=offset), alerts=code)

    def _backtest(self, days, **kwargs):
        options = dict(
            name="Capital curves", scenario=self.scenario, start_date=self.start,
            end_date=self.start + timedelta(days=days - 1), capital_total=Decimal("1000"),
            capital_per_ticker=Decimal("100"), capital_mode="REINVEST", include_all_tickers=True,
            universe_snapshot=[self.symbol.ticker], signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            warmup_days=0, close_positions_at_end=False,
        )
        options.update(kwargs)
        return Backtest.objects.create(**options)

    def _assert_capital(self, result, costs, cash):
        rows = result["portfolio"]["daily"]
        self.assertEqual([r["date"] for r in rows], [str(self.start + timedelta(days=i)) for i in range(len(costs))])
        self.assertEqual([Decimal(r["positions_cost"]) for r in rows], list(map(Decimal, costs)))
        self.assertEqual([Decimal(r["available_cash"]) for r in rows], list(map(Decimal, cash)))

    def test_no_positions_and_allocation_without_affordable_share(self):
        self._prices(self.symbol, ["33.33"] * 3)
        for capital in (Decimal("1000"), Decimal("0")):
            with self.subTest(capital=capital):
                bt = self._backtest(3, capital_total=capital, capital_per_ticker=Decimal("20"))
                self._assert_capital(run_backtest(bt).results, ["0"] * 3, [capital] * 3)
        self._signal(self.symbol, 1, "A1")
        for capital in (Decimal("1000"), Decimal("0")):
            with self.subTest(capital=capital, allocated=True):
                bt = self._backtest(3, capital_total=capital, capital_per_ticker=Decimal("20"))
                result = run_backtest(bt).results
                self._assert_capital(result, ["0"] * 3, [capital, capital or "20", capital or "20"])
                self.assertEqual(Decimal(result["portfolio"]["daily"][-1]["invested"]), Decimal("20"))

    def test_rounded_purchase_price_changes_sales_and_reentry_in_both_modes(self):
        self._prices(self.symbol, ["33.33", "33.33", "50", "40", "20"])
        self._signal(self.symbol, 1, "A1")
        self._signal(self.symbol, 3, "B1")
        self._signal(self.symbol, 4, "A1")
        for sale_price, pnl in (("40", Decimal("20.01")), ("20", Decimal("-39.99"))):
            DailyBar.objects.filter(symbol=self.symbol, date=self.start + timedelta(days=3)).update(close=Decimal(sale_price))
            for mode in ("FIXED", "REINVEST"):
                for capital in (Decimal("1000"), Decimal("0")):
                    for large in (False, True):
                        with self.subTest(sale=sale_price, mode=mode, capital=capital, large=large):
                            bt = self._backtest(5, capital_mode=mode, capital_total=capital)
                            result = run_backtest(bt, large_result_mode=large).results
                            funded = capital or Decimal("100")
                            reentry_cost = Decimal("100") if mode == "FIXED" else (Decimal("120") if pnl > 0 else Decimal("60"))
                            self._assert_capital(
                                result, ["0", "99.99", "99.99", "0", reentry_cost],
                                [capital, funded - Decimal("99.99"), funded - Decimal("99.99"), funded + pnl, funded + pnl - reentry_cost],
                            )
                            sale = result["portfolio"]["daily"][3]
                            self.assertEqual(Decimal(sale["pnl_global"]), pnl)
                            self.assertEqual(Decimal(sale["invested"]), Decimal("100"))
                            self.assertEqual(Decimal(sale["bank_total"]), pnl if mode == "FIXED" else Decimal("0"))

    def test_multiple_tickers_and_lines_are_aggregated(self):
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        self._prices(self.symbol, ["33.33", "50", "40"])
        self._prices(other, ["60", "70", "80"])
        self._signal(self.symbol, 0, "A1")
        self._signal(other, 0, "A1")
        self._signal(self.symbol, 2, "B1")
        bt = self._backtest(3, universe_snapshot=["AAA", "BBB"], signal_lines=[{"buy": ["A1"], "sell": ["B1"]}] * 2)
        self._assert_capital(run_backtest(bt).results, ["319.98", "319.98", "120"], ["680.02", "680.02", "920.02"])

    def test_forced_closes_apply_on_each_last_price_date_and_preserve_existing_snapshots(self):
        other = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        self._prices(self.symbol, ["33.33", "40"])
        self._prices(other, ["60", "65", "68", "70"])
        self._signal(self.symbol, 0, "A1")
        self._signal(other, 0, "A1")
        for mode in ("FIXED", "REINVEST"):
            for large in (False, True):
                with self.subTest(mode=mode, large=large):
                    bt = self._backtest(4, capital_mode=mode, universe_snapshot=["AAA", "BBB"])
                    before = run_backtest(bt, large_result_mode=large).results
                    bt.close_positions_at_end = True
                    after = run_backtest(bt, large_result_mode=large).results
                    self._assert_capital(after, ["159.99", "60", "60", "0"], ["840.01", "960.01", "960.01", "1030.01"])
                    for old, new in zip(before["portfolio"]["daily"], after["portfolio"]["daily"]):
                        self.assertEqual(
                            {k: v for k, v in old.items() if k not in {"positions_cost", "available_cash"}},
                            {k: v for k, v in new.items() if k not in {"positions_cost", "available_cash"}},
                        )

    def test_forced_close_with_loss_includes_signed_fixed_bank(self):
        self._prices(self.symbol, ["33.33", "20"])
        self._signal(self.symbol, 0, "A1")
        for mode in ("FIXED", "REINVEST"):
            with self.subTest(mode=mode):
                bt = self._backtest(2, capital_mode=mode, close_positions_at_end=True)
                result = run_backtest(bt).results
                self._assert_capital(result, ["99.99", "0"], ["900.01", "960.01"])
                self.assertEqual(Decimal(result["portfolio"]["kpi"]["TOTAL_PNL_AMOUNT"]), Decimal("-39.99"))

    def _results_page(self, daily):
        self.client.force_login(get_user_model().objects.create_user(username="capital-reader"))
        bt = self._backtest(len(daily))
        bt.results = {
            "meta": {"CP": "1000", "CP_infinite": False, "effective_currency": "CNY"},
            "tickers": {"AAA": {"lines": [{"line_index": 1, "daily": [], "final": {}}]}},
            "portfolio": {"kpi": {"capital_total": "1000"}, "daily": daily},
        }
        bt.save(update_fields=["results"])
        with patch("core.views.build_diagnostic_chart_payload", return_value=None):
            response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        bt.refresh_from_db()
        self.assertEqual(bt.results["portfolio"]["daily"], daily)
        return response

    def test_chart_keeps_every_day_and_formats_backtest_currency(self):
        daily = [{
            "date": str(self.start + timedelta(days=i)), "equity": "1000", "invested": "100",
            "pnl_global": "0", "positions_cost": "99.99", "available_cash": "900.01",
        } for i in range(405)]
        response = self._results_page(daily)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.context["portfolio_pnl_daily"]), 405)
        self.assertFalse(response.context["portfolio_capital_recalculation_required"])
        self.assertContains(response, 'currency: "CNY"')
        self.assertContains(response, 'label: "Capital véritablement investi"')
        self.assertContains(response, 'label: "Capital disponible"')
        self.assertContains(response, "FIXED")
        self.assertContains(response, "capital illimité")
        self.assertNotContains(response, "Recalcul nécessaire")

    def test_legacy_or_partial_capital_data_requires_recalculation_without_invention(self):
        complete = {"date": "2024-01-01", "equity": "1000", "invested": "100", "positions_cost": "90", "available_cash": "910"}
        incomplete = deepcopy(complete)
        incomplete["date"] = "2024-01-02"
        del incomplete["positions_cost"]
        response = self._results_page([complete, incomplete])
        self.assertTrue(response.context["portfolio_capital_recalculation_required"])
        self.assertContains(response, "Recalcul nécessaire")
        self.assertNotIn("positions_cost", response.context["portfolio_pnl_daily"][1])
        self.assertEqual(response.context["portfolio_pnl_daily"][1]["pnl_global"], "0")
