from django.contrib.auth import get_user_model
from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
import json

from core.models import Backtest, BacktestPortfolioKPI, DailyMetric, GameScenario, ProcessingJob, Scenario, Study, Symbol, Universe


class LargeSymbolFormViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(username="tester", password="secret123")
        cls.symbols = [
            Symbol(
                ticker=f"SYM{i:04d}",
                exchange="NASDAQ" if i % 2 else "NYSE",
                name=f"Company {i}",
                sector="Technology" if i % 3 == 0 else "Finance",
                country="US",
                active=True,
            )
            for i in range(1, 181)
        ]
        Symbol.objects.bulk_create(cls.symbols)
        cls.symbol_ids_csv = ",".join(str(pk) for pk in Symbol.objects.order_by("id").values_list("id", flat=True))

    def setUp(self):
        self.client = Client()
        self.client.force_login(self.user)

    def _scenario_payload(self, **overrides):
        data = {
            "name": "Scenario mass tickers",
            "description": "bulk",
            "a": "1",
            "b": "1",
            "c": "1",
            "d": "1",
            "e": "1",
            "n1": "5",
            "n2": "3",
            "npente": "100",
            "slope_threshold": "0.1",
            "npente_basse": "20",
            "slope_threshold_basse": "0.02",
            "nglobal": "20",
            "history_years": "2",
            "active": "on",
            "symbols": self.symbol_ids_csv,
        }
        data.update(overrides)
        return data

    def test_symbol_search_returns_sector_and_respects_exclude(self):
        first_two = list(Symbol.objects.order_by("id").values_list("id", flat=True)[:2])
        response = self.client.get(reverse("symbol_search"), {"q": "SYM", "exclude": ",".join(map(str, first_two)), "limit": 10})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        self.assertNotIn(first_two[0], [row["id"] for row in payload])
        self.assertIn("sector", payload[0])
        self.assertIn("country", payload[0])

    def test_universe_create_accepts_large_symbol_csv(self):
        response = self.client.post(reverse("universe_create"), {
            "name": "All US",
            "description": "Large selection",
            "active": "on",
            "symbols": self.symbol_ids_csv,
        })
        self.assertEqual(response.status_code, 302)
        universe = Universe.objects.get(name="All US")
        self.assertEqual(universe.symbols.count(), Symbol.objects.count())

    def test_scenario_create_accepts_large_symbol_csv(self):
        response = self.client.post(reverse("scenario_create"), self._scenario_payload())
        self.assertEqual(response.status_code, 302)
        scenario = Scenario.objects.get(name="Scenario mass tickers")
        self.assertEqual(scenario.symbols.count(), Symbol.objects.count())

    def test_universe_symbols_json_returns_metadata(self):
        universe = Universe.objects.create(name="Selection", active=True)
        selected = list(Symbol.objects.order_by("ticker")[:5])
        universe.symbols.set(selected)

        response = self.client.get(reverse("universe_symbols_json", args=[universe.pk]))
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["ids"]), 5)
        self.assertEqual(len(payload["symbols"]), 5)
        self.assertEqual(sorted(payload["ids"]), sorted([s.id for s in selected]))
        self.assertIn("sector", payload["symbols"][0])
        self.assertIn("exchange", payload["symbols"][0])

    def test_study_edit_accepts_large_symbol_csv(self):
        scenario = Scenario.objects.create(
            name="Study source",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            npente=100,
            slope_threshold=0.1,
            npente_basse=20,
            slope_threshold_basse=0.02,
            nglobal=20,
            history_years=2,
        )
        study = Study.objects.create(name="Study Alpha", scenario=scenario, created_by=self.user)

        response = self.client.post(reverse("study_edit", args=[study.pk]), {
            "study-name": "Study Alpha",
            "study-description": "updated",
            "sc-a": "1",
            "sc-b": "1",
            "sc-c": "1",
            "sc-d": "1",
            "sc-e": "1",
            "sc-n1": "5",
            "sc-n2": "3",
            "sc-npente": "100",
            "sc-slope_threshold": "0.1",
            "sc-npente_basse": "20",
            "sc-slope_threshold_basse": "0.02",
            "sc-nglobal": "20",
            "sc-history_years": "2",
            "sc-symbols": self.symbol_ids_csv,
        })
        self.assertEqual(response.status_code, 302)
        study.refresh_from_db()
        self.assertEqual(study.scenario.symbols.count(), Symbol.objects.count())


    def test_symbol_filter_preview_returns_total_and_preview(self):
        response = self.client.get(reverse("symbol_filter_preview"), {"exchange": "NASDAQ", "sector": "Technology", "limit": 25})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        expected_total = Symbol.objects.filter(active=True, exchange="NASDAQ", sector="Technology").count()
        self.assertEqual(payload["total_count"], expected_total)
        self.assertLessEqual(payload["preview_count"], 25)
        self.assertEqual(len(payload["symbols"]), payload["preview_count"])

    def test_symbol_filter_preview_include_all_returns_full_population(self):
        response = self.client.get(reverse("symbol_filter_preview"), {"exchange": "NYSE", "include_all": "1"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        expected_total = Symbol.objects.filter(active=True, exchange="NYSE").count()
        self.assertEqual(payload["total_count"], expected_total)
        self.assertEqual(len(payload["symbols"]), expected_total)

    def test_universe_form_renders_new_bulk_selection_ui(self):
        response = self.client.get(reverse("universe_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Ajouter tous les résultats", body)
        self.assertIn("Recherche dans la sélection", body)

    def test_scenario_form_renders_bulk_selection_ui(self):
        Universe.objects.create(name="US Market", active=True)
        response = self.client.get(reverse("scenario_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Ajouter tous les résultats", body)
        self.assertIn("Appliquer un univers existant", body)
        self.assertIn("Recherche dans la sélection", body)

    def test_scenario_duplicate_preloads_existing_symbols_in_hidden_picker_state(self):
        scenario = Scenario.objects.create(
            name="Base Scenario",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            npente=100,
            slope_threshold=0.1,
            npente_basse=20,
            slope_threshold_basse=0.02,
            nglobal=20,
            history_years=2,
        )
        selected = list(Symbol.objects.order_by("id")[:3])
        scenario.symbols.set(selected)
        response = self.client.get(reverse("scenario_duplicate", args=[scenario.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        for sym in selected:
            self.assertIn(sym.ticker, body)
        self.assertIn('server-selected-bootstrap', body)

    def test_backtest_create_view_hides_gm_codes_from_signal_choices_but_keeps_gm_filters(self):
        scenario = Scenario.objects.create(
            name="Scenario GM UI",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        response = self.client.get(reverse("backtest_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("GM positif", body)
        self.assertIn("GM négatif", body)
        self.assertIn("GM neutre", body)
        self.assertNotIn("GM_POS (momentum global positif)", body)
        self.assertNotIn("GM_NEG (momentum global négatif)", body)
        self.assertNotIn("GM_NEU (momentum global neutre)", body)

    def test_backtest_edit_view_preserves_existing_signal_lines_json(self):
        signal_lines = [
            {
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "buy_gm_filter": "GM_POS",
                "buy_gm_operator": "AND",
                "sell": [],
                "sell_logic": "OR",
                "sell_gm_filter": "IGNORE",
                "sell_gm_operator": "AND",
            }
        ]
        scenario = Scenario.objects.create(
            name="Scenario GM Edit",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT GM Edit",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=signal_lines,
            universe_snapshot=[],
        )
        response = self.client.get(reverse("backtest_update", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["signal_lines_json"], json.dumps(signal_lines))
        body = response.content.decode()
        self.assertIn('"buy_gm_filter": "GM_POS"', body)
        self.assertIn('"trading_model": "LATCH_STATEFUL"', body)
        self.assertIn('"buy": ["Af", "SPVa_basse"]', body)

    def test_backtest_detail_displays_buy_gm_filter_in_french(self):
        signal_lines = [
            {
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "buy_gm_filter": "GM_POS",
                "buy_gm_operator": "AND",
                "sell": [],
                "sell_logic": "OR",
                "sell_gm_filter": "IGNORE",
                "sell_gm_operator": "AND",
            }
        ]
        scenario = Scenario.objects.create(
            name="Scenario GM Detail",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT GM Detail",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=signal_lines,
            universe_snapshot=[],
        )
        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Filtre GM achat : GM positif", body)
        self.assertNotIn("Filtre GM vente", body)

    def test_game_scenario_form_hides_gm_codes_from_signal_choices_but_keeps_gm_filters(self):
        response = self.client.get(reverse("game_scenario_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("GM positif", body)
        self.assertIn("GM négatif", body)
        self.assertIn("GM neutre", body)
        self.assertNotIn("GM_POS (momentum global positif)", body)
        self.assertNotIn("GM_NEG (momentum global négatif)", body)
        self.assertNotIn("GM_NEU (momentum global neutre)", body)

    def test_game_scenario_edit_view_preserves_existing_signal_lines_json(self):
        signal_lines = [
            {
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "buy_logic": "AND",
                "buy_gm_filter": "GM_POS",
                "buy_gm_operator": "AND",
                "sell": [],
                "sell_logic": "OR",
                "sell_gm_filter": "IGNORE",
                "sell_gm_operator": "AND",
            }
        ]
        game = GameScenario.objects.create(
            name="Game GM Edit",
            active=True,
            signal_lines=signal_lines,
        )
        response = self.client.get(reverse("game_scenario_edit", args=[game.pk]))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["signal_lines_json"], json.dumps(signal_lines))
        body = response.content.decode()
        self.assertIn('"buy_gm_filter": "GM_POS"', body)
        self.assertIn('"trading_model": "LATCH_STATEFUL"', body)
        self.assertIn('"buy": ["Af", "SPVa_basse"]', body)


class SymbolCsvSubmissionRegressionTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="tester", password="pw123456")
        self.client.force_login(self.user)
        self.symbols = [Symbol.objects.create(ticker=f"TK{i}", exchange="NASDAQ", active=True) for i in range(1,4)]

    def test_universe_create_accepts_csv_symbols_once(self):
        resp = self.client.post("/universes/new/", {
            "name": "U CSV",
            "description": "",
            "active": "on",
            "symbols": ",".join(str(s.id) for s in self.symbols),
        }, follow=True)
        self.assertNotContains(resp, "n’est pas une valeur correcte", status_code=200)
        u = Universe.objects.get(name="U CSV")
        self.assertEqual(set(u.symbols.values_list("id", flat=True)), {s.id for s in self.symbols})

    def test_scenario_create_accepts_csv_symbols_once(self):
        payload = {
            "name": "S CSV",
            "description": "",
            "is_default": "",
            "a": 1, "b": 1, "c": 1, "d": 1, "e": "0.01",
            "n1": 20, "n2": 50, "npente": 100, "slope_threshold": "0",
            "npente_basse": 20, "slope_threshold_basse": "0",
            "nglobal": 20, "history_years": 10, "active": "on",
            "symbols": ",".join(str(s.id) for s in self.symbols),
        }
        resp = self.client.post("/scenarios/new/", payload, follow=True)
        self.assertNotContains(resp, "n’est pas une valeur correcte", status_code=200)
        scenario = Scenario.objects.get(name="S CSV")
        self.assertEqual(set(scenario.symbols.values_list("id", flat=True)), {s.id for s in self.symbols})



class BacktestResultsRenderTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="btuser", password="secret123")
        self.client.force_login(self.user)
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.scenario = Scenario.objects.create(
            name="Scenario Backtest View",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )

    def _create_metric(self, symbol, dt, **overrides):
        payload = {
            "symbol": symbol,
            "scenario": self.scenario,
            "date": dt,
            "P": "100",
            "K1": "1",
            "K1f": "99",
            "K2f": "98",
            "K2": "2",
            "K3": "3",
            "K4": "4",
            "sum_slope": "0.12",
            "slope_vrai": "0.08",
            "sum_slope_basse": "0.03",
            "slope_vrai_basse": "0.02",
            "ratio_P": "0.5",
        }
        payload.update(overrides)
        defaults = payload.copy()
        symbol_obj = defaults.pop("symbol")
        scenario_obj = defaults.pop("scenario")
        date_value = defaults.pop("date")
        obj, _created = DailyMetric.objects.update_or_create(
            symbol=symbol_obj,
            scenario=scenario_obj,
            date=date_value,
            defaults=defaults,
        )
        return obj

    def _build_diagnostic_backtest(self, *, signal_lines, ticker_lines, extra_symbols=None):
        symbols = {self.symbol.ticker: self.symbol}
        if extra_symbols:
            symbols.update({sym.ticker: sym for sym in extra_symbols})
        for sym in symbols.values():
            self._create_metric(sym, "2024-01-02")
            self._create_metric(sym, "2024-01-03", P="101")
            self._create_metric(sym, "2024-01-04", P="102")

        results = {
            "meta": {"start_date": "2024-01-01", "end_date": "2024-01-31"},
            "tickers": ticker_lines,
            "portfolio": {"kpi": {}, "daily": []},
        }
        return Backtest.objects.create(
            name="BT Diagnostic",
            scenario=self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=signal_lines,
            universe_snapshot=list(symbols.keys()),
            results=results,
        )

    def test_backtest_results_renders_portfolio_kpis_and_legend_terms(self):
        bt = Backtest.objects.create(
            name="BT View",
            scenario=self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"], "buy_gm_filter": "GM_POS"}],
            universe_snapshot=[self.symbol.ticker],
            results={
                "tickers": {
                    self.symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "buy_gm_filter": "GM_POS",
                            "allocated": "100",
                            "daily": [],
                            "final": {
                                "N": 1,
                                "S_G_N": "0.5",
                                "BT": "0.5",
                                "PNL_AMOUNT": "50",
                                "FINAL_EQUITY": "150",
                                "AVG_TRADE_AMOUNT": "50",
                                "TOTAL_GAIN_AMOUNT": "50",
                                "TOTAL_LOSS_AMOUNT": "0",
                                "PROFIT_FACTOR_AMOUNT": None,
                                "WIN_TRADES": 1,
                                "LOSS_TRADES": 0,
                                "WIN_RATE_AMOUNT": "100",
                                "MAX_GAIN_AMOUNT": "50",
                                "MAX_LOSS_AMOUNT": None,
                                "NB_JOUR_OUVRES": 3,
                                "BUY_DAYS_CLOSED": 2,
                                "BMJ": "0.1",
                                "BMD": "0.2",
                            },
                        }]
                    }
                },
                "portfolio": {
                    "kpi": {
                        "TOTAL_PNL_AMOUNT": "50",
                        "FINAL_EQUITY": "1050",
                        "TOTAL_GAIN_AMOUNT": "50",
                        "TOTAL_LOSS_AMOUNT": "0",
                        "AVG_TRADE_AMOUNT": "50",
                        "PROFIT_FACTOR_AMOUNT": None,
                        "MAX_GAIN_AMOUNT": "50",
                        "MAX_LOSS_AMOUNT": None,
                        "TOTAL_TRADES": 1,
                        "WIN_RATE_AMOUNT": "100",
                        "max_drawdown_amount": "0",
                    },
                    "daily": [],
                },
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Synthèse portefeuille globale", body)
        self.assertIn("BT — Retour portefeuille / investi final", body)
        self.assertIn("BMJ — Retour portefeuille moyen / jour investi", body)
        self.assertIn("P&amp;L total", body)
        self.assertIn("1050", body)
        self.assertIn("TOTAL_PNL_AMOUNT", body)
        self.assertIn("max_drawdown_amount", body)
        self.assertIn("Filtre GM achat : <b>GM positif</b>", body)

    def test_backtest_results_portfolio_recomputes_bt_from_equity_and_invested(self):
        bt = Backtest.objects.create(
            name="BT View Recomputed BT",
            scenario=self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            results={
                "tickers": {
                    self.symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "allocated": "100",
                            "daily": [],
                            "final": {"N": 0},
                        }]
                    }
                },
                "portfolio": {
                    "kpi": {
                        "TOTAL_RETURN_ON_CAPITAL": "0.05",
                        "equity_end": "1050",
                        "invested_end": "1000",
                        "NB_DAYS": 4,
                        "TOTAL_PNL_AMOUNT": "50",
                        "FINAL_EQUITY": "1050",
                    },
                    "daily": [],
                },
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("5.000%", body)
        self.assertIn("1.2500%", body)

    def test_backtest_results_portfolio_uses_persisted_kpi_fallbacks(self):
        bt = Backtest.objects.create(
            name="BT View Persisted KPI Fallback",
            scenario=self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=[self.symbol.ticker],
            results={
                "tickers": {
                    self.symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "allocated": "100",
                            "daily": [],
                            "final": {"N": 0},
                        }]
                    }
                },
                "portfolio": {"kpi": {"TOTAL_PNL_AMOUNT": "50", "FINAL_EQUITY": "1050"}, "daily": []},
            },
        )
        BacktestPortfolioKPI.objects.create(
            backtest=bt,
            capital_total="1000",
            invested_end="100",
            equity_end="1050",
            bt_return="0.05",
            bmj_return="0.0125",
            nb_days=4,
            max_drawdown="0",
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("5.000%", body)
        self.assertIn("1.2500%", body)

    def test_backtest_debug_excel_export_queues_job(self):
        scenario = Scenario.objects.create(
            name="BT Debug",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        symbol = Symbol.objects.order_by('id').first()
        bt = Backtest.objects.create(
            name="BT", scenario=scenario, start_date="2024-01-01", end_date="2024-01-31",
            results={
                "meta": {"start_date": "2024-01-01", "end_date": "2024-01-31"},
                "tickers": {
                    symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "daily": [{"date": "2024-01-02", "open": 10, "high": 11, "low": 9, "close": 10.5, "action": "BUY"}],
                            "final": {"BT": 0.12, "BMD": 0.01},
                        }]
                    }
                }
            },
        )

        response = self.client.get(reverse("backtest_export_debug_excel", args=[bt.pk]), {"ticker": symbol.ticker, "line": 1})
        self.assertEqual(response.status_code, 302)
        job = ProcessingJob.objects.filter(backtest=bt, job_type=ProcessingJob.JobType.EXPORT_BACKTEST_DEBUG_XLSX).latest('id')
        self.assertEqual(job.status, ProcessingJob.Status.PENDING)

    def test_backtest_results_diagnostic_payload_is_generated_for_selected_ticker_line_only(self):
        other = Symbol.objects.create(ticker="BBB", exchange="NASDAQ", active=True)
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            extra_symbols=[other],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
                "BBB": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "20", "action": None},
                    {"date": "2024-01-03", "price_close": "21", "action": "BUY"},
                    {"date": "2024-01-04", "price_close": "22", "action": "SELL"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]), {"ticker": "BBB", "line": 1})
        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["ticker"], "BBB")
        self.assertEqual(payload["line_index"], 1)
        self.assertEqual(payload["dates"], ["2024-01-02", "2024-01-03", "2024-01-04"])
        body = response.content.decode()
        self.assertIn("Diagnostic visuel de la stratégie", body)
        self.assertIn('id="diagnosticPriceChart"', body)
        self.assertNotIn('id="diagnosticChart"', body)

    def test_backtest_results_diagnostic_payload_omits_gm_when_filter_is_ignored(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "buy_gm_filter": "IGNORE"}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "buy_gm_filter": "IGNORE", "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertIsNone(payload["gm"])
        body = response.content.decode()
        self.assertIn("Diagnostic visuel de la stratégie", body)
        self.assertNotIn("Filtre GM</h4>", body)
        self.assertNotIn('id="diagnosticGmChart"', body)

    def test_backtest_results_diagnostic_payload_includes_gm_only_as_filter_when_configured(self):
        for gm_filter in ["GM_POS", "GM_NEG", "GM_NEU", "GM_POS_OR_NEU", "GM_NEG_OR_NEU"]:
            with self.subTest(gm_filter=gm_filter):
                bt = self._build_diagnostic_backtest(
                    signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "buy_gm_filter": gm_filter}],
                    ticker_lines={
                        "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "buy_gm_filter": gm_filter, "daily": [
                            {"date": "2024-01-02", "price_close": "10", "action": None},
                            {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                        ], "final": {}}]},
                    },
                )
                response = self.client.get(reverse("backtest_results", args=[bt.pk]))
                payload = response.context["diagnostic_chart_payload"]
                self.assertEqual(payload["gm"]["role"], "filter")
                self.assertEqual(payload["gm"]["filter_code"], gm_filter)
                self.assertEqual(payload["gm"]["label"], "Filtre GM")
                body = response.content.decode()
                self.assertIn("Filtre GM", body)
                self.assertIn("GM affiché comme <b>filtre</b>, jamais comme signal.", body)
                self.assertIn('id="diagnosticGmChart"', body)
                self.assertIn("Filtre GM (différence)", body)
                self.assertNotIn("Filtre GM (%)", body)
                self.assertIn('data: buildMarkerSeriesFromValues(markerType, gmValues)', body)
                self.assertNotIn("signal GM", body)

    def test_backtest_results_diagnostic_payload_parses_buy_sell_and_forced_sell_markers(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": "BUY"},
                    {"date": "2024-01-03", "price_close": "11", "action": "SELL"},
                    {"date": "2024-01-04", "price_close": "12", "action": "FORCED_SELL"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(
            payload["markers"],
            [
                {"date": "2024-01-02", "type": "BUY"},
                {"date": "2024-01-03", "type": "SELL"},
                {"date": "2024-01-04", "type": "FORCED_SELL"},
            ],
        )

    def test_backtest_results_diagnostic_payload_splits_combined_actions_into_multiple_markers(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": "SELL+BUY"},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY+FORCED_SELL"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(
            payload["markers"],
            [
                {"date": "2024-01-02", "type": "SELL"},
                {"date": "2024-01-02", "type": "BUY"},
                {"date": "2024-01-03", "type": "BUY"},
                {"date": "2024-01-03", "type": "FORCED_SELL"},
            ],
        )

    def test_backtest_results_diagnostic_payload_maps_supported_signals_to_expected_series(self):
        cases = [
            (["Af"], {"P", "Kf2bis"}),
            (["SPa"], {"SUM_SLOPE"}),
            (["SPVa"], {"SLOPE_VRAI"}),
            (["SPa_basse"], {"SUM_SLOPE_BASSE"}),
            (["SPVa_basse"], {"SLOPE_VRAI_BASSE"}),
            (["A1"], {"K1"}),
            (["C1"], {"K2"}),
            (["E1"], {"K3"}),
            (["G1"], {"K4"}),
        ]
        for buy_codes, expected_keys in cases:
            with self.subTest(buy_codes=buy_codes):
                bt = self._build_diagnostic_backtest(
                    signal_lines=[{"buy": buy_codes, "sell": ["Bf"]}],
                    ticker_lines={
                        "AAA": {"lines": [{"line_index": 1, "buy": buy_codes, "sell": ["Bf"], "daily": [
                            {"date": "2024-01-02", "price_close": "10", "action": None},
                            {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                        ], "final": {}}]},
                    },
                )
                response = self.client.get(reverse("backtest_results", args=[bt.pk]))
                payload = response.context["diagnostic_chart_payload"]
                self.assertTrue(expected_keys.issubset(set(payload["signal_series"].keys())))

    def test_backtest_results_diagnostic_payload_exposes_slope_thresholds(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["SPVa_basse"], "sell": ["SPVv_basse"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["SPVa_basse"], "sell": ["SPVv_basse"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["thresholds"]["slope_threshold"], str(self.scenario.slope_threshold))
        self.assertEqual(payload["thresholds"]["slope_threshold_basse"], str(self.scenario.slope_threshold_basse))

    def test_backtest_results_diagnostic_slope_panel_contains_main_threshold_line(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["SPVa"], "sell": ["SPVv"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["SPVa"], "sell": ["SPVv"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": "BUY"},
                    {"date": "2024-01-03", "price_close": "11", "action": "SELL"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        body = response.content.decode()
        self.assertIn('label: "Seuil pente"', body)

    def test_backtest_results_diagnostic_slope_panel_appears_for_slope_signals(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["SPVa_basse"], "sell": ["SPVv_basse"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["SPVa_basse"], "sell": ["SPVv_basse"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        body = response.content.decode()
        self.assertIn("Signaux de pente / oscillateurs", body)
        self.assertIn('id="diagnosticSlopeChart"', body)
        self.assertIn('label: "Seuil pente basse"', body)
        self.assertIn('label: "Ligne zéro"', body)
        self.assertIn('data: buildMarkerSeriesFromValues(markerType, firstSlopeValues)', body)

    def test_backtest_results_diagnostic_slope_panel_absent_for_af_only(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        body = response.content.decode()
        self.assertNotIn("Signaux de pente / oscillateurs", body)
        self.assertNotIn('id="diagnosticSlopeChart"', body)

    def test_backtest_results_diagnostic_price_panel_lists_marker_datasets(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": "BUY"},
                    {"date": "2024-01-03", "price_close": "11", "action": "SELL"},
                    {"date": "2024-01-04", "price_close": "12", "action": "FORCED_SELL"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        body = response.content.decode()
        self.assertIn('["BUY", "SELL", "FORCED_SELL"].forEach((markerType)', body)
        self.assertIn('pointStyles = {', body)
        self.assertIn('BUY: "triangle"', body)
        self.assertIn('SELL: "triangle"', body)
        self.assertIn('FORCED_SELL: "rectRot"', body)

    def test_backtest_results_diagnostic_payload_is_absent_for_kpi_only_like_results(self):
        bt = Backtest.objects.create(
            name="BT KPI Only Like",
            scenario=self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            universe_snapshot=[self.symbol.ticker],
            results={
                "meta": {"start_date": "2024-01-01", "end_date": "2024-01-31"},
                "tickers": {
                    "AAA": {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["Af"],
                            "sell": ["Bf"],
                            "final": {"N": 1, "BT": "0.1"},
                        }]
                    }
                },
                "portfolio": {"kpi": {}, "daily": []},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertIsNone(response.context.get("diagnostic_chart_payload"))
        self.assertNotIn("Diagnostic visuel de la stratégie", response.content.decode())
