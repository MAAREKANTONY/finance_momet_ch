from django.contrib.auth import get_user_model
from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse

from core.models import Backtest, ProcessingJob, Scenario, Study, Symbol, Universe


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
        self.assertIn("P&amp;L total", body)
        self.assertIn("1050", body)
        self.assertIn("TOTAL_PNL_AMOUNT", body)
        self.assertIn("max_drawdown_amount", body)

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
