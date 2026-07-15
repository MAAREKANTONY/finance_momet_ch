import copy
import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.db import connection
from django.test import Client, SimpleTestCase, TestCase
from django.test.utils import CaptureQueriesContext
from django.urls import reverse

from core.forms import ScenarioForm
from core.models import Alert, Backtest, GameScenario, JobLog, Scenario, Symbol
from core.views import _symbol_display_labels_for_tickers


class SymbolDisplayPropertiesTests(SimpleTestCase):
    def test_english_name_is_preferred_and_code_keeps_exchange(self):
        symbol = Symbol(
            ticker=" 601006 ",
            exchange=" SHG ",
            name="000780",
            name_en=" Daqin Railway Co Ltd ",
        )

        self.assertEqual(symbol.display_name, "Daqin Railway Co Ltd")
        self.assertEqual(symbol.display_code, "601006.SHG")
        self.assertEqual(symbol.display_label, "601006.SHG — Daqin Railway Co Ltd")

    def test_historical_name_then_ticker_are_the_fallbacks(self):
        local = Symbol(ticker="TICKER", exchange="EXCHANGE", name=" Nom local ", name_en="  ")
        unnamed = Symbol(ticker="TICKER", exchange="EXCHANGE", name=" ", name_en="")

        self.assertEqual(local.display_label, "TICKER.EXCHANGE — Nom local")
        self.assertEqual(unnamed.display_name, "TICKER")
        self.assertEqual(unnamed.display_label, "TICKER.EXCHANGE")

    def test_ticker_or_code_used_as_name_is_not_duplicated(self):
        self.assertEqual(Symbol(ticker="ABC", exchange="US", name="ABC").display_label, "ABC.US")
        self.assertEqual(Symbol(ticker="ABC", exchange="US", name_en=" abc.us ").display_label, "ABC.US")


class SymbolFrontDisplayTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.user = get_user_model().objects.create_user(username="symbol-label-user", password="secret")
        self.client.force_login(self.user)
        self.symbol = Symbol.objects.create(
            ticker="601006",
            exchange="SHG",
            name="000780",
            name_en="Daqin Railway Co Ltd",
            sector="Industrials",
            active=True,
        )
        self.local_symbol = Symbol.objects.create(
            ticker="LOCAL",
            exchange="US",
            name="Nom local",
            active=True,
        )
        self.scenario = Scenario.objects.create(name="Scenario labels", active=True)
        self.scenario.symbols.add(self.symbol, self.local_symbol)

    def _create_backtest(self):
        results = {
            "meta": {
                "warnings": [
                    {
                        "ticker": "601006",
                        "line_index": 1,
                        "sell_date": "2024-01-02",
                        "buy_date": "2024-01-03",
                    }
                ],
                "warning_count": 1,
            },
            "tickers": {
                "601006": {
                    "lines": [
                        {
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "daily": [
                                {
                                    "date": "2024-01-02",
                                    "action": "BUY",
                                    "price_close": "10",
                                    "shares": 1,
                                }
                            ],
                            "final": {"N": 1, "BT": "0.1"},
                        }
                    ]
                }
            },
            "portfolio": {"kpi": {}, "daily": []},
        }
        return Backtest.objects.create(
            name="Historical label result",
            scenario=self.scenario,
            status=Backtest.Status.DONE,
            universe_snapshot=[{"ticker": "601006", "exchange": "SHG", "sector": "Industrials"}],
            results=results,
        )

    def test_tickers_page_and_symbol_detail_use_central_label_and_escape_html(self):
        unsafe = Symbol.objects.create(
            ticker="SAFE",
            exchange="US",
            name_en="<script>alert(1)</script>",
            active=True,
        )

        with CaptureQueriesContext(connection) as queries:
            response = self.client.get(reverse("symbols_page"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "601006.SHG — Daqin Railway Co Ltd")
        self.assertContains(response, "LOCAL.US — Nom local")
        self.assertContains(response, "SAFE.US — &lt;script&gt;alert(1)&lt;/script&gt;", html=False)
        self.assertNotContains(response, "SAFE.US — <script>alert(1)</script>", html=False)
        self.assertContains(response, reverse("symbol_scenarios_edit", args=[unsafe.pk]))
        symbol_select_queries = [q for q in queries if 'FROM "core_symbol"' in q["sql"]]
        self.assertEqual(len(symbol_select_queries), 1)

        detail = self.client.get(reverse("symbol_scenarios_edit", args=[self.symbol.pk]))
        self.assertContains(detail, "601006.SHG — Daqin Railway Co Ltd")

    def test_alerts_use_display_label_but_keep_ticker_filter_value(self):
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date="2024-01-02", alerts="A1")

        response = self.client.get(reverse("alerts_table"))

        self.assertContains(response, "601006.SHG — Daqin Railway Co Ltd", count=2)
        self.assertContains(response, 'value="601006"', html=False)

    def test_search_finds_name_en_and_keeps_technical_payload(self):
        response = self.client.get(reverse("symbol_search"), {"q": "Daqin Railway"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()), 1)
        item = response.json()[0]
        self.assertEqual(item["id"], self.symbol.id)
        self.assertEqual(item["ticker"], "601006")
        self.assertEqual(item["exchange"], "SHG")
        self.assertEqual(item["name"], "000780")
        self.assertEqual(item["display_label"], "601006.SHG — Daqin Railway Co Ltd")

        preview = self.client.get(reverse("symbol_filter_preview"), {"q": "Daqin Railway"})
        self.assertEqual(preview.json()["symbols"][0]["display_label"], "601006.SHG — Daqin Railway Co Ltd")

    def test_symbol_form_label_and_picker_payload_use_display_label_without_changing_id(self):
        form = ScenarioForm(instance=self.scenario)
        field = form.fields["symbols"]
        payload = json.loads(field.widget.attrs["data_selected_json"])
        daqin = next(item for item in payload if item["id"] == self.symbol.id)

        self.assertEqual(field.label_from_instance(self.symbol), "601006.SHG — Daqin Railway Co Ltd | Secteur: Industrials")
        self.assertEqual(daqin["id"], self.symbol.id)
        self.assertEqual(daqin["ticker"], "601006")
        self.assertEqual(daqin["display_label"], "601006.SHG — Daqin Railway Co Ltd")

    @patch("core.views.build_diagnostic_chart_payload", return_value=None)
    def test_backtest_views_resolve_labels_in_one_query_without_rewriting_results(self, _diagnostic):
        backtest = self._create_backtest()
        stored_results = copy.deepcopy(backtest.results)

        detail = self.client.get(reverse("backtest_detail", args=[backtest.pk]))
        self.assertContains(detail, "601006.SHG — Daqin Railway Co Ltd")

        with CaptureQueriesContext(connection) as queries:
            results = self.client.get(reverse("backtest_results", args=[backtest.pk]))
        self.assertEqual(results.status_code, 200)
        self.assertContains(results, "601006.SHG — Daqin Railway Co Ltd")
        self.assertContains(results, 'data-ticker="601006"', html=False)
        self.assertContains(results, "?ticker=601006&line=1", html=False)
        self.assertContains(results, "label.includes(query)", html=False)
        symbol_select_queries = [q for q in queries if 'FROM "core_symbol"' in q["sql"]]
        self.assertEqual(len(symbol_select_queries), 1)

        backtest.refresh_from_db()
        self.assertEqual(backtest.results, stored_results)

        debug = self.client.get(reverse("backtest_debug", args=[backtest.pk]))
        self.assertContains(debug, "601006.SHG — Daqin Railway Co Ltd")
        self.assertContains(debug, "clé technique : 601006")

    def test_grouped_label_resolution_does_not_add_one_query_per_ticker(self):
        extras = [
            Symbol.objects.create(ticker=f"BATCH{i:02d}", exchange="US", name_en=f"Company {i}")
            for i in range(20)
        ]
        tickers = [self.symbol.ticker, *(symbol.ticker for symbol in extras)]

        with CaptureQueriesContext(connection) as queries:
            labels = _symbol_display_labels_for_tickers(tickers)

        self.assertEqual(len(queries), 1)
        self.assertEqual(labels["BATCH19"], "BATCH19.US — Company 19")

    def test_game_result_gets_label_without_mutating_stored_snapshot(self):
        snapshot = {"date": "2024-01-02", "rows": [{"ticker": "601006", "bmd": "1", "ok": True}]}
        game = GameScenario.objects.create(name="Game labels", today_results=snapshot)

        response = self.client.get(reverse("game_scenario_detail", args=[game.pk]))

        self.assertContains(response, "601006.SHG — Daqin Railway Co Ltd")
        game.refresh_from_db()
        self.assertEqual(game.today_results, snapshot)

    def test_job_log_symbol_uses_display_label(self):
        JobLog.objects.create(job="display", symbol=self.symbol, message="test")

        response = self.client.get(reverse("logs_page"))

        self.assertContains(response, "601006.SHG — Daqin Railway Co Ltd")
