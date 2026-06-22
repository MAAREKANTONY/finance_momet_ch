from decimal import Decimal
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse
import json

from core.models import Alert, Backtest, BacktestPortfolioKPI, DailyBar, DailyMetric, GameScenario, HistoricalMarketCap, ProcessingJob, Scenario, Study, Symbol, Universe, UniverseCoverageSnapshot, UniverseCoverageStatus, UniverseDefinition, UniverseImportBatch, UniverseMembership
from core.views import _build_realized_gains_cumulative_series


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
        self.assertIn("1. Modèle de prix", body)
        self.assertIn("2. Fenêtres d’indicateurs", body)
        self.assertIn("3. Fenêtres de pente &amp; régime", body)
        self.assertIn("4. Signal anti-chute RHD — Repli depuis haut récent", body)
        self.assertIn("5. Historique &amp; calcul", body)
        self.assertIn("6. Contrôles avancés des indicateurs", body)
        self.assertIn("Le scénario = le modèle d’indicateurs.", body)
        self.assertIn("Mode d’univers", body)
        self.assertIn("S&amp;P500 historique dynamique", body)
        self.assertIn("Les Games restent inchangés.", body)
        self.assertIn("static-ticker-selection", body)
        self.assertIn("dynamic-universe-help", body)
        self.assertIn("Les actions seront déterminées automatiquement", body)
        self.assertIn("Vous n’avez pas besoin de sélectionner de tickers", body)
        self.assertIn("SP500_HISTORICAL_DYNAMIC", body)
        self.assertNotIn("prochaines phases", body)
        self.assertIn("Seuil de déclenchement vente", body)
        self.assertIn("Seuil de déclenchement vente — pente basse", body)
        self.assertIn("Fenêtre du plus haut récent", body)
        self.assertIn("Repli maximal RHD", body)
        self.assertIn("RHD est un signal ticker calculé", body)
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

    def test_scenario_duplicate_preserves_sell_threshold_values(self):
        scenario = Scenario.objects.create(
            name="Slope Duplicate",
            active=True,
            universe_mode=Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            npente=100,
            slope_threshold=0.1,
            slope_sell_threshold=0.05,
            npente_basse=20,
            slope_threshold_basse=0.02,
            slope_sell_threshold_basse=0.01,
            nglobal=20,
            history_years=2,
        )
        response = self.client.get(reverse("scenario_duplicate", args=[scenario.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn('name="slope_sell_threshold"', body)
        self.assertIn('value="0.05', body)
        self.assertIn('name="slope_sell_threshold_basse"', body)
        self.assertIn('value="0.01', body)
        self.assertIn('name="universe_mode"', body)
        self.assertIn('value="SP500_HISTORICAL_DYNAMIC" selected', body)
        self.assertIn("Les actions seront déterminées automatiquement", body)
        self.assertIn("static-ticker-selection", body)
        self.assertIn("dynamic-universe-help", body)

    def test_backtest_create_view_shows_only_line_market_conditions_for_gm(self):
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
        self.assertIn("1. Périmètre / Période", body)
        self.assertIn("2. Signaux d’entrée &amp; de sortie", body)
        self.assertIn("3. Conditions de marché", body)
        self.assertIn("4. Filtres de tradabilité &amp; risque", body)
        self.assertIn("5. Capital &amp; exécution", body)
        self.assertIn("Un signal est un déclencheur", body)
        self.assertIn("Un filtre est une condition bloquante", body)
        self.assertNotIn("Filtres de tendance", body)
        self.assertNotIn("Combiner les filtres de tendance avec", body)
        self.assertNotIn('data-role="buy_gm_filter"', body)
        self.assertNotIn('data-role="sell_gm_filter"', body)
        self.assertNotIn('data-role="buy_gm_operator"', body)
        self.assertNotIn('data-role="sell_gm_operator"', body)
        self.assertNotIn("GM_POS (momentum global positif)", body)
        self.assertNotIn("GM_NEG (momentum global négatif)", body)
        self.assertNotIn("GM_NEU (momentum global neutre)", body)

    def test_backtest_create_view_renders_market_cap_filter_fields(self):
        Scenario.objects.create(
            name="Scenario Market Cap UI",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )

        response = self.client.get(reverse("backtest_create"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Min Market Cap", body)
        self.assertIn("Max Market Cap", body)
        self.assertIn("If Market Cap Missing", body)
        self.assertIn("Block BUY (recommended)", body)
        self.assertIn("Allow BUY", body)
        self.assertIn("Minimum historical company market capitalization required to allow BUY.", body)
        self.assertIn("Maximum historical company market capitalization allowed for BUY.", body)
        self.assertIn("What to do when no historical market capitalization exists at or before the BUY date.", body)
        self.assertIn("BUY is blocked when market cap is unknown.", body)
        self.assertIn("BUY remains allowed when market cap is unknown.", body)

    def test_backtest_create_view_hides_global_trend_filter_fields(self):
        Scenario.objects.create(
            name="Scenario Trend UI",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        response = self.client.get(reverse("backtest_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Filtres de tendance", body)
        self.assertNotIn("Combiner les filtres de tendance avec", body)


class SymbolMetadataViewTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="symbol-meta", password="secret123")
        self.client = Client()
        self.client.force_login(self.user)
        self.default_scenario = Scenario.objects.create(name="Default", active=True, is_default=True)

    def test_symbols_page_renders_update_buttons(self):
        symbol = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", active=True)

        response = self.client.get(reverse("symbols_page"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Update missing metadata", body)
        self.assertIn("Ensure benchmark ETFs", body)
        self.assertIn("Sync benchmark ETFs", body)
        self.assertIn(reverse("symbol_update_metadata", args=[symbol.id]), body)
        self.assertIn("Update metadata", body)

    @patch("core.views.enrich_symbols_metadata")
    @patch("core.views.sync_benchmark_etfs_for_symbols")
    def test_web_symbol_add_calls_enrichment_service(self, benchmark_mock, enrich_mock):
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 1,
            "unchanged": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "AAPL", "updated_fields": ["name"], "error": "", "skipped": False}],
        }
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY"],
            "created": 1,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(reverse("symbol_add"), {"ticker": "AAPL"})

        self.assertEqual(response.status_code, 302)
        self.assertTrue(enrich_mock.called)
        passed_symbols = enrich_mock.call_args.args[0]
        self.assertEqual(len(passed_symbols), 1)
        self.assertEqual(passed_symbols[0].ticker, "AAPL")
        self.assertEqual(enrich_mock.call_args.kwargs["only_missing"], True)
        benchmark_mock.assert_called_once()
        self.assertEqual(benchmark_mock.call_args.kwargs["skip_ohlc"], True)

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_web_symbol_add_fills_missing_metadata(self, metadata_mock, benchmark_mock):
        metadata_mock.return_value = {
            "name": "Apple Inc.",
            "country": "United States",
            "currency": "USD",
            "sector": "Technology",
            "instrument_type": "Common Stock",
        }
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY", "XLK"],
            "created": 2,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(reverse("symbol_add"), {"ticker": "AAPL", "exchange": "NASDAQ"}, follow=True)

        self.assertEqual(response.status_code, 200)
        symbol = Symbol.objects.get(ticker="AAPL", exchange="NASDAQ")
        self.assertEqual(symbol.name, "Apple Inc.")
        self.assertEqual(symbol.country, "United States")
        self.assertEqual(symbol.currency, "USD")
        self.assertEqual(symbol.sector, "Technology")
        self.assertEqual(symbol.instrument_type, "Common Stock")

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    @patch("core.services.symbol_enrichment.TwelveDataClient.fetch_symbol_metadata")
    def test_web_symbol_add_does_not_overwrite_user_entered_values(self, metadata_mock, benchmark_mock):
        metadata_mock.return_value = {
            "name": "Apple Inc.",
            "exchange": "NASDAQ",
            "country": "United States",
        }
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY"],
            "created": 1,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        self.client.post(
            reverse("symbol_add"),
            {"ticker": "AAPL", "exchange": "NYSE", "name": "Custom Name", "country": "France"},
            follow=True,
        )

        symbol = Symbol.objects.get(ticker="AAPL", exchange="NYSE")
        self.assertEqual(symbol.name, "Custom Name")
        self.assertEqual(symbol.country, "France")

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    @patch("core.views.enrich_symbols_metadata")
    def test_web_symbol_add_still_creates_symbol_if_enrichment_fails(self, enrich_mock, benchmark_mock):
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 0,
            "unchanged": 0,
            "skipped": 0,
            "errors": 1,
            "per_symbol": [{"symbol": "AAPL", "updated_fields": [], "error": "provider exploded", "skipped": False}],
        }
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY"],
            "created": 1,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(reverse("symbol_add"), {"ticker": "AAPL"}, follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(Symbol.objects.filter(ticker="AAPL").exists())
        messages = list(response.context["messages"])
        self.assertTrue(any("metadata update failed" in str(m) for m in messages))

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    @patch("core.views.enrich_symbols_metadata")
    def test_add_via_search_flow_also_triggers_enrichment(self, enrich_mock, benchmark_mock):
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 1,
            "unchanged": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "AAPL:NASDAQ", "updated_fields": ["sector"], "error": "", "skipped": False}],
        }
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY"],
            "created": 1,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(
            reverse("symbol_add"),
            {
                "ticker": "AAPL",
                "exchange": "NASDAQ",
                "name": "Apple Search",
                "instrument_type": "Common Stock",
                "country": "United States",
                "currency": "USD",
            },
        )

        self.assertEqual(response.status_code, 302)
        self.assertTrue(enrich_mock.called)
        self.assertTrue(benchmark_mock.called)

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    @patch("core.views.enrich_symbols_metadata")
    def test_symbol_add_calls_benchmark_ensure_with_skip_ohlc_true(self, enrich_mock, benchmark_mock):
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 0,
            "unchanged": 1,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "AAPL", "updated_fields": [], "error": "", "skipped": False}],
        }
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY", "XLK"],
            "created": 2,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(reverse("symbol_add"), {"ticker": "AAPL", "country": "US", "sector": "Technology"}, follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(benchmark_mock.called)
        self.assertEqual(benchmark_mock.call_args.kwargs["skip_ohlc"], True)

    @patch("core.views.sync_benchmark_etfs_for_symbols", side_effect=RuntimeError("bench fail"))
    @patch("core.views.enrich_symbols_metadata")
    def test_symbol_add_still_succeeds_if_benchmark_ensure_fails(self, enrich_mock, benchmark_mock):
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 0,
            "unchanged": 1,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "AAPL", "updated_fields": [], "error": "", "skipped": False}],
        }

        response = self.client.post(reverse("symbol_add"), {"ticker": "AAPL"}, follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(Symbol.objects.filter(ticker="AAPL").exists())
        messages = list(response.context["messages"])
        self.assertTrue(any("Benchmark ETF ensure failed" in str(m) for m in messages))

    @patch("core.views.enrich_symbols_metadata")
    def test_per_symbol_update_button_calls_enrichment(self, enrich_mock):
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 0,
            "unchanged": 1,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "AAPL:NASDAQ", "updated_fields": [], "error": "", "skipped": False}],
        }
        symbol = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", active=True)

        response = self.client.post(reverse("symbol_update_metadata", args=[symbol.id]), follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(enrich_mock.called)
        passed_symbols = enrich_mock.call_args.args[0]
        self.assertEqual([sym.id for sym in passed_symbols], [symbol.id])

    @patch("core.views.enrich_symbols_metadata")
    def test_bulk_update_missing_selects_only_symbols_with_blank_metadata(self, enrich_mock):
        complete = Symbol.objects.create(
            ticker="FULL",
            exchange="NASDAQ",
            name="Full",
            instrument_type="Common Stock",
            country="United States",
            currency="USD",
            sector="Technology",
            active=True,
        )
        missing = Symbol.objects.create(ticker="MISS", exchange="", active=True)
        inactive_missing = Symbol.objects.create(ticker="OFF", exchange="", active=False)
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 1,
            "unchanged": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "MISS", "updated_fields": ["exchange"], "error": "", "skipped": False}],
        }

        response = self.client.post(reverse("symbols_update_missing_metadata"), follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(enrich_mock.called)
        symbols = list(enrich_mock.call_args.args[0])
        self.assertEqual([sym.id for sym in symbols], [missing.id])
        self.assertNotIn(complete.id, [sym.id for sym in symbols])
        self.assertNotIn(inactive_missing.id, [sym.id for sym in symbols])

    @patch("core.views.enrich_symbols_metadata")
    def test_bulk_update_missing_displays_summary(self, enrich_mock):
        Symbol.objects.create(ticker="MISS", exchange="", active=True)
        enrich_mock.return_value = {
            "processed": 1,
            "updated": 1,
            "unchanged": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{"symbol": "MISS", "updated_fields": ["exchange"], "error": "", "skipped": False}],
        }

        response = self.client.post(reverse("symbols_update_missing_metadata"), follow=True)

        self.assertEqual(response.status_code, 200)
        messages = list(response.context["messages"])
        self.assertTrue(any("Metadata update: processed=1 updated=1" in str(m) for m in messages))

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    def test_ensure_benchmark_button_calls_service_with_skip_ohlc_true(self, benchmark_mock):
        Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", country="US", sector="Technology", active=True)
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY", "XLK"],
            "created": 2,
            "existing": 0,
            "ohlc": None,
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(reverse("symbols_ensure_benchmark_etfs"), follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(benchmark_mock.called)
        self.assertEqual(benchmark_mock.call_args.kwargs["skip_ohlc"], True)

    @patch("core.views.sync_benchmark_etfs_for_symbols")
    def test_sync_benchmark_button_calls_service_with_skip_ohlc_false(self, benchmark_mock):
        Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", country="US", sector="Technology", active=True)
        benchmark_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY", "XLK"],
            "created": 2,
            "existing": 0,
            "ohlc": {"symbols": 2, "bars": 50},
            "enrichment": {"per_symbol": []},
        }

        response = self.client.post(reverse("symbols_sync_benchmark_etfs"), follow=True)

        self.assertEqual(response.status_code, 200)
        self.assertTrue(benchmark_mock.called)
        self.assertEqual(benchmark_mock.call_args.kwargs["skip_ohlc"], False)

    def test_get_to_update_endpoints_not_allowed(self):
        symbol = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", active=True)

        response_single = self.client.get(reverse("symbol_update_metadata", args=[symbol.id]))
        response_bulk = self.client.get(reverse("symbols_update_missing_metadata"))
        response_ensure = self.client.get(reverse("symbols_ensure_benchmark_etfs"))
        response_sync = self.client.get(reverse("symbols_sync_benchmark_etfs"))

        self.assertEqual(response_single.status_code, 405)
        self.assertEqual(response_bulk.status_code, 405)
        self.assertEqual(response_ensure.status_code, 405)
        self.assertEqual(response_sync.status_code, 405)

    def test_backtest_edit_view_normalizes_existing_signal_lines_json(self):
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
        normalized_signal_lines = [{
            **signal_lines[0],
            "buy_gm_filter": "IGNORE",
            "buy_gm_operator": "AND",
            "buy_market_gm_current": "GM_POS",
            "buy_market_gm_market": "IGNORE",
            "buy_market_gm_sector": "IGNORE",
            "buy_market_operator": "AND",
            "sell_gm_filter": "IGNORE",
            "sell_gm_operator": "AND",
            "gm_buy_conditions": {
                "operator": "AND",
                "current": {"mode": "POS", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
            "gm_sell_market_exit_conditions": {
                "operator": "AND",
                "current": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
            "gm_push_buy_conditions": {
                "operator": "AND",
                "current": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
            "gm_push_sell_market_exit_conditions": {
                "operator": "AND",
                "current": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
        }]
        self.assertEqual(json.loads(response.context["signal_lines_json"]), normalized_signal_lines)
        body = response.content.decode()
        self.assertIn("1. Périmètre / Période", body)
        self.assertIn("2. Signaux d’entrée &amp; de sortie", body)
        self.assertIn("3. Conditions de marché", body)
        self.assertIn("4. Filtres de tradabilité &amp; risque", body)
        self.assertIn("5. Capital &amp; exécution", body)
        self.assertNotIn('data-role="buy_gm_filter"', body)
        self.assertNotIn('data-role="sell_gm_filter"', body)
        self.assertIn('"buy_gm_filter": "IGNORE"', body)
        self.assertIn('"buy_market_gm_current": "GM_POS"', body)
        self.assertNotIn('<option value="GM_POS" selected>', body)
        self.assertIn('"trading_model": "LATCH_STATEFUL"', body)
        self.assertIn('"buy": ["Af", "SPVa_basse"]', body)

    def test_backtest_edit_view_shows_persisted_market_cap_filter_values(self):
        scenario = Scenario.objects.create(
            name="Scenario Market Cap Edit",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT Market Cap Edit",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            universe_snapshot=[],
            settings={
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "ALLOW",
            },
        )

        response = self.client.get(reverse("backtest_update", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn('value="100000000"', body)
        self.assertIn('value="5000000000"', body)
        self.assertIn('<option value="ALLOW" selected>', body)

    def test_backtest_edit_view_hides_persisted_legacy_trend_filter_values(self):
        scenario = Scenario.objects.create(
            name="Scenario Trend Edit",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT Trend Edit",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            universe_snapshot=[],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            },
        )
        response = self.client.get(reverse("backtest_update", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Combiner les filtres de tendance avec", body)
        self.assertNotIn("GM_market", body)
        self.assertNotIn("GM_sector", body)

    def test_backtest_detail_displays_prep_warnings_even_when_failed(self):
        scenario = Scenario.objects.create(
            name="Scenario Prep Warning",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT Prep Warning",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            status=Backtest.Status.FAILED,
            error_message="Erreur technique après préparation",
            results={
                "prep": {
                    "did_fetch_bars": False,
                    "did_compute_metrics": False,
                    "notes": ["Attention : couverture prix partielle."],
                }
            },
        )

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Avertissements de préparation", body)
        self.assertIn("Attention : couverture prix partielle.", body)
        self.assertIn("Erreur technique après préparation", body)

    def test_backtest_detail_hides_legacy_buy_gm_filter_after_normalized_save(self):
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
        response = self.client.post(
            reverse("backtest_update", args=[bt.pk]),
            {
                "name": bt.name,
                "description": bt.description,
                "scenario": str(scenario.id),
                "start_date": bt.start_date,
                "end_date": bt.end_date,
                "capital_total": bt.capital_total,
                "capital_per_ticker": bt.capital_per_ticker,
                "capital_mode": bt.capital_mode,
                "ratio_threshold": bt.ratio_threshold,
                "include_all_tickers": "on",
                "signal_lines": json.dumps(signal_lines),
                "warmup_days": bt.warmup_days,
                "close_positions_at_end": "on",
            },
            follow=True,
        )
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("GM actuel", body)
        self.assertIn("GM positif", body)
        self.assertNotIn("Legacy GM filter:", body)
        self.assertNotIn("Filtre GM vente", body)

    def test_backtest_detail_displays_configured_market_cap_filter(self):
        scenario = Scenario.objects.create(
            name="Scenario Market Cap Detail",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT Market Cap Detail",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            universe_snapshot=[],
            settings={
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "BLOCK",
            },
        )

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Min Market Cap", body)
        self.assertIn("100000000", body)
        self.assertIn("Max Market Cap", body)
        self.assertIn("5000000000", body)
        self.assertIn("Missing Cap Policy", body)
        self.assertIn("BLOCK", body)

    def test_backtest_detail_omits_market_cap_filter_for_legacy_settings(self):
        scenario = Scenario.objects.create(
            name="Scenario Legacy Detail",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT Legacy Detail",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            universe_snapshot=[],
            settings={},
        )

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Min Market Cap", response.content.decode())

    def test_backtest_detail_hides_configured_legacy_trend_filters(self):
        scenario = Scenario.objects.create(
            name="Scenario Trend Detail",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
        )
        bt = Backtest.objects.create(
            name="BT Trend Detail",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            universe_snapshot=[],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            },
        )
        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))
        body = response.content.decode()
        self.assertNotIn("Opérateur des filtres de tendance", body)
        self.assertNotIn("GM_market", body)
        self.assertNotIn("GM_sector", body)

    def test_backtest_detail_displays_recent_high_drawdown_when_enabled(self):
        scenario = Scenario.objects.create(
            name="Scenario Anti Drop Detail",
            active=True,
            a=1, b=1, c=1, d=1, e=1,
            n1=5, n2=3, npente=100, slope_threshold=0.1,
            npente_basse=20, slope_threshold_basse=0.02, nglobal=20, history_years=2,
            recent_high_drawdown_lookback_days=10,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
        )
        bt = Backtest.objects.create(
            name="BT Anti Drop Detail",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
            universe_snapshot=[],
        )
        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))
        body = response.content.decode()
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", body)
        self.assertIn("fenêtre 10 j précédents", body)
        self.assertIn("-10.00%", body)

    def test_game_scenario_form_shows_only_line_market_conditions_for_gm(self):
        response = self.client.get(reverse("game_scenario_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("1. Périmètre / Période", body)
        self.assertIn("2. Signaux d’entrée &amp; de sortie", body)
        self.assertIn("3. Conditions de marché", body)
        self.assertIn("4. Filtres de tradabilité &amp; risque", body)
        self.assertIn("5. Capital &amp; exécution", body)
        self.assertIn("6. Avancé / Diagnostic", body)
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", body)
        self.assertIn("Un signal est un déclencheur", body)
        self.assertIn("Un filtre est une condition bloquante", body)
        self.assertNotIn("Filtres de tendance", body)
        self.assertNotIn("Combiner les filtres de tendance avec", body)
        self.assertIn("Seuil de déclenchement vente", body)
        self.assertIn("Seuil de déclenchement vente — pente basse", body)
        self.assertIn("Fenêtre du plus haut récent", body)
        self.assertIn("Repli maximal RHD", body)
        self.assertNotIn('data-role="buy_gm_filter"', body)
        self.assertNotIn('data-role="sell_gm_filter"', body)
        self.assertNotIn('data-role="buy_gm_operator"', body)
        self.assertNotIn('data-role="sell_gm_operator"', body)
        self.assertNotIn("GM_POS (momentum global positif)", body)
        self.assertNotIn("GM_NEG (momentum global négatif)", body)
        self.assertNotIn("GM_NEU (momentum global neutre)", body)

    def test_game_scenario_create_view_renders_market_cap_filter_fields(self):
        response = self.client.get(reverse("game_scenario_create"))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Min Market Cap", body)
        self.assertIn("Max Market Cap", body)
        self.assertIn("If Market Cap Missing", body)
        self.assertIn("Block BUY (recommended)", body)
        self.assertIn("Allow BUY", body)
        self.assertIn("Minimum historical company market capitalization required to allow BUY.", body)
        self.assertIn("Maximum historical company market capitalization allowed for BUY.", body)
        self.assertIn("What to do when no historical market capitalization exists at or before the BUY date.", body)

    def test_game_scenario_create_view_hides_global_trend_filter_fields(self):
        response = self.client.get(reverse("game_scenario_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Filtres de tendance", body)
        self.assertNotIn("Combiner les filtres de tendance avec", body)

    def test_game_scenario_edit_view_normalizes_existing_signal_lines_json(self):
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
        normalized_signal_lines = [{
            **signal_lines[0],
            "buy_gm_filter": "IGNORE",
            "buy_gm_operator": "AND",
            "buy_market_gm_current": "GM_POS",
            "buy_market_gm_market": "IGNORE",
            "buy_market_gm_sector": "IGNORE",
            "buy_market_operator": "AND",
            "sell_gm_filter": "IGNORE",
            "sell_gm_operator": "AND",
            "gm_buy_conditions": {
                "operator": "AND",
                "current": {"mode": "POS", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
            "gm_sell_market_exit_conditions": {
                "operator": "AND",
                "current": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
            "gm_push_buy_conditions": {
                "operator": "AND",
                "current": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
            "gm_push_sell_market_exit_conditions": {
                "operator": "AND",
                "current": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "market": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
                "sector": {"mode": "IGNORE", "threshold": None, "buy_threshold": None, "sell_threshold": None, "buy_max_threshold": None, "explicit_threshold": False},
            },
        }]
        self.assertEqual(json.loads(response.context["signal_lines_json"]), normalized_signal_lines)
        body = response.content.decode()
        self.assertIn("1. Périmètre / Période", body)
        self.assertIn("2. Signaux d’entrée &amp; de sortie", body)
        self.assertIn("3. Conditions de marché", body)
        self.assertIn("4. Filtres de tradabilité &amp; risque", body)
        self.assertIn("5. Capital &amp; exécution", body)
        self.assertIn("6. Avancé / Diagnostic", body)
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", body)
        self.assertNotIn('data-role="buy_gm_filter"', body)
        self.assertNotIn('data-role="sell_gm_filter"', body)
        self.assertIn('"buy_gm_filter": "IGNORE"', body)
        self.assertIn('"buy_market_gm_current": "GM_POS"', body)
        self.assertNotIn('<option value="GM_POS" selected>', body)
        self.assertIn('"trading_model": "LATCH_STATEFUL"', body)
        self.assertIn('"buy": ["Af", "SPVa_basse"]', body)

    def test_game_scenario_edit_view_shows_persisted_market_cap_filter_values(self):
        game = GameScenario.objects.create(
            name="Game Market Cap Edit",
            active=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "ALLOW",
            },
        )

        response = self.client.get(reverse("game_scenario_edit", args=[game.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn('value="100000000"', body)
        self.assertIn('value="5000000000"', body)
        self.assertIn('<option value="ALLOW" selected>', body)

    def test_game_scenario_edit_view_hides_persisted_legacy_trend_filter_values(self):
        game = GameScenario.objects.create(
            name="Game Trend Edit",
            active=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            },
        )
        response = self.client.get(reverse("game_scenario_edit", args=[game.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Combiner les filtres de tendance avec", body)
        self.assertNotIn("GM_market", body)
        self.assertNotIn("GM_sector", body)

    def test_game_scenario_detail_displays_configured_market_cap_filter(self):
        game = GameScenario.objects.create(
            name="Game Market Cap Detail",
            active=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "BLOCK",
            },
        )

        response = self.client.get(reverse("game_scenario_detail", args=[game.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Min Market Cap", body)
        self.assertIn("100000000", body)
        self.assertIn("Max Market Cap", body)
        self.assertIn("5000000000", body)
        self.assertIn("Missing Cap Policy", body)
        self.assertIn("BLOCK", body)

    def test_game_scenario_detail_omits_market_cap_filter_for_legacy_settings(self):
        game = GameScenario.objects.create(
            name="Game Legacy Detail",
            active=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={},
        )

        response = self.client.get(reverse("game_scenario_detail", args=[game.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertNotIn("Min Market Cap", response.content.decode())

    def test_game_scenario_detail_hides_configured_legacy_trend_filters(self):
        game = GameScenario.objects.create(
            name="Game Trend Detail",
            active=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            },
        )
        response = self.client.get(reverse("game_scenario_detail", args=[game.pk]))
        body = response.content.decode()
        self.assertNotIn("Opérateur des filtres de tendance", body)
        self.assertNotIn("GM_market", body)
        self.assertNotIn("GM_sector", body)

    def test_game_scenario_detail_displays_recent_high_drawdown_when_enabled(self):
        game = GameScenario.objects.create(
            name="Game Anti Drop Detail",
            active=True,
            recent_high_drawdown_lookback_days=10,
            recent_high_drawdown_max_drop_pct=Decimal("-0.10"),
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
        )
        response = self.client.get(reverse("game_scenario_detail", args=[game.pk]))
        body = response.content.decode()
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", body)
        self.assertIn("fenêtre 10 j précédents", body)
        self.assertIn("-10.00%", body)


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

    def _minimal_results(self, *, universe_meta=None):
        meta = {"start_date": "2024-01-01", "end_date": "2024-01-31"}
        if universe_meta is not None:
            meta["universe"] = universe_meta
        return {
            "meta": meta,
            "tickers": {
                self.symbol.ticker: {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [],
                        "final": {
                            "N": 0,
                            "S_G_N": "0",
                            "BT": "0",
                            "PNL_AMOUNT": "0",
                            "FINAL_EQUITY": "0",
                            "AVG_TRADE_AMOUNT": "0",
                            "TOTAL_GAIN_AMOUNT": "0",
                            "TOTAL_LOSS_AMOUNT": "0",
                            "PROFIT_FACTOR_AMOUNT": None,
                            "WIN_TRADES": 0,
                            "LOSS_TRADES": 0,
                            "WIN_RATE_AMOUNT": "0",
                            "MAX_GAIN_AMOUNT": None,
                            "MAX_LOSS_AMOUNT": None,
                            "TRADABLE_DAYS": 0,
                            "TRADABLE_DAYS_NOT_IN_POSITION": 0,
                            "TRADABLE_DAYS_IN_POSITION_CLOSED": 0,
                            "BMJ": "0",
                            "BMD": "0",
                        },
                    }]
                }
            },
            "portfolio": {"kpi": {}, "daily": []},
        }

    def _create_done_backtest(self, *, scenario=None, results=None, universe_snapshot=None):
        return Backtest.objects.create(
            name="BT Universe Metadata",
            scenario=scenario or self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            universe_snapshot=universe_snapshot if universe_snapshot is not None else [self.symbol.ticker],
            status=Backtest.Status.DONE,
            results=results or self._minimal_results(),
        )

    def _create_validated_sp500_membership(self, symbol=None, *, valid_from=None, valid_to=None):
        symbol = symbol or self.symbol
        start = valid_from or date(2024, 1, 1)
        end = valid_to or date(2024, 1, 31)
        universe = UniverseDefinition.objects.create(
            code="SP500",
            name="S&P 500",
            source="test",
            active=True,
        )
        UniverseMembership.objects.create(
            universe=universe,
            symbol=symbol,
            ticker=symbol.ticker,
            exchange=symbol.exchange,
            provider_symbol=f"{symbol.ticker}.US",
            valid_from=start,
            valid_to=valid_to,
            source="test",
        )
        batch = UniverseImportBatch.objects.create(
            universe=universe,
            provider="test",
            source_name="test",
            period_start=start,
            period_end=end,
            expected_member_count=1,
            imported_member_count=1,
            mapped_member_count=1,
            unmapped_member_count=0,
            status=UniverseCoverageStatus.VALIDATED,
        )
        current = start
        while current <= end:
            UniverseCoverageSnapshot.objects.create(
                universe=universe,
                import_batch=batch,
                coverage_date=current,
                expected_member_count=1,
                actual_member_count=1,
                mapped_member_count=1,
                unmapped_member_count=0,
                status=UniverseCoverageStatus.VALIDATED,
            )
            current += timedelta(days=1)
        return universe

    def _create_daily_bar(self, symbol, value_date):
        return DailyBar.objects.create(
            symbol=symbol,
            date=value_date,
            open=Decimal("10"),
            high=Decimal("10"),
            low=Decimal("10"),
            close=Decimal("10"),
            volume=100,
            source="test",
        )

    def test_backtest_detail_displays_dynamic_universe_metadata_with_business_wording(self):
        self.scenario.universe_mode = Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
        self.scenario.save(update_fields=["universe_mode"])
        bt = self._create_done_backtest(
            universe_snapshot=[
                {"ticker": "AAA", "exchange": "NYSE", "sector": "Technology"},
                {"ticker": "OLD", "exchange": "NYSE", "sector": "Industrials"},
            ],
            results=self._minimal_results(universe_meta={
                "mode": Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
                "universe_code": "SP500",
                "coverage_start": "2024-01-01",
                "coverage_end": "2024-01-31",
                "superset_count": 2,
                "ticker_count": 2,
                "source": "manual_csv",
            }),
        )

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Univers du backtest", body)
        self.assertIn("S&P500 historique dynamique", body)
        self.assertIn("SP500", body)
        self.assertIn("2024-01-01", body)
        self.assertIn("2024-01-31", body)
        self.assertIn("Période couverte par l’historique S&amp;P 500", body)
        self.assertIn("Actions analysées sur la période", body)
        self.assertIn("manual_csv", body)
        self.assertIn("Ce nombre correspond à toutes les actions ayant appartenu au S&amp;P 500", body)
        self.assertIn("L’appartenance à l’indice est ensuite évaluée date par date", body)
        self.assertNotIn("Tickers dans le superset", body)
        self.assertNotIn("Superset de tickers du backtest", body)
        self.assertNotIn("metadata d’univers", body)

    def test_backtest_results_displays_dynamic_universe_metadata(self):
        self.scenario.universe_mode = Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
        self.scenario.save(update_fields=["universe_mode"])
        bt = self._create_done_backtest(results=self._minimal_results(universe_meta={
            "mode": Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
            "universe_code": "SP500",
            "coverage_start": "2024-01-01",
            "coverage_end": "2024-01-31",
            "superset_count": 3,
            "ticker_count": 3,
            "source": "manual_csv",
        }))

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Univers du backtest", body)
        self.assertIn("S&P500 historique dynamique", body)
        self.assertIn("SP500", body)
        self.assertIn("2024-01-01", body)
        self.assertIn("2024-01-31", body)
        self.assertIn("Période couverte par l’historique S&amp;P 500", body)
        self.assertIn("Actions analysées sur la période", body)
        self.assertIn("manual_csv", body)
        self.assertNotIn("Tickers dans le superset", body)
        self.assertNotIn("metadata d’univers", body)

    def test_backtest_detail_warns_when_dynamic_universe_metadata_missing(self):
        self.scenario.universe_mode = Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
        self.scenario.save(update_fields=["universe_mode"])
        bt = self._create_done_backtest(results=self._minimal_results())

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Ce résultat a été généré avant l’ajout de cette information", body)
        self.assertIn("Relancez le backtest pour l’afficher", body)
        self.assertNotIn("metadata d’univers ne sont pas présentes", body)

    def test_backtest_detail_displays_static_universe_without_dynamic_claims(self):
        bt = self._create_done_backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Sélection statique de tickers", body)
        self.assertIn("Univers du backtest", body)
        self.assertNotIn("S&P500 historique dynamique", body)
        self.assertNotIn("Superset de tickers du backtest", body)
        self.assertNotIn("Préparation des données OHLC", body)

    def test_backtest_detail_displays_dynamic_ohlc_ready_state(self):
        self.scenario.universe_mode = Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
        self.scenario.save(update_fields=["universe_mode"])
        self._create_validated_sp500_membership(self.symbol)
        self._create_daily_bar(self.symbol, date(2024, 1, 1))
        self._create_daily_bar(self.symbol, date(2024, 1, 31))
        bt = self._create_done_backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Préparation du S&amp;P500 historique", body)
        self.assertIn("Prêt", body)
        self.assertIn("Prêt pour le backtest", body)
        self.assertIn("Les prix sont disponibles pour les 1 actions attendues", body)
        self.assertNotIn("Préparer les données OHLC</button>", body)

    @patch("core.views.launch_processing_job")
    def test_backtest_detail_displays_dynamic_ohlc_missing_without_launching_job(self, launch_mock):
        self.scenario.universe_mode = Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
        self.scenario.save(update_fields=["universe_mode"])
        self._create_validated_sp500_membership(self.symbol)
        bt = self._create_done_backtest()

        response = self.client.get(reverse("backtest_detail", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        launch_mock.assert_not_called()
        body = response.content.decode()
        self.assertIn("Préparation du S&amp;P500 historique", body)
        self.assertIn("Prêt avec avertissement", body)
        self.assertIn("0 actions sur 1 ont des prix disponibles", body)
        self.assertIn("AAA", body)
        self.assertIn("Télécharger les prix des actions", body)
        self.assertIn("Trigger", body)
        self.assertNotIn("Préparer les données OHLC</button>", body)

    @patch("core.views.launch_processing_job")
    def test_prepare_dynamic_universe_ohlc_post_launches_existing_job_with_safe_defaults(self, launch_mock):
        self.scenario.universe_mode = Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC
        self.scenario.save(update_fields=["universe_mode"])
        bt = self._create_done_backtest()
        launch_mock.return_value = SimpleNamespace(
            job=SimpleNamespace(id=123),
            dispatch_error=None,
        )

        response = self.client.post(reverse("backtest_prepare_dynamic_universe_ohlc", args=[bt.pk]))

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.url, reverse("backtest_detail", args=[bt.pk]))
        launch_mock.assert_called_once()
        kwargs = launch_mock.call_args.kwargs
        self.assertEqual(kwargs["job_type"], ProcessingJob.JobType.FETCH_BARS)
        self.assertEqual(kwargs["backtest"], bt)
        self.assertEqual(kwargs["scenario"], self.scenario)
        self.assertEqual(
            kwargs["message"],
            "Téléchargement des prix des actions en attente",
        )
        self.assertEqual(
            kwargs["task_kwargs"],
            {
                "backtest_id": bt.id,
                "provider": "eodhd",
                "force_refresh": False,
                "max_symbols": 50,
                "exclude_tickers": ["DKEEP", "DNEW", "KEEP", "NEW", "OLD", "DOLD"],
                "user_id": self.user.id,
            },
        )

    @patch("core.views.launch_processing_job")
    def test_prepare_dynamic_universe_ohlc_post_rejects_static_backtest(self, launch_mock):
        bt = self._create_done_backtest()

        response = self.client.post(reverse("backtest_prepare_dynamic_universe_ohlc", args=[bt.pk]))

        self.assertEqual(response.status_code, 302)
        launch_mock.assert_not_called()

    def _add_historical_market_cap(self, symbol, dt, value, provider="eodhd"):
        return HistoricalMarketCap.objects.create(
            symbol=symbol,
            date=dt,
            market_cap=Decimal(value),
            provider=provider,
        )

    def _add_bar(self, symbol, dt, price):
        return DailyBar.objects.create(
            symbol=symbol,
            date=dt,
            open=Decimal(price),
            high=Decimal(price),
            low=Decimal(price),
            close=Decimal(price),
            volume=1000,
        )

    def test_realized_gains_cumulative_series_aggregates_closing_events_only(self):
        results = {
            "meta": {"start_date": "2024-01-01"},
            "tickers": {
                "AAA": {
                    "lines": [
                        {
                            "events": [
                                {"date": "2024-01-02", "action": "BUY", "action_PNL_AMOUNT": "999"},
                                {"date": "2024-01-03", "action": "SELL", "action_PNL_AMOUNT": "500"},
                                {"date": "2024-01-04", "action": "SELL", "action_PNL_AMOUNT": "-300"},
                                {"date": "2024-01-05", "action": "BUY"},
                            ]
                        }
                    ]
                },
                "BBB": {
                    "lines": [
                        {
                            "events": [
                                {"date": "2024-01-03", "action": "SELL", "action_PNL_AMOUNT": "25"},
                                {"date": "2024-01-04", "action": "SELL+BUY", "action_PNL_AMOUNT": "-50"},
                                {"date": "2024-01-06", "action": "FORCED_SELL", "action_PNL_AMOUNT": "-400"},
                            ]
                        },
                        {
                            "events": [
                                {
                                    "date": "2024-01-07",
                                    "action": "SELL",
                                    "action_PNL_AMOUNT": "20",
                                    "action_reason": "Protection marché GM (GM marché négatif)",
                                }
                            ]
                        },
                    ]
                },
            },
        }

        series = _build_realized_gains_cumulative_series(results, initial_date="2024-01-01")

        self.assertEqual(series[0], {
            "date": "2024-01-01",
            "realized_pnl_daily": "0",
            "realized_pnl_cumulative": "0",
        })
        self.assertEqual(
            [(row["date"], row["realized_pnl_daily"], row["realized_pnl_cumulative"]) for row in series[1:]],
            [
                ("2024-01-03", "525", "525"),
                ("2024-01-04", "-350", "175"),
                ("2024-01-06", "-400", "-225"),
                ("2024-01-07", "20", "-205"),
            ],
        )

    def test_realized_gains_cumulative_series_stays_flat_without_closing_events(self):
        results = {
            "meta": {"start_date": "2024-01-01"},
            "tickers": {
                "AAA": {
                    "lines": [
                        {
                            "events": [
                                {"date": "2024-01-02", "action": "BUY", "action_PNL_AMOUNT": "999"},
                                {"date": "2024-01-03", "action": "BUY"},
                            ],
                            "daily": [],
                        }
                    ]
                }
            },
        }

        self.assertEqual(
            _build_realized_gains_cumulative_series(results, initial_date="2024-01-01"),
            [{"date": "2024-01-01", "realized_pnl_daily": "0", "realized_pnl_cumulative": "0"}],
        )

    def test_backtest_results_renders_realized_gains_curve_from_events_without_daily_rows(self):
        bt = Backtest.objects.create(
            name="BT Realized Events",
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
                "meta": {
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "detailed_daily_rows_omitted": True,
                },
                "tickers": {
                    self.symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "events": [
                                {"date": "2024-01-02", "action": "BUY", "price_close": "10"},
                                {"date": "2024-01-03", "action": "SELL", "action_PNL_AMOUNT": "15"},
                            ],
                            "daily_rows_omitted": True,
                            "final": {"N": 1, "BT": "0.15", "PNL_AMOUNT": "15"},
                        }]
                    }
                },
                "portfolio": {"kpi": {}, "daily": []},
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Gains réalisés cumulés", body)
        self.assertIn("Cette courbe cumule uniquement les gains/pertes réalisés", body)
        self.assertIn('"realized_pnl_cumulative": "0"', body)
        self.assertIn('"realized_pnl_cumulative": "15"', body)
        self.assertIn("realizedGainsChart", body)

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
        self.assertIn("Conditions de marché de la ligne : <b>GM actuel: GM positif</b>", body)

    def test_backtest_results_renders_sticky_quick_nav_with_section_anchors(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            ticker_lines={
                "AAA": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [{"date": "2024-01-02", "action": "BUY"}],
                        "final": {"N": 0, "BT": "0"},
                    }]
                }
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Accès rapide", body)
        for anchor in [
            "backtest-overview",
            "backtest-transactions",
            "backtest-selection",
            "backtest-charts",
            "backtest-details",
        ]:
            self.assertIn(f'href="#{anchor}"', body)
            self.assertIn(f'id="{anchor}"', body)
        self.assertIn('class="card backtest-quick-nav"', body)
        self.assertIn('position: sticky;', body)
        self.assertIn('overflow-x: auto;', body)
        self.assertIn('data-acc-key="selection"', body)
        self.assertIn('data-acc-key="daily"', body)
        self.assertIn('id="tickerLineSearch"', body)
        self.assertIn('id="tickerLineSelect"', body)

    def test_backtest_results_quick_nav_links_warnings_when_warning_section_exists(self):
        results = self._minimal_results()
        results["meta"]["warning_count"] = 1
        results["meta"]["warnings"] = [{
            "ticker": "AAA",
            "line_index": 1,
            "sell_date": "2024-01-03",
            "buy_date": "2024-01-04",
        }]
        results["tickers"]["AAA"]["lines"][0]["daily"] = [
            {"date": "2024-01-02", "action": "BUY"}
        ]
        bt = self._create_done_backtest(results=results)

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn('href="#backtest-warnings"', body)
        self.assertIn('id="backtest-warnings"', body)
        self.assertIn("Avertissements", body)

    def test_backtest_results_selection_lists_only_played_tickers(self):
        bbb = Symbol.objects.create(ticker="BBB", exchange="NYSE", active=True)
        ccc = Symbol.objects.create(ticker="CCC", exchange="NYSE", active=True)
        ddd = Symbol.objects.create(ticker="DDD", exchange="NYSE", active=True)
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            extra_symbols=[bbb, ccc, ddd],
            ticker_lines={
                "CCC": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [{"date": "2024-01-03", "action": "SELL+BUY"}],
                        "final": {"N": 1, "BT": "0.1"},
                    }]
                },
                "BBB": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [{"date": "2024-01-03", "action": None}],
                        "final": {"N": 0, "BT": "0"},
                    }]
                },
                "AAA": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [{"date": "2024-01-02", "action": "BUY"}],
                        "final": {"N": 0, "BT": "0"},
                    }]
                },
                "DDD": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [{"date": "2024-01-04", "action": "FORCED_SELL"}],
                        "final": {"N": 1, "BT": "0.1"},
                    }]
                },
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        options = response.context["ticker_options"]
        self.assertEqual([opt["ticker"] for opt in options], ["AAA", "CCC", "DDD"])
        body = response.content.decode()
        self.assertIn('id="tickerLineSearch"', body)
        self.assertIn("Rechercher un ticker", body)
        self.assertIn("Tickers joués uniquement", body)
        self.assertIn('value="AAA|1"', body)
        self.assertIn('value="CCC|1"', body)
        self.assertIn('value="DDD|1"', body)
        self.assertNotIn('value="BBB|1"', body)

    def test_backtest_results_selection_deduplicates_played_tickers(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            ticker_lines={
                "AAA": {
                    "lines": [
                        {
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "daily": [{"date": "2024-01-02", "action": "BUY"}],
                            "final": {"N": 0, "BT": "0"},
                        },
                        {
                            "line_index": 2,
                            "buy": ["A2"],
                            "sell": ["B2"],
                            "daily": [{"date": "2024-01-03", "action": "SELL"}],
                            "final": {"N": 1, "BT": "0.1"},
                        },
                    ]
                }
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        options = response.context["ticker_options"]
        self.assertEqual([opt["ticker"] for opt in options], ["AAA"])
        body = response.content.decode()
        self.assertEqual(body.count('data-ticker="AAA"'), 1)

    def test_backtest_results_selection_uses_final_trade_count_when_daily_rows_are_omitted(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            ticker_lines={
                "AAA": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [],
                        "daily_rows_omitted": True,
                        "final": {"N": 2, "BT": "0.1"},
                    }]
                }
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual([opt["ticker"] for opt in response.context["ticker_options"]], ["AAA"])
        self.assertContains(response, 'value="AAA|1"')

    def test_backtest_results_selection_shows_empty_state_without_played_tickers(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            ticker_lines={
                "AAA": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["A1"],
                        "sell": ["B1"],
                        "daily": [],
                        "final": {"N": 0, "BT": "0"},
                    }]
                }
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["ticker_options"], [])
        body = response.content.decode()
        self.assertIn("Aucun ticker joué sur ce backtest", body)
        self.assertNotIn('id="tickerLineSelect"', body)

    def test_backtest_results_renders_large_result_mode_warning_without_daily_rows(self):
        bt = Backtest.objects.create(
            name="BT Large Mode",
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
                "meta": {
                    "start_date": "2024-01-01",
                    "end_date": "2024-01-31",
                    "large_result_mode": True,
                    "detailed_daily_rows_omitted": True,
                    "estimated_daily_rows": 900000,
                },
                "tickers": {
                    self.symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["A1"],
                            "sell": ["B1"],
                            "daily": [],
                            "daily_rows_omitted": True,
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
                                "TRADABLE_DAYS": 3,
                                "TRADABLE_DAYS_NOT_IN_POSITION": 2,
                                "TRADABLE_DAYS_IN_POSITION_CLOSED": 1,
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
        self.assertIn("Large backtest mode", body)
        self.assertIn("Large mode: detailed diagnostics are loaded on demand.", body)

    @patch("core.services.provider_twelvedata.TwelveDataClient.time_series_daily", side_effect=AssertionError("no provider call expected"))
    @patch("core.services.provider_eodhd.EODHDClient.fetch_historical_market_cap", side_effect=AssertionError("no provider call expected"))
    def test_backtest_results_large_mode_builds_on_demand_diagnostic_from_local_data(self, _market_cap_mock, _td_mock):
        self._create_metric(self.symbol, "2024-01-02", P="100", Kf2bis="99", ratio_P="0.5")
        self._create_metric(self.symbol, "2024-01-03", P="101", Kf2bis="100", ratio_P="0.6")
        self._create_metric(self.symbol, "2024-01-04", P="102", Kf2bis="101", ratio_P="0.7")
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date="2024-01-03", alerts="Af")
        Alert.objects.create(symbol=self.symbol, scenario=self.scenario, date="2024-01-04", alerts="Bf")
        self._add_bar(self.symbol, "2024-01-02", "10")
        self._add_bar(self.symbol, "2024-01-03", "11")
        self._add_bar(self.symbol, "2024-01-04", "12")

        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        self.symbol.sector = "Technology"
        self.symbol.country = "US"
        self.symbol.save(update_fields=["sector", "country"])
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", country="US", active=True)
        xlk = Symbol.objects.create(ticker="XLK", exchange="NYSE", country="US", sector="Technology", active=True)
        self._add_bar(spy, "2024-01-02", "100")
        self._add_bar(spy, "2024-01-03", "110")
        self._add_bar(spy, "2024-01-04", "121")
        self._add_bar(xlk, "2024-01-02", "200")
        self._add_bar(xlk, "2024-01-03", "220")
        self._add_bar(xlk, "2024-01-04", "242")

        bt = Backtest.objects.create(
            name="BT Large Mode On Demand",
            scenario=self.scenario,
            start_date="2024-01-02",
            end_date="2024-01-04",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "buy": ["Af"],
                "sell": ["Bf"],
                "buy_market_gm_current": "GM_POS",
                "buy_market_gm_market": "GM_POS",
                "buy_market_gm_sector": "GM_POS",
            }],
            universe_snapshot=[self.symbol.ticker],
            results={
                "meta": {
                    "start_date": "2024-01-02",
                    "end_date": "2024-01-04",
                    "large_result_mode": True,
                    "detailed_daily_rows_omitted": True,
                    "estimated_daily_rows": 900000,
                },
                "tickers": {
                    self.symbol.ticker: {
                        "lines": [{
                            "line_index": 1,
                            "buy": ["Af"],
                            "sell": ["Bf"],
                            "buy_market_gm_current": "GM_POS",
                            "buy_market_gm_market": "GM_POS",
                            "buy_market_gm_sector": "GM_POS",
                            "daily": [],
                            "daily_rows_omitted": True,
                            "events": [
                                {"date": "2024-01-03", "action": "BUY", "price_close": "11"},
                                {"date": "2024-01-04", "action": "SELL", "price_close": "12", "action_G": "0.1"},
                            ],
                            "final": {"N": 1, "BT": "0.1"},
                        }]
                    }
                },
                "portfolio": {
                    "kpi": {},
                    "daily": [
                        {"date": "2024-01-02", "avg_global_nglobal": None},
                        {"date": "2024-01-03", "avg_global_nglobal": "0.2"},
                        {"date": "2024-01-04", "avg_global_nglobal": "0.3"},
                    ],
                },
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]), {"ticker": "AAA", "line": 1})

        self.assertEqual(response.status_code, 200)
        payload = response.context["diagnostic_chart_payload"]
        daily = response.context["daily"]
        self.assertEqual(bt.results["tickers"][self.symbol.ticker]["lines"][0]["daily"], [])
        self.assertEqual([row["date"] for row in daily], ["2024-01-02", "2024-01-03", "2024-01-04"])
        self.assertEqual([row["price_close"] for row in daily], ["10.000000", "11.000000", "12.000000"])
        self.assertEqual([row["shares"] for row in daily], [0, 1, 0])
        self.assertEqual([row["BT"] for row in daily], ["0", "0", "0.1"])
        self.assertEqual([row["BMJ"] for row in daily], ["0", "0", "0.05"])
        self.assertEqual([row["BMD"] for row in daily], [None, "0", "0.1"])
        self.assertEqual(daily[1]["alerts"], ["Af"])
        self.assertEqual(daily[2]["alerts"], ["Bf"])
        self.assertEqual(daily[1]["action"], "BUY")
        self.assertEqual(daily[2]["action"], "SELL")
        self.assertEqual(payload["markers"], [{"date": "2024-01-03", "type": "BUY"}, {"date": "2024-01-04", "type": "SELL"}])
        self.assertIn("P", payload["signal_series"])
        self.assertIn("Kf2bis", payload["signal_series"])
        self.assertEqual(payload["trend_filters"]["current"]["values"], [None, "0.2", "0.3"])
        self.assertEqual(payload["trend_filters"]["market"]["benchmark_ticker"], "SPY")
        self.assertEqual(payload["trend_filters"]["sector"]["benchmark_ticker"], "XLK")
        body = response.content.decode()
        self.assertIn("Large mode: detailed diagnostics are loaded on demand.", body)

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
        self.assertIsNone(payload["trend_filters"])
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
                self.assertIn("Filtre GM — rendement borné (%)", body)
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
        self.scenario.slope_sell_threshold = Decimal("0.05")
        self.scenario.slope_sell_threshold_basse = Decimal("0.01")
        self.scenario.save(update_fields=["slope_sell_threshold", "slope_sell_threshold_basse"])
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
        self.assertEqual(payload["thresholds"]["slope_sell_threshold"], str(self.scenario.slope_sell_threshold))
        self.assertEqual(payload["thresholds"]["slope_threshold_basse"], str(self.scenario.slope_threshold_basse))
        self.assertEqual(payload["thresholds"]["slope_sell_threshold_basse"], str(self.scenario.slope_sell_threshold_basse))

    def test_backtest_results_diagnostic_payload_exposes_recent_high_drawdown(self):
        self.scenario.recent_high_drawdown_lookback_days = 2
        self.scenario.recent_high_drawdown_max_drop_pct = Decimal("-0.10")
        self.scenario.save(update_fields=["recent_high_drawdown_lookback_days", "recent_high_drawdown_max_drop_pct"])
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["RHD_OK"], "sell": ["RHD_FAIL"], "daily": [
                    {"date": "2024-01-02", "price_close": "100", "action": None},
                    {"date": "2024-01-03", "price_close": "101", "action": "BUY"},
                    {"date": "2024-01-04", "price_close": "102", "action": "SELL"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["recent_high_drawdown"]["lookback_days"], 2)
        self.assertEqual(Decimal(payload["recent_high_drawdown"]["max_drop_pct"]), Decimal("-0.10"))
        self.assertEqual(len(payload["recent_high_drawdown"]["threshold_price"]), len(payload["dates"]))
        body = response.content.decode()
        self.assertIn("Seuil RHD", body)
        self.assertIn("Le signal anti-chute RHD affiche le seuil calculé à partir du plus haut récent", body)

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
        self.assertIn('label: "Seuil pente vente"', body)

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
        self.assertIn('label: "Seuil pente basse vente"', body)
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

    def test_backtest_results_diagnostic_payload_includes_market_cap_series_for_selected_ticker(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                    {"date": "2024-01-04", "price_close": "12", "action": "SELL"},
                ], "final": {}}]},
            },
        )
        self._add_historical_market_cap(self.symbol, "2024-01-02", "120000000")
        self._add_historical_market_cap(self.symbol, "2024-01-04", "125000000")

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(
            payload["market_cap"],
            {
                "label": "Historical Market Cap",
                "values": ["120000000", "120000000", "125000000"],
                "min": None,
                "max": None,
                "missing_policy": None,
                "has_data": True,
            },
        )
        body = response.content.decode()
        self.assertIn("Historical Market Cap", body)
        self.assertIn('id="diagnosticMarketCapChart"', body)
        self.assertIn('id="diagnosticPriceChart"', body)

    def test_backtest_results_diagnostic_payload_includes_market_cap_thresholds_and_policy(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": "BUY"},
                    {"date": "2024-01-03", "price_close": "11", "action": "SELL"},
                ], "final": {}}]},
            },
        )
        bt.settings = {
            "market_cap_min": "100000000",
            "market_cap_max": "5000000000",
            "market_cap_missing_policy": "BLOCK",
        }
        bt.save(update_fields=["settings"])

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["market_cap"]["min"], "100000000")
        self.assertEqual(payload["market_cap"]["max"], "5000000000")
        self.assertEqual(payload["market_cap"]["missing_policy"], "BLOCK")

    def test_backtest_results_diagnostic_payload_market_cap_never_uses_future_values(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                    {"date": "2024-01-04", "price_close": "12", "action": "SELL"},
                ], "final": {}}]},
            },
        )
        self._add_historical_market_cap(self.symbol, "2024-01-04", "125000000")

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["market_cap"]["values"], [None, None, "125000000"])

    def test_backtest_results_diagnostic_payload_omits_market_cap_when_no_filter_and_no_data(self):
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

        payload = response.context["diagnostic_chart_payload"]
        self.assertIsNone(payload["market_cap"])
        body = response.content.decode()
        self.assertNotIn("Historical Market Cap", body)
        self.assertNotIn('id="diagnosticMarketCapChart"', body)

    def test_backtest_results_diagnostic_payload_market_cap_panel_can_render_without_series_when_filter_configured(self):
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        bt.settings = {"market_cap_min": "100000000", "market_cap_missing_policy": "ALLOW"}
        bt.save(update_fields=["settings"])

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["market_cap"]["values"], [None, None])
        self.assertEqual(payload["market_cap"]["min"], "100000000")
        self.assertEqual(payload["market_cap"]["missing_policy"], "ALLOW")
        body = response.content.decode()
        self.assertIn("Historical Market Cap", body)
        self.assertIn("Uses latest known local market capitalization at or before each date.", body)
        self.assertIn("No local historical market-cap data available for this ticker/date range.", body)

    def test_backtest_results_diagnostic_payload_includes_market_trend_curve_when_configured(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", country="US", active=True)
        self._add_bar(spy, "2024-01-02", "100")
        self._add_bar(spy, "2024-01-03", "110")
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "buy_market_gm_market": "GM_POS"}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "buy_market_gm_market": "GM_POS", "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertTrue(payload["trend_filters"]["market"]["active"])
        self.assertEqual(payload["trend_filters"]["market"]["benchmark_ticker"], "SPY")
        self.assertEqual(payload["trend_filters"]["market"]["values"], [None, "0.1"])
        self.assertEqual(payload["trend_filters"]["zero_line"], "0")
        self.assertIn("diagnosticTrendMarketChart", response.content.decode())

    def test_backtest_results_diagnostic_payload_includes_new_gm_buy_current_curve_with_threshold(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        gm_buy_conditions = {
            "operator": "AND",
            "current": {"mode": "GM_POS", "threshold": "0.03", "explicit_threshold": True},
        }
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "gm_buy_conditions": gm_buy_conditions}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "gm_buy_conditions": gm_buy_conditions, "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        bt.results["portfolio"]["daily"] = [
            {"date": "2024-01-02", "avg_global_nglobal": ""},
            {"date": "2024-01-03", "avg_global_nglobal": "0.2"},
        ]
        bt.save(update_fields=["results"])
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        payload = response.context["diagnostic_chart_payload"]
        self.assertTrue(payload["trend_filters"]["current"]["active"])
        self.assertEqual(payload["trend_filters"]["current"]["values"], [None, "0.2"])
        self.assertEqual(payload["trend_filters"]["current"]["status"], "passed")
        self.assertEqual(payload["trend_filters"]["current"]["thresholds"][0]["role"], "BUY")
        self.assertEqual(payload["trend_filters"]["current"]["thresholds"][0]["threshold"], "0.03")
        self.assertIn("diagnosticTrendCurrentChart", response.content.decode())

    def test_backtest_results_diagnostic_payload_includes_gm_sell_market_exit_sector_curve(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        self.symbol.sector = "Technology"
        self.symbol.save(update_fields=["sector"])
        xlk = Symbol.objects.create(ticker="XLK", exchange="NYSE", country="US", sector="Technology", active=True)
        self._add_bar(xlk, "2024-01-02", "100")
        self._add_bar(xlk, "2024-01-03", "110")
        gm_sell_conditions = {
            "operator": "AND",
            "sector": {"mode": "GM_POS", "threshold": "0.05", "explicit_threshold": True},
        }
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "gm_sell_market_exit_conditions": gm_sell_conditions}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "gm_sell_market_exit_conditions": gm_sell_conditions, "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertTrue(payload["trend_filters"]["sector"]["active"])
        self.assertEqual(payload["trend_filters"]["sector"]["benchmark_ticker"], "XLK")
        self.assertEqual(payload["trend_filters"]["sector"]["values"], [None, "0.1"])
        self.assertEqual(payload["trend_filters"]["sector"]["thresholds"][0]["role"], "SELL")
        self.assertEqual(payload["trend_filters"]["sector"]["thresholds"][0]["threshold"], "0.05")
        self.assertIn("diagnosticTrendSectorChart", response.content.decode())

    def test_backtest_results_diagnostic_payload_omits_gm_push_when_not_configured(self):
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

        payload = response.context["diagnostic_chart_payload"]
        self.assertIsNone(payload["gm_push"])
        body = response.content.decode()
        self.assertNotIn("Impulsion GM Push", body)
        self.assertNotIn('<canvas id="diagnosticGmPushCurrentChart"', body)

    def test_backtest_results_diagnostic_payload_includes_gm_push_current_curve_with_states_and_thresholds(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        gm_push_buy_conditions = {
            "operator": "AND",
            "current": {
                "mode": "GM_POS",
                "threshold": "0.03",
                "buy_threshold": "0.03",
                "sell_threshold": "-0.03",
                "explicit_threshold": True,
            },
        }
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "gm_push_buy_conditions": gm_push_buy_conditions}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "gm_push_buy_conditions": gm_push_buy_conditions, "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                    {"date": "2024-01-04", "price_close": "12", "action": None},
                ], "final": {}}]},
            },
        )
        bt.results["portfolio"]["daily"] = [
            {"date": "2024-01-02", "gm_push_current": "0.01"},
            {"date": "2024-01-03", "gm_push_current": "0.05"},
            {"date": "2024-01-04", "gm_push_current": "0.04"},
        ]
        bt.save(update_fields=["results"])

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertTrue(payload["gm_push"]["current"]["active"])
        self.assertEqual(payload["gm_push"]["operator_buy"], "AND")
        self.assertEqual(payload["gm_push"]["current"]["values"], ["0.01", "0.05", "0.04"])
        self.assertEqual(payload["gm_push"]["current"]["states"], ["UNKNOWN", "POS_ACTIVE", "POS_ACTIVE"])
        self.assertEqual(payload["gm_push"]["current"]["thresholds"][0]["role"], "BUY")
        self.assertEqual(payload["gm_push"]["current"]["thresholds"][0]["threshold"], "0.03")
        self.assertEqual(payload["gm_push"]["current"]["thresholds"][1]["role"], "SELL")
        self.assertEqual(payload["gm_push"]["current"]["thresholds"][1]["threshold"], "0.03")
        body = response.content.decode()
        self.assertIn("Impulsion GM Push", body)
        self.assertIn("diagnosticGmPushCurrentChart", body)
        self.assertIn("État mémorisé", body)

    def test_backtest_results_diagnostic_payload_displays_implicit_zero_gm_push_thresholds(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        gm_push_buy_conditions = {
            "operator": "AND",
            "current": {
                "mode": "GM_POS",
                "threshold": None,
                "buy_threshold": None,
                "sell_threshold": None,
                "explicit_threshold": False,
            },
        }
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "gm_push_buy_conditions": gm_push_buy_conditions}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "gm_push_buy_conditions": gm_push_buy_conditions, "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        bt.results["portfolio"]["daily"] = [
            {"date": "2024-01-02", "gm_push_current": "-0.01"},
            {"date": "2024-01-03", "gm_push_current": "0.02"},
        ]
        bt.save(update_fields=["results"])

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["gm_push"]["current"]["states"], ["UNKNOWN", "POS_ACTIVE"])
        self.assertEqual(payload["gm_push"]["current"]["roles"][0]["buy_threshold"], "0")
        self.assertEqual(payload["gm_push"]["current"]["roles"][0]["sell_threshold"], "0")
        self.assertFalse(payload["gm_push"]["current"]["roles"][0]["explicit_threshold"])
        self.assertEqual(payload["gm_push"]["current"]["thresholds"][0]["threshold"], "0")
        self.assertEqual(payload["gm_push"]["current"]["thresholds"][1]["threshold"], "0")

    def test_backtest_results_diagnostic_payload_includes_gm_push_market_curve_with_benchmark(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", country="US", active=True)
        self._add_bar(spy, "2024-01-02", "100")
        self._add_bar(spy, "2024-01-03", "101")
        self._add_bar(spy, "2024-01-04", "105")
        gm_push_buy_conditions = {
            "operator": "AND",
            "market": {
                "mode": "GM_POS",
                "threshold": "0.02",
                "buy_threshold": "0.02",
                "sell_threshold": "0.02",
                "explicit_threshold": True,
            },
        }
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "gm_push_buy_conditions": gm_push_buy_conditions}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "gm_push_buy_conditions": gm_push_buy_conditions, "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                    {"date": "2024-01-04", "price_close": "12", "action": None},
                ], "final": {}}]},
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertTrue(payload["gm_push"]["market"]["active"])
        self.assertEqual(payload["gm_push"]["market"]["benchmark_ticker"], "SPY")
        self.assertEqual(payload["gm_push"]["market"]["values"], [None, "0.01", "0.0396039603960396039603960396"])
        self.assertEqual(payload["gm_push"]["market"]["states"], ["UNKNOWN", "UNKNOWN", "POS_ACTIVE"])
        self.assertIn("diagnosticGmPushMarketChart", response.content.decode())

    def test_backtest_results_diagnostic_payload_includes_gm_push_sector_curve_with_benchmark_and_exit_marker(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        self.symbol.sector = "Technology"
        self.symbol.save(update_fields=["sector"])
        xlk = Symbol.objects.create(ticker="XLK", exchange="NYSE", country="US", sector="Technology", active=True)
        self._add_bar(xlk, "2024-01-02", "100")
        self._add_bar(xlk, "2024-01-03", "103")
        self._add_bar(xlk, "2024-01-04", "95")
        gm_push_sell_conditions = {
            "operator": "OR",
            "sector": {
                "mode": "GM_NEG",
                "threshold": "0.02",
                "explicit_threshold": True,
            },
        }
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "gm_push_sell_market_exit_conditions": gm_push_sell_conditions}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "gm_push_sell_market_exit_conditions": gm_push_sell_conditions, "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": "BUY"},
                    {"date": "2024-01-03", "price_close": "11", "action": None},
                    {
                        "date": "2024-01-04",
                        "price_close": "12",
                        "action": "SELL",
                        "action_reason": "GM_PUSH_MARKET_EXIT (GM_push secteur négatif)",
                    },
                ], "final": {}}]},
            },
        )

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["gm_push"]["operator_sell"], "OR")
        self.assertTrue(payload["gm_push"]["sector"]["active"])
        self.assertEqual(payload["gm_push"]["sector"]["benchmark_ticker"], "XLK")
        self.assertEqual(payload["gm_push"]["sector"]["values"], [None, "0.03", "-0.07766990291262135922330097087"])
        self.assertEqual(payload["gm_push"]["sector"]["states"], ["UNKNOWN", "UNKNOWN", "NEG_ACTIVE"])
        self.assertEqual(payload["gm_push"]["sector"]["roles"][0]["threshold"], "0.02")
        self.assertEqual(payload["gm_push"]["sector"]["roles"][0]["sell_threshold"], "0.02")
        self.assertEqual(payload["gm_push"]["sector"]["thresholds"][1]["user_threshold"], "0.02")
        self.assertEqual(payload["gm_push"]["sector"]["thresholds"][1]["threshold"], "0.02")
        self.assertIn({"date": "2024-01-04", "type": "GM_PUSH_MARKET_EXIT"}, payload["markers"])
        body = response.content.decode()
        self.assertIn("diagnosticGmPushSectorChart", body)
        self.assertIn("GM_PUSH_MARKET_EXIT", body)

    def test_backtest_results_hides_legacy_trend_filter_diagnostic(self):
        self.scenario.nglobal = 1
        self.scenario.save(update_fields=["nglobal"])
        self.symbol.sector = "Technology"
        self.symbol.save(update_fields=["sector"])
        spy = Symbol.objects.create(ticker="SPY", exchange="NYSE", country="US", active=True)
        xlk = Symbol.objects.create(ticker="XLK", exchange="NYSE", country="US", sector="Technology", active=True)
        self._add_bar(spy, "2024-01-02", "100")
        self._add_bar(spy, "2024-01-03", "110")
        self._add_bar(xlk, "2024-01-02", "100")
        self._add_bar(xlk, "2024-01-03", "110")
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            ticker_lines={
                "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
        )
        bt.settings = {
            "trend_filter_operator": "AND",
            "trend_filter_gm_current": "GM_POS",
            "trend_filter_gm_market": "GM_POS",
            "trend_filter_gm_sector": "GM_POS",
        }
        bt.results["portfolio"]["daily"] = [
            {"date": "2024-01-02", "avg_global_nglobal": None},
            {"date": "2024-01-03", "avg_global_nglobal": "0.2"},
        ]
        bt.save(update_fields=["settings", "results"])
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertNotIn("Filtres de tendance (achat uniquement)", body)
        self.assertNotIn("Contexte GM de ligne", body)
        self.assertNotIn('<canvas id="diagnosticTrendCurrentChart"', body)
        self.assertNotIn('<canvas id="diagnosticTrendMarketChart"', body)
        self.assertNotIn('<canvas id="diagnosticTrendSectorChart"', body)

    def test_backtest_results_diagnostic_payload_warns_on_missing_market_mapping_or_data(self):
        symbol = Symbol.objects.create(ticker="INTL", exchange="", country="", sector="Technology", active=True)
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "buy_market_gm_market": "GM_POS"}],
            ticker_lines={
                "INTL": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "buy_market_gm_market": "GM_POS", "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
            extra_symbols=[symbol],
        )
        bt.universe_snapshot = ["INTL"]
        bt.save(update_fields=["universe_snapshot"])
        response = self.client.get(reverse("backtest_results", args=[bt.pk]), {"ticker": "INTL"})
        payload = response.context["diagnostic_chart_payload"]
        self.assertIn("missing benchmark mapping", payload["trend_filters"]["market"]["reason"])

    def test_backtest_results_diagnostic_payload_warns_on_missing_sector_mapping_or_data(self):
        symbol = Symbol.objects.create(ticker="UNKN", exchange="NYSE", country="US", sector="", active=True)
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "buy_market_gm_sector": "GM_POS"}],
            ticker_lines={
                "UNKN": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "buy_market_gm_sector": "GM_POS", "daily": [
                    {"date": "2024-01-02", "price_close": "10", "action": None},
                    {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
                ], "final": {}}]},
            },
            extra_symbols=[symbol],
        )
        bt.universe_snapshot = ["UNKN"]
        bt.save(update_fields=["universe_snapshot"])
        response = self.client.get(reverse("backtest_results", args=[bt.pk]), {"ticker": "UNKN"})
        payload = response.context["diagnostic_chart_payload"]
        self.assertIn("missing sector mapping", payload["trend_filters"]["sector"]["reason"])

    def test_backtest_results_diagnostic_payload_dedupes_benchmark_curves_and_caps_sector_summary(self):
        extra_symbols = [
            Symbol.objects.create(ticker="BBB", exchange="NYSE", country="US", sector="Financials", active=True),
            Symbol.objects.create(ticker="CCC", exchange="NYSE", country="US", sector="Healthcare", active=True),
            Symbol.objects.create(ticker="DDD", exchange="NYSE", country="US", sector="Energy", active=True),
            Symbol.objects.create(ticker="EEE", exchange="NYSE", country="US", sector="Industrials", active=True),
            Symbol.objects.create(ticker="FFF", exchange="NYSE", country="US", sector="Utilities", active=True),
            Symbol.objects.create(ticker="GGG", exchange="NYSE", country="US", sector="Real Estate", active=True),
            Symbol.objects.create(ticker="HHH", exchange="NYSE", country="US", sector="Materials", active=True),
        ]
        ticker_lines = {
            "AAA": {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                {"date": "2024-01-02", "price_close": "10", "action": None},
                {"date": "2024-01-03", "price_close": "11", "action": "BUY"},
            ], "buy_market_gm_market": "GM_POS", "buy_market_gm_sector": "GM_POS", "final": {}}]},
        }
        for symbol in extra_symbols:
            ticker_lines[symbol.ticker] = {"lines": [{"line_index": 1, "buy": ["Af"], "sell": ["Bf"], "daily": [
                {"date": "2024-01-02", "price_close": "10", "action": None},
                {"date": "2024-01-03", "price_close": "11", "action": None},
            ], "final": {}}]}
        bt = self._build_diagnostic_backtest(
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"], "buy_market_gm_market": "GM_POS", "buy_market_gm_sector": "GM_POS"}],
            ticker_lines=ticker_lines,
            extra_symbols=extra_symbols,
        )
        bt.universe_snapshot = ["AAA"] + [symbol.ticker for symbol in extra_symbols]
        bt.save(update_fields=["universe_snapshot"])
        response = self.client.get(reverse("backtest_results", args=[bt.pk]))
        payload = response.context["diagnostic_chart_payload"]
        self.assertEqual(payload["trend_filters"]["universe"]["market_benchmarks"], ["SPY"])
        self.assertEqual(len(payload["trend_filters"]["universe"]["sector_benchmarks"]), 6)
        self.assertEqual(payload["trend_filters"]["universe"]["sector_benchmark_total"], 7)
        self.assertIn("Too many distinct sector ETF curves", payload["trend_filters"]["universe"]["sector_warning"])

    def test_backtest_results_kpi_only_like_payload_does_not_include_market_cap_diagnostic(self):
        bt = Backtest.objects.create(
            name="BT KPI Only Market Cap",
            scenario=self.scenario,
            start_date="2024-01-01",
            end_date="2024-01-31",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"buy": ["Af"], "sell": ["Bf"]}],
            universe_snapshot=[self.symbol.ticker],
            settings={"market_cap_min": "100000000", "market_cap_missing_policy": "BLOCK"},
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
        self._add_historical_market_cap(self.symbol, "2024-01-02", "120000000")

        response = self.client.get(reverse("backtest_results", args=[bt.pk]))

        self.assertIsNone(response.context.get("diagnostic_chart_payload"))
        self.assertNotIn("Historical Market Cap", response.content.decode())
