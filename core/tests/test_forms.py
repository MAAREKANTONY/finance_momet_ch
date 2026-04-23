import json

from django.test import TestCase

from core.forms import BacktestForm, ScenarioForm, StudyScenarioForm, UniverseForm, GameScenarioForm
from core.models import GameScenario, Scenario, Study, Symbol, Universe


class SymbolPickerFormTests(TestCase):
    def setUp(self):
        self.sym1 = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", name="Apple", sector="Tech", active=True)
        self.sym2 = Symbol.objects.create(ticker="MSFT", exchange="NASDAQ", name="Microsoft", sector="Software", active=True)

    def test_universe_form_preloads_selected_symbols_with_metadata(self):
        universe = Universe.objects.create(name="US Big Tech", active=True)
        universe.symbols.set([self.sym1, self.sym2])

        form = UniverseForm(instance=universe)
        payload = json.loads(form.fields["symbols"].widget.attrs["data_selected_json"])

        self.assertEqual([item["ticker"] for item in payload], ["AAPL", "MSFT"])
        self.assertEqual(payload[0]["sector"], "Tech")
        self.assertEqual(form.fields["symbols"].initial, [self.sym1, self.sym2])

    def test_scenario_form_accepts_csv_symbol_ids(self):
        data = {
            "name": "Momentum",
            "description": "test",
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
            "symbols": f"{self.sym1.id},{self.sym2.id}",
        }
        form = ScenarioForm(data=data)
        self.assertTrue(form.is_valid(), form.errors)
        scenario = form.save()
        self.assertEqual(list(scenario.symbols.order_by("ticker").values_list("ticker", flat=True)), ["AAPL", "MSFT"])

    def test_study_scenario_form_preloads_selected_symbols(self):
        scenario = Scenario.objects.create(name="Clone", active=True)
        scenario.symbols.set([self.sym2])
        study = Study.objects.create(name="Study", scenario=scenario)

        form = StudyScenarioForm(instance=study.scenario, prefix="sc")
        payload = json.loads(form.fields["symbols"].widget.attrs["data_selected_json"])
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["ticker"], "MSFT")
        self.assertEqual(payload[0]["sector"], "Software")

    def test_game_scenario_form_cleans_signal_lines(self):
        form = GameScenarioForm(
            data={
                "name": "Game 1",
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "nglobal": "20",
                "presence_threshold_pct": "30",
                "email_recipients": "",
                "a": "1",
                "b": "1",
                "c": "1",
                "d": "1",
                "e": "1",
                "n1": "5",
                "n2": "3",
                "capital_total": "10000",
                "capital_per_ticker": "1000",
                "capital_mode": "FIXED",
                "signal_lines": json.dumps([
                    {"buy": ["Af"], "sell": ["Bf"], "buy_gm_filter": "GM_POS", "buy_gm_operator": "OR"}
                ]),
                "warmup_days": "30",
                "close_positions_at_end": "on",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        cleaned = form.cleaned_data["signal_lines"]
        self.assertEqual(cleaned[0]["buy_gm_filter"], "GM_POS")
        self.assertEqual(cleaned[0]["buy_gm_operator"], "OR")

    def test_backtest_form_rejects_invalid_price_range(self):
        scenario = Scenario.objects.create(name="Price Range", active=True)
        form = BacktestForm(
            data={
                "name": "BT price range",
                "description": "",
                "scenario": str(scenario.id),
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "capital_total": "1000",
                "capital_per_ticker": "100",
                "capital_mode": "FIXED",
                "ratio_threshold": "0",
                "include_all_tickers": "on",
                "signal_lines": json.dumps([
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}
                ]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
                "min_price": "100",
                "max_price": "50",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("max_price", form.errors)

    def test_backtest_form_persists_optional_price_range_in_settings(self):
        scenario = Scenario.objects.create(name="BT Settings", active=True)
        form = BacktestForm(
            data={
                "name": "BT settings",
                "description": "",
                "scenario": str(scenario.id),
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "capital_total": "1000",
                "capital_per_ticker": "100",
                "capital_mode": "FIXED",
                "ratio_threshold": "0",
                "include_all_tickers": "on",
                "signal_lines": json.dumps([
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}
                ]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
                "min_price": "10",
                "max_price": "50",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.settings["min_price"], "10")
        self.assertEqual(obj.settings["max_price"], "50")

    def test_game_scenario_form_persists_optional_price_range_in_settings(self):
        form = GameScenarioForm(
            data={
                "name": "Game price range",
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "nglobal": "20",
                "presence_threshold_pct": "30",
                "email_recipients": "",
                "a": "1",
                "b": "1",
                "c": "1",
                "d": "1",
                "e": "1",
                "n1": "5",
                "n2": "3",
                "capital_total": "10000",
                "capital_per_ticker": "1000",
                "capital_mode": "FIXED",
                "signal_lines": json.dumps([
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}
                ]),
                "warmup_days": "30",
                "close_positions_at_end": "on",
                "min_price": "12.5",
                "max_price": "80",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.settings["min_price"], "12.5")
        self.assertEqual(obj.settings["max_price"], "80")

    def test_game_scenario_form_allows_empty_price_range(self):
        obj = GameScenario.objects.create(
            name="Game existing",
            settings={"min_price": "12", "max_price": "45"},
        )
        form = GameScenarioForm(
            data={
                "name": "Game existing",
                "description": "",
                "active": "on",
                "study_days": str(obj.study_days),
                "tradability_threshold": str(obj.tradability_threshold),
                "npente": str(obj.npente),
                "slope_threshold": str(obj.slope_threshold),
                "npente_basse": str(obj.npente_basse),
                "slope_threshold_basse": str(obj.slope_threshold_basse),
                "nglobal": str(obj.nglobal),
                "presence_threshold_pct": str(obj.presence_threshold_pct),
                "email_recipients": "",
                "a": str(obj.a),
                "b": str(obj.b),
                "c": str(obj.c),
                "d": str(obj.d),
                "e": str(obj.e),
                "n1": str(obj.n1),
                "n2": str(obj.n2),
                "capital_total": str(obj.capital_total),
                "capital_per_ticker": str(obj.capital_per_ticker),
                "capital_mode": obj.capital_mode,
                "signal_lines": json.dumps([
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}
                ]),
                "warmup_days": str(obj.warmup_days),
                "close_positions_at_end": "on",
                "min_price": "",
                "max_price": "",
            },
            instance=obj,
        )

        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertNotIn("min_price", saved.settings)
        self.assertNotIn("max_price", saved.settings)
