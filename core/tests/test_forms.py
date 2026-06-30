import json

from django.test import TestCase

from core.forms import BacktestForm, ScenarioForm, StudyScenarioForm, UniverseForm, GameScenarioForm
from core.models import Backtest, GameScenario, Scenario, Study, Symbol, Universe


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
            "rhd_ok_reactivation_mode": "classic",
            "rhd_ok_rebound_threshold": "0.08",
            "rhd_ok_confirmation_days": "2",
            "rhd_ok_reentry_max_drawdown": "0.40",
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

    def test_scenario_form_renders_sell_threshold_fields(self):
        form = ScenarioForm()
        self.assertIn("universe_mode", form.fields)
        self.assertIn("slope_sell_threshold", form.fields)
        self.assertIn("slope_sell_threshold_basse", form.fields)
        self.assertIn("recent_high_drawdown_lookback_days", form.fields)
        self.assertIn("recent_high_drawdown_max_drop_pct", form.fields)
        self.assertIn("rhd_ok_reactivation_mode", form.fields)
        self.assertIn("rhd_ok_rebound_threshold", form.fields)
        self.assertEqual(form.fields["universe_mode"].initial, Scenario.UniverseMode.STATIC_TICKERS)
        self.assertEqual(form.fields["slope_sell_threshold"].label, "Seuil de déclenchement vente")
        self.assertIn("Si vide, le seuil d'achat est réutilisé.", form.fields["slope_sell_threshold"].help_text)
        self.assertEqual(form.fields["recent_high_drawdown_lookback_days"].label, "Fenêtre du plus haut récent")

    def test_scenario_default_universe_mode_is_static_tickers(self):
        scenario = Scenario.objects.create(name="Default universe mode", active=True)
        self.assertEqual(scenario.universe_mode, Scenario.UniverseMode.STATIC_TICKERS)

    def test_scenario_form_exposes_universe_mode_choices_and_saves_dynamic_mode(self):
        form = ScenarioForm(data={
            "name": "Scenario dynamic universe",
            "description": "test",
            "is_default": "",
            "universe_mode": Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC,
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
            "rhd_ok_reactivation_mode": "classic",
            "rhd_ok_rebound_threshold": "0.08",
            "rhd_ok_confirmation_days": "2",
            "rhd_ok_reentry_max_drawdown": "0.40",
            "nglobal": "20",
            "history_years": "2",
            "active": "on",
            "symbols": f"{self.sym1.id},{self.sym2.id}",
        })
        choices = {choice[0] for choice in form.fields["universe_mode"].choices}
        self.assertIn(Scenario.UniverseMode.STATIC_TICKERS, choices)
        self.assertIn(Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC, choices)
        self.assertTrue(form.is_valid(), form.errors)
        scenario = form.save()
        self.assertEqual(scenario.universe_mode, Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)
        self.assertEqual(set(scenario.symbols.values_list("ticker", flat=True)), {"AAPL", "MSFT"})

    def test_scenario_form_missing_universe_mode_keeps_static_tickers_and_symbols(self):
        form = ScenarioForm(data={
            "name": "Scenario legacy post",
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
            "rhd_ok_reactivation_mode": "classic",
            "rhd_ok_rebound_threshold": "0.08",
            "rhd_ok_confirmation_days": "2",
            "rhd_ok_reentry_max_drawdown": "0.40",
            "nglobal": "20",
            "history_years": "2",
            "active": "on",
            "symbols": f"{self.sym1.id}",
        })
        self.assertTrue(form.is_valid(), form.errors)
        scenario = form.save()
        self.assertEqual(scenario.universe_mode, Scenario.UniverseMode.STATIC_TICKERS)
        self.assertEqual(list(scenario.symbols.values_list("ticker", flat=True)), ["AAPL"])

    def test_scenario_form_saves_explicit_sell_thresholds(self):
        form = ScenarioForm(data={
            "name": "Scenario sell thresholds",
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
            "slope_sell_threshold": "0.05",
            "npente_basse": "20",
            "slope_threshold_basse": "0.02",
            "slope_sell_threshold_basse": "0.01",
            "recent_high_drawdown_lookback_days": "10",
            "recent_high_drawdown_max_drop_pct": "-0.10",
            "rhd_ok_reactivation_mode": "classic",
            "rhd_ok_rebound_threshold": "0.08",
            "rhd_ok_confirmation_days": "2",
            "rhd_ok_reentry_max_drawdown": "0.40",
            "nglobal": "20",
            "history_years": "2",
            "active": "on",
            "symbols": f"{self.sym1.id}",
        })
        self.assertTrue(form.is_valid(), form.errors)
        scenario = form.save()
        self.assertEqual(str(scenario.slope_sell_threshold), "0.05")
        self.assertEqual(str(scenario.slope_sell_threshold_basse), "0.01")
        self.assertEqual(scenario.recent_high_drawdown_lookback_days, 10)
        self.assertEqual(str(scenario.recent_high_drawdown_max_drop_pct), "-0.10")
        self.assertEqual(scenario.rhd_ok_reactivation_mode, "classic")

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
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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

    def test_game_scenario_form_renders_sell_threshold_fields(self):
        form = GameScenarioForm()
        self.assertFalse(hasattr(GameScenario, "universe_mode"))
        self.assertNotIn("universe_mode", form.fields)
        self.assertIn("slope_sell_threshold", form.fields)
        self.assertIn("slope_sell_threshold_basse", form.fields)
        self.assertIn("recent_high_drawdown_lookback_days", form.fields)
        self.assertIn("recent_high_drawdown_max_drop_pct", form.fields)
        self.assertIn("rhd_ok_reactivation_mode", form.fields)
        self.assertIn("rhd_ok_rebound_threshold", form.fields)
        self.assertEqual(form.fields["slope_sell_threshold_basse"].label, "Seuil de déclenchement vente — pente basse")
        self.assertEqual(form.fields["recent_high_drawdown_max_drop_pct"].label, "Repli maximal RHD")

    def test_game_scenario_form_saves_explicit_sell_thresholds(self):
        form = GameScenarioForm(
            data={
                "name": "Game sell thresholds",
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "slope_sell_threshold": "0.05",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "slope_sell_threshold_basse": "0.01",
                "recent_high_drawdown_lookback_days": "10",
                "recent_high_drawdown_max_drop_pct": "-0.10",
                "rhd_ok_reactivation_mode": "rebound_confirmed",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        game = form.save()
        self.assertEqual(str(game.slope_sell_threshold), "0.05")
        self.assertEqual(str(game.slope_sell_threshold_basse), "0.01")
        self.assertEqual(game.recent_high_drawdown_lookback_days, 10)
        self.assertEqual(str(game.recent_high_drawdown_max_drop_pct), "-0.10")
        self.assertEqual(game.rhd_ok_reactivation_mode, "rebound_confirmed")
        self.assertEqual(str(game.rhd_ok_rebound_threshold), "0.08")
        self.assertEqual(str(game.rhd_ok_reentry_max_drawdown), "0.40")

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

    def test_backtest_form_persists_market_cap_filter_settings(self):
        scenario = Scenario.objects.create(name="BT Market Cap Settings", active=True)
        form = BacktestForm(
            data={
                "name": "BT market cap settings",
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
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "ALLOW",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.settings["market_cap_min"], "100000000")
        self.assertEqual(obj.settings["market_cap_max"], "5000000000")
        self.assertEqual(obj.settings["market_cap_missing_policy"], "ALLOW")

    def test_backtest_form_allows_empty_market_cap_filter_and_defaults_policy(self):
        scenario = Scenario.objects.create(name="BT Empty Market Cap", active=True)
        obj = Backtest.objects.create(
            name="BT existing market cap",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-03",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "ALLOW",
            },
        )
        form = BacktestForm(
            data={
                "name": "BT existing market cap",
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
                "market_cap_min": "",
                "market_cap_max": "",
                "market_cap_missing_policy": "BLOCK",
            },
            instance=obj,
        )

        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertNotIn("market_cap_min", saved.settings)
        self.assertNotIn("market_cap_max", saved.settings)
        self.assertNotIn("market_cap_missing_policy", saved.settings)
        self.assertEqual(BacktestForm(instance=saved).fields["market_cap_missing_policy"].initial, "BLOCK")

    def test_backtest_form_rejects_invalid_market_cap_range(self):
        scenario = Scenario.objects.create(name="BT Bad Market Cap", active=True)
        form = BacktestForm(
            data={
                "name": "BT bad market cap",
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
                "market_cap_min": "5000000000",
                "market_cap_max": "100000000",
                "market_cap_missing_policy": "BLOCK",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("market_cap_max", form.errors)

    def test_backtest_form_persists_trend_filter_settings(self):
        scenario = Scenario.objects.create(name="BT Trend Filters", active=True)
        form = BacktestForm(
            data={
                "name": "BT trend filters",
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
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": [], "buy_gm_filter": "IGNORE"}
                ]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.settings["trend_filter_operator"], "OR")
        self.assertEqual(obj.settings["trend_filter_gm_current"], "GM_POS")
        self.assertEqual(obj.settings["trend_filter_gm_market"], "GM_NEG")
        self.assertEqual(obj.settings["trend_filter_gm_sector"], "GM_NEU")
        self.assertEqual(obj.signal_lines[0]["buy_gm_filter"], "IGNORE")

    def test_backtest_form_preserves_legacy_trend_settings_when_fields_are_not_submitted(self):
        scenario = Scenario.objects.create(name="BT Hidden Trend Filters", active=True)
        obj = Backtest.objects.create(
            name="BT hidden trend filters",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-03",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            },
        )
        form = BacktestForm(
            data={
                "name": obj.name,
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
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.settings["trend_filter_operator"], "OR")
        self.assertEqual(saved.settings["trend_filter_gm_current"], "GM_POS")
        self.assertEqual(saved.settings["trend_filter_gm_market"], "GM_NEG")
        self.assertEqual(saved.settings["trend_filter_gm_sector"], "GM_NEU")

    def test_backtest_form_omits_inert_trend_filter_settings(self):
        scenario = Scenario.objects.create(name="BT Empty Trend Filters", active=True)
        obj = Backtest.objects.create(
            name="BT existing trend filters",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-03",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": [], "buy_gm_filter": "GM_POS"}],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
            },
        )
        form = BacktestForm(
            data={
                "name": obj.name,
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
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": [], "buy_gm_filter": "GM_POS"}
                ]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
                "trend_filter_operator": "AND",
                "trend_filter_gm_current": "IGNORE",
                "trend_filter_gm_market": "IGNORE",
                "trend_filter_gm_sector": "IGNORE",
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.settings["trend_filter_operator"], "AND")
        self.assertEqual(saved.settings["trend_filter_gm_current"], "GM_POS")
        self.assertNotIn("trend_filter_gm_market", saved.settings)
        self.assertNotIn("trend_filter_gm_sector", saved.settings)
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")

    def test_backtest_form_normalizes_legacy_gm_values_on_save(self):
        scenario = Scenario.objects.create(name="BT Preserve Legacy GM", active=True)
        obj = Backtest.objects.create(
            name="BT preserve legacy gm",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-03",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["Af"],
                "sell": ["Bf"],
                "buy_gm_filter": "GM_POS",
                "buy_gm_operator": "OR",
                "sell_gm_filter": "GM_NEG",
                "sell_gm_operator": "AND",
            }],
            settings={},
        )
        form = BacktestForm(
            data={
                "name": obj.name,
                "description": "",
                "scenario": str(scenario.id),
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "capital_total": "1000",
                "capital_per_ticker": "100",
                "capital_mode": "FIXED",
                "ratio_threshold": "0",
                "include_all_tickers": "on",
                "signal_lines": json.dumps([{
                    "trading_model": "LEGACY_DAILY",
                    "buy": ["Af"],
                    "sell": ["Bf"],
                    "buy_gm_filter": "GM_POS",
                    "buy_gm_operator": "OR",
                    "sell_gm_filter": "GM_NEG",
                    "sell_gm_operator": "AND",
                }]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertNotIn("trend_filter_gm_current", saved.settings)
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")
        self.assertEqual(saved.signal_lines[0]["buy_gm_operator"], "AND")
        self.assertEqual(saved.signal_lines[0]["buy_market_gm_current"], "GM_POS")
        self.assertEqual(saved.signal_lines[0]["sell_gm_filter"], "IGNORE")
        self.assertEqual(saved.signal_lines[0]["sell_gm_operator"], "AND")

    def test_backtest_form_trend_filter_gm_current_wins_over_legacy_buy_gm_filter(self):
        scenario = Scenario.objects.create(name="BT Trend Current Wins", active=True)
        obj = Backtest.objects.create(
            name="BT trend current wins",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-03",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["Af"],
                "sell": ["Bf"],
                "buy_gm_filter": "GM_POS",
            }],
            settings={"trend_filter_gm_current": "GM_NEG"},
        )
        form = BacktestForm(
            data={
                "name": obj.name,
                "description": "",
                "scenario": str(scenario.id),
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "capital_total": "1000",
                "capital_per_ticker": "100",
                "capital_mode": "FIXED",
                "ratio_threshold": "0",
                "include_all_tickers": "on",
                "signal_lines": json.dumps([{
                    "trading_model": "LEGACY_DAILY",
                    "buy": ["Af"],
                    "sell": ["Bf"],
                    "buy_gm_filter": "GM_POS",
                }]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
                "trend_filter_operator": "AND",
                "trend_filter_gm_current": "GM_NEG",
                "trend_filter_gm_market": "IGNORE",
                "trend_filter_gm_sector": "IGNORE",
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.settings["trend_filter_gm_current"], "GM_NEG")
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")

    def test_backtest_form_preserves_legacy_composite_gm_current_on_save(self):
        scenario = Scenario.objects.create(name="BT Composite Legacy GM", active=True)
        obj = Backtest.objects.create(
            name="BT composite legacy gm",
            scenario=scenario,
            start_date="2024-01-01",
            end_date="2024-01-03",
            capital_total="1000",
            capital_per_ticker="100",
            capital_mode="FIXED",
            include_all_tickers=True,
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["Af"],
                "sell": ["Bf"],
                "buy_gm_filter": "GM_POS_OR_NEU",
            }],
            settings={},
        )
        form = BacktestForm(
            data={
                "name": obj.name,
                "description": "",
                "scenario": str(scenario.id),
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "capital_total": "1000",
                "capital_per_ticker": "100",
                "capital_mode": "FIXED",
                "ratio_threshold": "0",
                "include_all_tickers": "on",
                "signal_lines": json.dumps([{
                    "trading_model": "LEGACY_DAILY",
                    "buy": ["Af"],
                    "sell": ["Bf"],
                    "buy_gm_filter": "GM_POS_OR_NEU",
                }]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
                "trend_filter_operator": "AND",
                "trend_filter_gm_current": "GM_POS_OR_NEU",
                "trend_filter_gm_market": "IGNORE",
                "trend_filter_gm_sector": "IGNORE",
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.settings["trend_filter_gm_current"], "GM_POS_OR_NEU")
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")

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
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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

    def test_game_scenario_form_persists_market_cap_filter_settings(self):
        form = GameScenarioForm(
            data={
                "name": "Game market cap",
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "ALLOW",
            }
        )

        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.settings["market_cap_min"], "100000000")
        self.assertEqual(obj.settings["market_cap_max"], "5000000000")
        self.assertEqual(obj.settings["market_cap_missing_policy"], "ALLOW")

    def test_game_scenario_form_allows_empty_market_cap_filter(self):
        obj = GameScenario.objects.create(
            name="Game existing market cap",
            settings={
                "market_cap_min": "100000000",
                "market_cap_max": "5000000000",
                "market_cap_missing_policy": "ALLOW",
            },
        )
        form = GameScenarioForm(
            data={
                "name": "Game existing market cap",
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
                "market_cap_min": "",
                "market_cap_max": "",
                "market_cap_missing_policy": "BLOCK",
            },
            instance=obj,
        )

        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertNotIn("market_cap_min", saved.settings)
        self.assertNotIn("market_cap_max", saved.settings)
        self.assertNotIn("market_cap_missing_policy", saved.settings)
        self.assertEqual(GameScenarioForm(instance=saved).fields["market_cap_missing_policy"].initial, "BLOCK")

    def test_game_scenario_form_rejects_invalid_market_cap_range(self):
        form = GameScenarioForm(
            data={
                "name": "Game bad market cap",
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
                "market_cap_min": "5000000000",
                "market_cap_max": "100000000",
                "market_cap_missing_policy": "BLOCK",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("market_cap_max", form.errors)

    def test_game_scenario_form_persists_trend_filter_settings(self):
        form = GameScenarioForm(
            data={
                "name": "Game trend filters",
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
                    {"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": [], "buy_gm_filter": "IGNORE"}
                ]),
                "warmup_days": "30",
                "close_positions_at_end": "on",
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        obj = form.save()
        self.assertEqual(obj.settings["trend_filter_operator"], "OR")
        self.assertEqual(obj.settings["trend_filter_gm_current"], "GM_POS")
        self.assertEqual(obj.settings["trend_filter_gm_market"], "GM_NEG")
        self.assertEqual(obj.settings["trend_filter_gm_sector"], "GM_NEU")
        self.assertEqual(obj.signal_lines[0]["buy_gm_filter"], "IGNORE")

    def test_game_scenario_form_preserves_legacy_trend_settings_when_fields_are_not_submitted(self):
        obj = GameScenario.objects.create(
            name="Game hidden trend filters",
            signal_lines=[{"trading_model": "LATCH_STATEFUL", "buy": ["Af"], "sell": []}],
            settings={
                "trend_filter_operator": "OR",
                "trend_filter_gm_current": "GM_POS",
                "trend_filter_gm_market": "GM_NEG",
                "trend_filter_gm_sector": "GM_NEU",
            },
        )
        form = GameScenarioForm(
            data={
                "name": obj.name,
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.settings["trend_filter_operator"], "OR")
        self.assertEqual(saved.settings["trend_filter_gm_current"], "GM_POS")
        self.assertEqual(saved.settings["trend_filter_gm_market"], "GM_NEG")
        self.assertEqual(saved.settings["trend_filter_gm_sector"], "GM_NEU")

    def test_game_scenario_form_normalizes_legacy_gm_values_on_save(self):
        obj = GameScenario.objects.create(
            name="Game preserve legacy gm",
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["Af"],
                "sell": ["Bf"],
                "buy_gm_filter": "GM_POS",
                "buy_gm_operator": "OR",
                "sell_gm_filter": "GM_NEG",
                "sell_gm_operator": "AND",
            }],
            settings={},
        )
        form = GameScenarioForm(
            data={
                "name": obj.name,
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
                "signal_lines": json.dumps([{
                    "trading_model": "LEGACY_DAILY",
                    "buy": ["Af"],
                    "sell": ["Bf"],
                    "buy_gm_filter": "GM_POS",
                    "buy_gm_operator": "OR",
                    "sell_gm_filter": "GM_NEG",
                    "sell_gm_operator": "AND",
                }]),
                "warmup_days": "30",
                "close_positions_at_end": "on",
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertNotIn("trend_filter_gm_current", saved.settings)
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")
        self.assertEqual(saved.signal_lines[0]["buy_gm_operator"], "AND")
        self.assertEqual(saved.signal_lines[0]["buy_market_gm_current"], "GM_POS")
        self.assertEqual(saved.signal_lines[0]["sell_gm_filter"], "IGNORE")
        self.assertEqual(saved.signal_lines[0]["sell_gm_operator"], "AND")

    def test_game_scenario_form_trend_filter_gm_current_wins_over_legacy_buy_gm_filter(self):
        obj = GameScenario.objects.create(
            name="Game trend current wins",
            signal_lines=[{
                "trading_model": "LEGACY_DAILY",
                "buy": ["Af"],
                "sell": ["Bf"],
                "buy_gm_filter": "GM_POS",
            }],
            settings={"trend_filter_gm_current": "GM_NEG"},
        )
        form = GameScenarioForm(
            data={
                "name": obj.name,
                "description": "",
                "active": "on",
                "study_days": "1000",
                "tradability_threshold": "0",
                "npente": "100",
                "slope_threshold": "0.1",
                "npente_basse": "20",
                "slope_threshold_basse": "0.02",
                "rhd_ok_reactivation_mode": "classic",
                "rhd_ok_rebound_threshold": "0.08",
                "rhd_ok_confirmation_days": "2",
                "rhd_ok_reentry_max_drawdown": "0.40",
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
                "signal_lines": json.dumps([{
                    "trading_model": "LEGACY_DAILY",
                    "buy": ["Af"],
                    "sell": ["Bf"],
                    "buy_gm_filter": "GM_POS",
                }]),
                "warmup_days": "30",
                "close_positions_at_end": "on",
                "trend_filter_operator": "AND",
                "trend_filter_gm_current": "GM_NEG",
                "trend_filter_gm_market": "IGNORE",
                "trend_filter_gm_sector": "IGNORE",
            },
            instance=obj,
        )
        self.assertTrue(form.is_valid(), form.errors)
        saved = form.save()
        self.assertEqual(saved.settings["trend_filter_gm_current"], "GM_NEG")
        self.assertEqual(saved.signal_lines[0]["buy_gm_filter"], "IGNORE")

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
