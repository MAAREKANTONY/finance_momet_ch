import json
from pathlib import Path

from django.contrib.auth import get_user_model
from django.test import TestCase

from core.forms import BacktestForm, ScenarioForm, StudyBacktestForm, StudyScenarioForm, UniverseForm, GameScenarioForm, _clean_signal_lines_json
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
        labels = dict(form.fields["universe_mode"].choices)
        self.assertIn(Scenario.UniverseMode.STATIC_TICKERS, choices)
        self.assertIn(Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC, choices)
        self.assertIn(Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC, choices)
        self.assertEqual(labels[Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC], "CSI 300 historique dynamique — via CSV")
        self.assertTrue(form.is_valid(), form.errors)
        scenario = form.save()
        self.assertEqual(scenario.universe_mode, Scenario.UniverseMode.SP500_HISTORICAL_DYNAMIC)
        self.assertEqual(set(scenario.symbols.values_list("ticker", flat=True)), {"AAPL", "MSFT"})

    def test_scenario_form_saves_csi300_historical_dynamic_mode(self):
        form = ScenarioForm(data={
            "name": "Scenario CSI300 dynamic universe",
            "description": "test",
            "is_default": "",
            "universe_mode": Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
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

        self.assertTrue(form.is_valid(), form.errors)
        scenario = form.save()
        self.assertEqual(scenario.universe_mode, Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC)

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

    def test_couloir_signal_lines_are_exclusive_v1(self):
        cleaned = _clean_signal_lines_json([
            {"buy": ["Af"], "sell": ["Bf"]},
            {
                "buy": ["COULOIR"],
                "sell": ["Bf"],
                "couloir_initial_low_lookback_days": 120,
                "couloir_buy_rebound_threshold": "0.12",
                "couloir_sell_drawdown_threshold": "0.08",
                "couloir_buy_confirmation_days": 3,
                "couloir_sell_confirmation_days": 2,
                "gm_sell_market_exit_conditions": {"operator": "AND", "market": {"mode": "GM_NEG", "threshold": "0.2"}},
            },
            {"buy": ["A1"], "sell": ["B1"]},
        ])
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["mode"], "couloir")
        self.assertEqual(cleaned[0]["buy"], ["COULOIR"])
        self.assertEqual(cleaned[0]["sell"], [])
        self.assertEqual(cleaned[0]["couloir_initial_low_lookback_days"], 120)
        self.assertEqual(cleaned[0]["couloir_buy_rebound_threshold"], "0.12")
        self.assertEqual(cleaned[0]["couloir_sell_drawdown_threshold"], "0.08")
        self.assertEqual(cleaned[0]["couloir_buy_confirmation_days"], 3)
        self.assertEqual(cleaned[0]["couloir_sell_confirmation_days"], 2)
        self.assertEqual(cleaned[0]["gm_sell_market_exit_conditions"]["market"]["mode"], "NEG")

    def test_couloir_signal_lines_keep_all_gm_and_gm_push_configs(self):
        cleaned = _clean_signal_lines_json([
            {
                "buy": ["COULOIR"],
                "sell": ["Bf"],
                "gm_buy_conditions": {
                    "operator": "OR",
                    "current": {"mode": "GM_POS", "threshold": "0.2", "buy_max_threshold": "0.4"},
                },
                "gm_sell_market_exit_conditions": {
                    "operator": "AND",
                    "market": {"mode": "GM_NEG", "threshold": "0.3"},
                },
                "gm_push_buy_conditions": {
                    "operator": "AND",
                    "current": {"mode": "GM_POS", "threshold": "0.2", "buy_max_threshold": "0.4"},
                },
                "gm_push_sell_market_exit_conditions": {
                    "operator": "OR",
                    "market": {"mode": "GM_NEG", "threshold": "0.2"},
                },
            }
        ])
        line = cleaned[0]
        self.assertEqual(line["buy"], ["COULOIR"])
        self.assertEqual(line["sell"], [])
        self.assertEqual(line["gm_buy_conditions"]["operator"], "OR")
        self.assertEqual(line["gm_buy_conditions"]["current"]["mode"], "POS")
        self.assertEqual(line["gm_buy_conditions"]["current"]["threshold"], "0.2")
        self.assertEqual(line["gm_sell_market_exit_conditions"]["market"]["mode"], "NEG")
        self.assertEqual(line["gm_push_buy_conditions"]["current"]["threshold"], "0.2")
        self.assertEqual(line["gm_push_buy_conditions"]["current"]["buy_max_threshold"], "0.4")
        self.assertEqual(line["gm_push_sell_market_exit_conditions"]["market"]["threshold"], "0.2")
        self.assertEqual(line["gm_push_sell_market_exit_conditions"]["market"]["sell_threshold"], "0.2")

    def test_game_scenario_form_keeps_couloir_gm_and_gm_push_configs(self):
        form = GameScenarioForm(
            data={
                "name": "Game Couloir GM",
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
                    {
                        "buy": ["COULOIR"],
                        "sell": [],
                        "gm_buy_conditions": {"current": {"mode": "GM_POS", "threshold": "0.2"}},
                        "gm_sell_market_exit_conditions": {"market": {"mode": "GM_NEG", "threshold": "0.3"}},
                        "gm_push_buy_conditions": {"current": {"mode": "GM_POS", "threshold": "0.2"}},
                        "gm_push_sell_market_exit_conditions": {"market": {"mode": "GM_NEG", "threshold": "0.2"}},
                    }
                ]),
                "warmup_days": "30",
                "close_positions_at_end": "on",
            }
        )
        self.assertTrue(form.is_valid(), form.errors)
        line = form.cleaned_data["signal_lines"][0]
        self.assertEqual(line["buy"], ["COULOIR"])
        self.assertEqual(line["gm_buy_conditions"]["current"]["threshold"], "0.2")
        self.assertEqual(line["gm_sell_market_exit_conditions"]["market"]["threshold"], "0.3")
        self.assertEqual(line["gm_push_buy_conditions"]["current"]["threshold"], "0.2")
        self.assertEqual(line["gm_push_sell_market_exit_conditions"]["market"]["sell_threshold"], "0.2")

    def test_couloir_templates_expose_global_gm_filters_outside_classic_panel(self):
        for relative_path in (
            "templates/backtest_create.html",
            "templates/backtest_edit.html",
            "templates/game_scenario_form.html",
        ):
            source = Path(relative_path).read_text()
            self.assertIn('id="couloir-global-filters-panel"', source)
            self.assertIn("Filtres GM / GM Push compatibles Couloir", source)
            self.assertIn("couloir_buy_market_gm_current", source)
            self.assertIn("couloir_gm_push_buy_current", source)
            self.assertIn("copyClassicGlobalFiltersToCouloirIfEmpty", source)
            self.assertLess(source.index('id="couloir-global-filters-panel"'), source.index('id="classic-signal-lines-panel"'))
            self.assertIn("threshold: threshold || null", source)
            self.assertNotIn("sell_threshold: threshold ? String(-Math.abs(Number(threshold)))", source)

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

    def _backtest_form_payload(self, scenario, **overrides):
        data = {
            "name": "BT capital",
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
        }
        data.update(overrides)
        return data

    def test_backtest_form_allows_unlimited_total_capital_with_positive_per_ticker_capital(self):
        scenario = Scenario.objects.create(name="Capital unlimited", active=True)
        form = BacktestForm(data=self._backtest_form_payload(scenario, capital_total="0", capital_per_ticker="100"))

        self.assertTrue(form.is_valid(), form.errors)

    def test_backtest_form_persists_cny_for_dynamic_csi300(self):
        scenario = Scenario.objects.create(
            name="CSI300 native CNY",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )
        form = BacktestForm(data=self._backtest_form_payload(scenario))

        self.assertTrue(form.is_valid(), form.errors)
        backtest = form.save()

        self.assertEqual(backtest.settings["effective_currency"], "CNY")

    def test_backtest_form_rejects_csi300_start_before_supported_history(self):
        scenario = Scenario.objects.create(
            name="CSI300 unsupported history",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )
        form = BacktestForm(data=self._backtest_form_payload(scenario, start_date="2023-01-02"))

        self.assertFalse(form.is_valid())
        self.assertIn("start_date", form.errors)
        self.assertIn("3 janvier 2023", str(form.errors["start_date"]))

    def test_backtest_form_accepts_csi300_start_at_supported_history(self):
        scenario = Scenario.objects.create(
            name="CSI300 supported history boundary",
            universe_mode=Scenario.UniverseMode.CSI300_HISTORICAL_DYNAMIC,
            active=True,
        )
        form = BacktestForm(data=self._backtest_form_payload(
            scenario,
            start_date="2023-01-03",
            end_date="2023-01-04",
        ))

        self.assertTrue(form.is_valid(), form.errors)

    def test_backtest_form_does_not_invent_currency_for_static_universe(self):
        scenario = Scenario.objects.create(
            name="Static currency neutral",
            universe_mode=Scenario.UniverseMode.STATIC_TICKERS,
            active=True,
        )
        form = BacktestForm(data=self._backtest_form_payload(scenario))

        self.assertTrue(form.is_valid(), form.errors)
        backtest = form.save()

        self.assertNotIn("effective_currency", backtest.settings)

    def test_backtest_form_rejects_zero_capital_per_ticker(self):
        scenario = Scenario.objects.create(name="Capital CT zero", active=True)
        form = BacktestForm(data=self._backtest_form_payload(scenario, capital_total="0", capital_per_ticker="0"))

        self.assertFalse(form.is_valid())
        self.assertIn("capital_per_ticker", form.errors)
        self.assertIn("Le capital par action doit être supérieur à zéro.", form.errors["capital_per_ticker"])

    def test_backtest_form_rejects_total_capital_below_capital_per_ticker(self):
        scenario = Scenario.objects.create(name="Capital CP too low", active=True)
        form = BacktestForm(data=self._backtest_form_payload(scenario, capital_total="50", capital_per_ticker="100"))

        self.assertFalse(form.is_valid())
        self.assertIn("capital_total", form.errors)
        self.assertIn("Le capital total doit être supérieur ou égal au capital par action, ou égal à zéro pour un capital global illimité.", form.errors["capital_total"])

    def test_study_backtest_form_rejects_zero_capital_per_ticker(self):
        scenario = Scenario.objects.create(name="Study Capital", active=True)
        bt = Backtest.objects.create(name="Study BT", scenario=scenario, capital_total="0", capital_per_ticker="100")
        form = StudyBacktestForm(
            data={
                "name": "Study BT",
                "description": "",
                "start_date": "2024-01-01",
                "end_date": "2024-01-03",
                "capital_total": "0",
                "capital_per_ticker": "0",
                "ratio_threshold": "0",
                "include_all_tickers": "on",
                "signal_lines": json.dumps([{"buy": ["A1"], "sell": ["B1"]}]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
            },
            instance=bt,
        )

        self.assertFalse(form.is_valid())
        self.assertIn("capital_per_ticker", form.errors)

    def test_game_scenario_form_rejects_total_capital_below_capital_per_ticker(self):
        form = GameScenarioForm(
            data={
                "name": "Game capital",
                "description": "",
                "active": "on",
                "study_days": "30",
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
                "capital_total": "50",
                "capital_per_ticker": "100",
                "capital_mode": "FIXED",
                "signal_lines": json.dumps([{ "buy": ["A1"], "sell": ["B1"] }]),
                "warmup_days": "0",
                "close_positions_at_end": "on",
            }
        )

        self.assertFalse(form.is_valid())
        self.assertIn("capital_total", form.errors)

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
                "capital_total": "0",
                "capital_per_ticker": "100",
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
                "capital_total": "0",
                "capital_per_ticker": "100",
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


class RunConfigurationSnapshotTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(username="snapshot-user", password="secret123")
        self.client.force_login(self.user)
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", name="AAA Corp", sector="Tech", active=True)
        self.scenario = Scenario.objects.create(
            name="Snapshot scenario",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            npente=100,
            slope_threshold="0.1",
            npente_basse=20,
            slope_threshold_basse="0.02",
            rhd_ok_reactivation_mode="rebound_confirmed",
            rhd_ok_rebound_threshold="0.08",
            rhd_ok_confirmation_days=2,
            rhd_ok_reentry_max_drawdown="0.40",
            nglobal=20,
            history_years=2,
        )
        self.scenario.symbols.set([self.symbol])

    def _backtest(self, **overrides):
        data = {
            "name": "Snapshot BT",
            "scenario": self.scenario,
            "capital_total": 10000,
            "capital_per_ticker": 1000,
            "ratio_threshold": 0,
            "include_all_tickers": True,
            "signal_lines": [{"buy": ["A1"], "sell": ["B1"], "gm_push_buy_conditions": {"current": {"mode": "NEG", "threshold": "0.2"}}}],
            "universe_snapshot": [{"ticker": "AAA", "exchange": "NYSE", "sector": "Tech"}],
            "results": {"tickers": {"AAA": {"daily": [1, 2, 3]}}},
        }
        data.update(overrides)
        return Backtest.objects.create(**data)

    def test_snapshot_hash_is_stable_and_changes_when_config_changes(self):
        from core.services.run_configuration_snapshots import build_backtest_snapshot_payload, compute_config_hash

        bt = self._backtest()
        scenario_snapshot, run_snapshot = build_backtest_snapshot_payload(bt)
        first = compute_config_hash("BACKTEST", scenario_snapshot, run_snapshot)
        second = compute_config_hash("BACKTEST", scenario_snapshot, run_snapshot)
        self.assertEqual(first, second)
        changed = dict(run_snapshot)
        changed["capital_total"] = "999"
        self.assertNotEqual(first, compute_config_hash("BACKTEST", scenario_snapshot, changed))
        self.assertNotIn("results", run_snapshot)

    def test_capture_backtest_deduplicates_and_preserves_gm_push_threshold(self):
        from core.services.run_configuration_snapshots import capture_backtest_configuration
        from core.models import RunConfigurationSnapshot

        bt = self._backtest()
        first = capture_backtest_configuration(bt)
        second = capture_backtest_configuration(bt)
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(RunConfigurationSnapshot.objects.filter(kind="BACKTEST").count(), 1)
        line = first.run_snapshot["signal_lines"][0]
        self.assertEqual(line["gm_push_buy_conditions"]["current"]["threshold"], "0.2")

    def test_capture_game_deduplicates_and_keeps_rhd_fields(self):
        from core.services.run_configuration_snapshots import capture_game_configuration
        from core.models import RunConfigurationSnapshot

        game = GameScenario.objects.create(
            name="Snapshot Game",
            study_days=120,
            active=True,
            rhd_ok_reactivation_mode="rebound_confirmed",
            rhd_ok_rebound_threshold="0.09",
            rhd_ok_confirmation_days=3,
            rhd_ok_reentry_max_drawdown="0.35",
            signal_lines=[{"buy": ["RHD_OK"], "sell": ["RHD_FAIL"]}],
        )
        first = capture_game_configuration(game)
        second = capture_game_configuration(game)
        self.assertIsNotNone(first)
        self.assertIsNone(second)
        self.assertEqual(RunConfigurationSnapshot.objects.filter(kind="GAME").count(), 1)
        self.assertEqual(first.scenario_snapshot["rhd_ok_reactivation_mode"], "rebound_confirmed")
        self.assertEqual(first.scenario_snapshot["rhd_ok_confirmation_days"], 3)

    def test_purge_keeps_50_latest_per_kind(self):
        from core.services.run_configuration_snapshots import capture_backtest_configuration
        from core.models import RunConfigurationSnapshot

        for idx in range(55):
            bt = self._backtest(name=f"BT {idx}", capital_total=idx + 1)
            capture_backtest_configuration(bt)
        self.assertEqual(RunConfigurationSnapshot.objects.filter(kind="BACKTEST").count(), 50)

    def test_restore_backtest_creates_scenario_and_backtest_copy(self):
        from core.services.run_configuration_snapshots import capture_backtest_configuration, restore_backtest_snapshot

        bt = self._backtest(signal_lines=[{"buy": ["COULOIR"], "sell": [], "couloir": {"buy_rebound_threshold": "0.10"}}])
        snapshot = capture_backtest_configuration(bt)
        restored = restore_backtest_snapshot(snapshot)
        self.assertNotEqual(restored.id, bt.id)
        self.assertNotEqual(restored.scenario_id, self.scenario.id)
        self.assertEqual(restored.signal_lines[0]["buy"], ["COULOIR"])
        self.assertEqual(restored.results, {})
        self.assertEqual(list(restored.scenario.symbols.values_list("ticker", flat=True)), ["AAA"])

    def test_restore_game_creates_copy(self):
        from core.services.run_configuration_snapshots import capture_game_configuration, restore_game_snapshot

        game = GameScenario.objects.create(
            name="Game Restore",
            study_days=90,
            signal_lines=[{"buy": ["A1"], "sell": ["B1"]}],
            today_results={"rows": [1]},
            last_run_status="done",
        )
        snapshot = capture_game_configuration(game)
        restored = restore_game_snapshot(snapshot)
        self.assertNotEqual(restored.id, game.id)
        self.assertEqual(restored.study_days, 90)
        self.assertEqual(restored.signal_lines[0]["buy"], ["A1"])
        self.assertEqual(restored.today_results, {})
        self.assertEqual(restored.last_run_status, "")

    def test_snapshot_ui_blocks_are_rendered_and_restore_redirects(self):
        from core.services.run_configuration_snapshots import capture_backtest_configuration, capture_game_configuration

        bt = self._backtest()
        bt_snapshot = capture_backtest_configuration(bt)
        response = self.client.get(f"/backtests/new/?snapshot_id={bt_snapshot.id}")
        self.assertContains(response, "Configurations sauvegardées")
        self.assertContains(response, "Charger cette configuration")
        response = self.client.post("/backtests/snapshots/restore/", {"snapshot_id": bt_snapshot.id})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(Backtest.objects.count(), 2)

        game = GameScenario.objects.create(name="UI Game", study_days=30, signal_lines=[{"buy": ["A1"], "sell": ["B1"]}])
        game_snapshot = capture_game_configuration(game)
        response = self.client.get(f"/games/new/?snapshot_id={game_snapshot.id}")
        self.assertContains(response, "Configurations sauvegardées")
        response = self.client.post("/games/snapshots/restore/", {"snapshot_id": game_snapshot.id})
        self.assertEqual(response.status_code, 302)
        self.assertEqual(GameScenario.objects.count(), 2)
