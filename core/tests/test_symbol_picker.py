from django import forms
from django.test import SimpleTestCase
from pathlib import Path

from core.widgets import SymbolPickerWidget
from core.forms import _clean_signal_lines_json


class SymbolPickerWidgetTests(SimpleTestCase):
    def test_value_from_datadict_parses_csv(self):
        widget = SymbolPickerWidget()
        value = widget.value_from_datadict({"symbols": "1, 2,3,, 4 "}, {}, "symbols")
        self.assertEqual(value, ["1", "2", "3", "4"])

    def test_value_from_datadict_empty_returns_empty_list(self):
        widget = SymbolPickerWidget()
        self.assertEqual(widget.value_from_datadict({"symbols": ""}, {}, "symbols"), [])
        self.assertEqual(widget.value_from_datadict({}, {}, "symbols"), [])


class SignalLinesCleaningTests(SimpleTestCase):
    def test_clean_signal_lines_keeps_gm_operators_and_uses_progressive_auto_default(self):
        cleaned = _clean_signal_lines_json([
            {
                "mode": "standard",
                "buy": ["Af", "SPa"],
                "sell": ["Bf"],
                "buy_logic": "and",
                "sell_logic": "or",
                "buy_gm_filter": "gm_pos",
                "buy_gm_operator": "or",
                "sell_gm_filter": "gm_neg_or_neu",
                "sell_gm_operator": "and",
            }
        ])
        self.assertEqual(len(cleaned), 1)
        line = cleaned[0]
        self.assertEqual(line["buy"], ["Af", "SPa"])
        self.assertEqual(line["sell"], ["Bf"])
        self.assertEqual(line["buy_logic"], "AND")
        self.assertEqual(line["sell_logic"], "OR")
        self.assertEqual(line["buy_gm_filter"], "GM_POS")
        self.assertEqual(line["buy_gm_operator"], "OR")
        self.assertEqual(line["sell_gm_filter"], "GM_NEG_OR_NEU")
        self.assertEqual(line["sell_gm_operator"], "AND")
        self.assertEqual(line["trading_model"], "PROGRESSIVE_AUTO_SELL")

    def test_clean_signal_lines_rejects_invalid_explicit_latch_config(self):
        with self.assertRaises(forms.ValidationError):
            _clean_signal_lines_json([
                {
                    "trading_model": "LATCH_STATEFUL",
                    "buy": ["A1"],
                    "sell": ["B1"],
                    "buy_logic": "AND",
                }
            ])

    def test_clean_signal_lines_keeps_gm_filter_separate_from_buy_signals(self):
        cleaned = _clean_signal_lines_json([
            {
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af", "SPVa_basse"],
                "sell": [],
                "buy_logic": "AND",
                "buy_gm_filter": "GM_POS",
                "buy_gm_operator": "AND",
            }
        ])
        self.assertEqual(len(cleaned), 1)
        line = cleaned[0]
        self.assertEqual(line["buy"], ["Af", "SPVa_basse"])
        self.assertEqual(line["buy_gm_filter"], "GM_POS")
        self.assertEqual(line["buy_gm_operator"], "AND")
        self.assertNotIn("GM_POS", line["buy"])

    def test_clean_signal_lines_stores_line_market_conditions_separately(self):
        cleaned = _clean_signal_lines_json([
            {
                "trading_model": "LATCH_STATEFUL",
                "buy": ["Af"],
                "sell": [],
                "buy_logic": "AND",
                "buy_market_gm_current": "GM_POS",
                "buy_market_gm_market": "GM_NEG",
                "buy_market_gm_sector": "IGNORE",
                "buy_market_operator": "OR",
            }
        ])
        self.assertEqual(len(cleaned), 1)
        line = cleaned[0]
        self.assertEqual(line["buy"], ["Af"])
        self.assertEqual(line["buy_market_gm_current"], "GM_POS")
        self.assertEqual(line["buy_market_gm_market"], "GM_NEG")
        self.assertEqual(line["buy_market_gm_sector"], "IGNORE")
        self.assertEqual(line["buy_market_operator"], "OR")
        self.assertNotIn("GM_POS", line["buy"])
        self.assertNotIn("GM_NEG", line["buy"])

    def test_clean_signal_lines_rejects_market_conditions_without_buy_signal(self):
        with self.assertRaises(forms.ValidationError):
            _clean_signal_lines_json([
                {
                    "trading_model": "LATCH_STATEFUL",
                    "buy": [],
                    "sell": [],
                    "buy_market_gm_market": "GM_POS",
                    "buy_market_operator": "AND",
                }
            ])

    def test_clean_signal_lines_rejects_gm_as_explicit_progressive_buy_signal(self):
        with self.assertRaises(forms.ValidationError):
            _clean_signal_lines_json([
                {
                    "trading_model": "LATCH_STATEFUL",
                    "buy": ["GM_POS"],
                    "sell": [],
                    "buy_logic": "AND",
                    "buy_gm_filter": "GM_POS",
                }
            ])

    def test_clean_signal_lines_accepts_rhd_as_standalone_ticker_signals(self):
        cleaned = _clean_signal_lines_json([
            {
                "trading_model": "LATCH_STATEFUL",
                "buy": ["RHD_OK"],
                "sell": ["RHD_FAIL"],
            }
        ])
        self.assertEqual(len(cleaned), 1)
        line = cleaned[0]
        self.assertEqual(line["buy"], ["RHD_OK"])
        self.assertEqual(line["sell"], ["RHD_FAIL"])
        self.assertEqual(line["trading_model"], "LATCH_STATEFUL")


class SignalLineTemplateDefaultsTests(SimpleTestCase):
    repo_root = Path(__file__).resolve().parents[2]

    def _template(self, name: str) -> str:
        return (self.repo_root / "templates" / name).read_text(encoding="utf-8")

    def test_new_default_backtest_lines_use_progressive_auto_sell_model(self):
        for template_name in ("backtest_create.html", "backtest_edit.html"):
            content = self._template(template_name)
            self.assertIn("trading_model:'PROGRESSIVE_AUTO_SELL', buy:['Af']", content)
            self.assertIn("trading_model:'PROGRESSIVE_AUTO_SELL', buy:[]", content)
            self.assertIn(
                "Les conditions de marché ne déclenchent pas un achat à elles seules. "
                "Elles autorisent l'achat lorsqu'un signal BUY est toujours actif.",
                content,
            )
            self.assertNotIn('<option value="">Automatique</option>', content)
            self.assertIn(
                '<option value="PROGRESSIVE_AUTO_SELL">Progressif avec vente automatique</option>',
                content,
            )
            self.assertIn(
                '<option value="PROGRESSIVE_EXPLICIT_SELL">Progressif avec vente explicite</option>',
                content,
            )

    def test_new_default_game_lines_use_progressive_auto_sell_model(self):
        content = self._template("game_scenario_form.html")
        self.assertIn("trading_model:'PROGRESSIVE_AUTO_SELL', buy:['Af']", content)
        self.assertIn("trading_model:'PROGRESSIVE_AUTO_SELL', buy:[]", content)
        self.assertIn(
            "Les conditions de marché ne déclenchent pas un achat à elles seules. "
            "Elles autorisent l'achat lorsqu'un signal BUY est toujours actif.",
            content,
        )
        self.assertNotIn('<option value="">Automatique</option>', content)
        self.assertIn(
            '<option value="PROGRESSIVE_AUTO_SELL">Progressif avec vente automatique</option>',
            content,
        )
        self.assertIn(
            '<option value="PROGRESSIVE_EXPLICIT_SELL">Progressif avec vente explicite</option>',
            content,
        )

    def test_price_range_fields_and_help_text_are_rendered_in_forms(self):
        for template_name in ("backtest_create.html", "backtest_edit.html"):
            content = self._template(template_name)
            self.assertIn("Prix minimum", content)
            self.assertIn("Prix maximum", content)
            self.assertIn("Un BUY est autorisé uniquement si le prix du jour reste dans cette plage.", content)
            self.assertIn("Ce contrôle de risque s’applique uniquement au BUY. Le SELL reste possible.", content)
        game_content = self._template("game_scenario_form.html")
        self.assertIn("Un BUY est autorisé uniquement si le prix du jour reste dans cette plage.", game_content)
        self.assertIn("Ce contrôle de risque s’applique uniquement au BUY. Le SELL reste possible.", game_content)

    def test_price_range_labels_are_rendered_in_detail_pages(self):
        for template_name in ("backtest_detail.html", "game_scenario_detail.html"):
            content = self._template(template_name)
            self.assertIn("Prix minimum d'achat", content)
            self.assertIn("Prix maximum d'achat", content)
            self.assertIn("aucune borne minimum", content)
            self.assertIn("aucune borne maximum", content)

    def test_anti_drop_copy_is_rendered_in_forms_and_help_page(self):
        scenario_content = self._template("scenario_form.html")
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", scenario_content)
        self.assertIn("RHD est un signal ticker calculé", scenario_content)
        game_content = self._template("game_scenario_form.html")
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", game_content)
        help_content = self._template("help_indicators.html")
        self.assertIn("Signal anti-chute RHD — Repli depuis haut récent", help_content)
        self.assertIn("Le jour courant est exclu du calcul", help_content)
        self.assertIn("RHD_OK et RHD_FAIL sont de vrais signaux ticker calculés", help_content)
        self.assertIn("RHD_OK / RHD_FAIL", help_content)

    def test_edit_templates_do_not_serialize_signal_lines_on_initial_load(self):
        for template_name in ("backtest_edit.html", "game_scenario_form.html"):
            content = self._template(template_name)
            self.assertNotIn("renumber();\n  serialize();", content)
