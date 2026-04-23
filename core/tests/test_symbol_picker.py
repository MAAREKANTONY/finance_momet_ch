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
    def test_clean_signal_lines_keeps_gm_operators_and_defaults(self):
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
        self.assertEqual(line["trading_model"], "LATCH_STATEFUL")

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


class SignalLineTemplateDefaultsTests(SimpleTestCase):
    repo_root = Path(__file__).resolve().parents[2]

    def _template(self, name: str) -> str:
        return (self.repo_root / "templates" / name).read_text(encoding="utf-8")

    def test_new_default_backtest_lines_use_explicit_progressive_model(self):
        for template_name in ("backtest_create.html", "backtest_edit.html"):
            content = self._template(template_name)
            self.assertIn("trading_model:'LATCH_STATEFUL', buy:['Af']", content)
            self.assertIn("trading_model:'LATCH_STATEFUL', buy:[]", content)
            self.assertIn('<option value="">Automatique</option>', content)
            self.assertIn(
                '<option value="LATCH_STATEFUL">Progressif : les conditions peuvent se valider dans le temps</option>',
                content,
            )

    def test_new_default_game_lines_use_explicit_progressive_model(self):
        content = self._template("game_scenario_form.html")
        self.assertIn("trading_model:'LATCH_STATEFUL', buy:['Af']", content)
        self.assertIn("trading_model:'LATCH_STATEFUL', buy:[]", content)
        self.assertIn('<option value="">Automatique</option>', content)
        self.assertIn(
            '<option value="LATCH_STATEFUL">Progressif : les conditions peuvent se valider dans le temps</option>',
            content,
        )

    def test_price_range_fields_and_help_text_are_rendered_in_forms(self):
        for template_name in ("backtest_create.html", "backtest_edit.html"):
            content = self._template(template_name)
            self.assertIn("Prix minimum", content)
            self.assertIn("Prix maximum", content)
            self.assertIn("Une action ne pourra être achetée que si son prix du jour est compris dans cette plage.", content)
            self.assertIn("Ce filtre s'applique uniquement à l'achat.", content)
            self.assertIn("La vente reste toujours possible.", content)
        game_content = self._template("game_scenario_form.html")
        self.assertIn("Une action ne pourra être achetée que si son prix du jour est compris dans cette plage.", game_content)
        self.assertIn("Ce filtre s'applique uniquement à l'achat.", game_content)
        self.assertIn("La vente reste toujours possible.", game_content)

    def test_price_range_labels_are_rendered_in_detail_pages(self):
        for template_name in ("backtest_detail.html", "game_scenario_detail.html"):
            content = self._template(template_name)
            self.assertIn("Prix minimum d'achat", content)
            self.assertIn("Prix maximum d'achat", content)
            self.assertIn("aucune borne minimum", content)
            self.assertIn("aucune borne maximum", content)
