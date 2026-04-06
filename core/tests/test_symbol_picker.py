from django.test import SimpleTestCase

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
