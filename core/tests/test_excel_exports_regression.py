from django.test import TestCase

from core.excel_utils import normalize_excel_cell
from core.services.backtesting.engine import _match_line_with_global_filter


class ExcelUtilsRegressionTests(TestCase):
    def test_normalize_excel_cell_flattens_list(self):
        self.assertEqual(normalize_excel_cell(["SPA", "SPVA"]), "SPA, SPVA")

    def test_normalize_excel_cell_serializes_dict(self):
        out = normalize_excel_cell({"buy": ["SPA", "SPVA"]})
        self.assertIn('"buy"', out)
        self.assertIn('SPA', out)


class BuySellLogicAuditTests(TestCase):
    def test_or_logic_matches_same_day_alert_only(self):
        self.assertTrue(_match_line_with_global_filter({"SPA"}, {"SPVB"}, ["SPA", "SPVA"], "OR", "GM_POS", "IGNORE", "AND"))

    def test_and_logic_uses_latched_memory(self):
        self.assertTrue(_match_line_with_global_filter({"SPA"}, {"SPVA"}, ["SPA", "SPVA"], "AND", "GM_POS", "IGNORE", "AND"))

    def test_gm_operator_and_requires_both(self):
        self.assertFalse(_match_line_with_global_filter({"SPA"}, set(), ["SPA"], "OR", "GM_NEG", "GM_POS", "AND"))

    def test_gm_operator_or_accepts_either(self):
        self.assertTrue(_match_line_with_global_filter(set(), set(), ["SPA"], "AND", "GM_POS", "GM_POS", "OR"))
