from django.test import SimpleTestCase

from core.views import _arrow_table_to_csv_safe


class ExportRegressionTests(SimpleTestCase):
    def test_arrow_csv_safe_helper_is_plain_function(self):
        class DummyTable:
            column_names = []

        table = DummyTable()
        result = _arrow_table_to_csv_safe(table)
        self.assertIs(result, table)


from openpyxl import load_workbook
from tempfile import NamedTemporaryFile
from types import SimpleNamespace

from core.backtest_debug import build_backtest_debug_workbook
from core.views import _build_backtest_workbook_full


class ExcelSerializationRegressionTests(SimpleTestCase):
    def _make_backtest_stub(self):
        scenario = SimpleNamespace(name="Scenario X", description="", a=1, b=1, c=1, d=1, e=0, vc=None, fl=None, n1=1, n2=1, n3=1, n4=1, n5=1, k2j=None, cr=None, n5f3=None, crf3=None, npente=None, nglobal=None, slope_threshold=None, npente_basse=None, slope_threshold_basse=None)
        results = {
            "tickers": {
                "AAA": {
                    "lines": [
                        {
                            "line_index": 1,
                            "buy": ["SPA", "SPVA"],
                            "sell": ["SVA"],
                            "allocated": True,
                            "final": {"N": 2, "S_G_N": 0.1, "BT": 0.2, "NB_JOUR_OUVRES": 3, "BMJ": 0.01, "BMD": 0.02, "BUY_DAYS_CLOSED": 1, "cash_ticker_end": 123.4},
                            "daily": [],
                        }
                    ]
                }
            },
            "portfolio": {"kpi": {}, "daily": []},
            "meta": {},
        }
        return SimpleNamespace(id=1, name="BT", description="", scenario=scenario, scenario_id=1, start_date=None, end_date=None, capital_total=0, capital_per_ticker=0, ratio_threshold=0, close_positions_at_end=False, status="DONE", universe_snapshot=[], results=results, capital_mode="fixed", include_all_tickers=False, warmup_days=0)

    def test_build_backtest_workbook_full_serializes_list_cells(self):
        bt = self._make_backtest_stub()
        wb, _ = _build_backtest_workbook_full(bt)
        with NamedTemporaryFile(suffix=".xlsx") as tmp:
            wb.save(tmp.name)
            loaded = load_workbook(tmp.name, read_only=True)
            rows = list(loaded["Summary"].iter_rows(values_only=True))
        self.assertEqual(rows[1][2], '["SPA", "SPVA"]')
        self.assertEqual(rows[1][3], '["SVA"]')

    def test_backtest_debug_workbook_serializes_nested_daily_values(self):
        scenario = SimpleNamespace(name="Scenario X", description="")
        bt = SimpleNamespace(id=1, name="BT", scenario=scenario, start_date=None, end_date=None, capital_total=0, capital_per_ticker=0, capital_mode="fixed", ratio_threshold=0, include_all_tickers=False, warmup_days=0, close_positions_at_end=False, results={
            "tickers": {
                "AAA": {
                    "lines": [{
                        "line_index": 1,
                        "buy": ["SPA", "SPVA"],
                        "sell": ["SVA"],
                        "daily": [{"date": "2026-01-01", "action": ["BUY", "SELL"]}],
                        "final": {"alerts": ["A", "B"]},
                    }]
                }
            }
        })
        wb, _ = build_backtest_debug_workbook(bt, ticker="AAA", line=1)
        with NamedTemporaryFile(suffix=".xlsx") as tmp:
            wb.save(tmp.name)
            loaded = load_workbook(tmp.name, read_only=True)
            rows = list(loaded["DATA"].iter_rows(values_only=True))
        self.assertIn('["BUY", "SELL"]', rows[1])
