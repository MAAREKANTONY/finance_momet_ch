from django.test import SimpleTestCase

from core.views import _arrow_table_to_csv_safe


class ExportRegressionTests(SimpleTestCase):
    def test_arrow_csv_safe_helper_is_plain_function(self):
        class DummyTable:
            column_names = []

        table = DummyTable()
        result = _arrow_table_to_csv_safe(table)
        self.assertIs(result, table)
