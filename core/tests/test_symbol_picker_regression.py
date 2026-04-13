from django.test import SimpleTestCase, TestCase
from django.urls import reverse

from core.models import Symbol, Universe
from core.widgets import SymbolPickerWidget


class SymbolPickerWidgetTests(SimpleTestCase):
    def test_value_from_csv_hidden_input(self):
        widget = SymbolPickerWidget()
        class Dummy(dict):
            def getlist(self, key):
                return [self.get(key)] if self.get(key) is not None else []
        data = Dummy(symbols='1,2,3')
        self.assertEqual(widget.value_from_datadict(data, {}, 'symbols'), ['1', '2', '3'])

    def test_value_from_repeated_values_fallback(self):
        widget = SymbolPickerWidget()
        class Dummy(dict):
            def getlist(self, key):
                return ['1', '2', '3']
        data = Dummy()
        self.assertEqual(widget.value_from_datadict(data, {}, 'symbols'), ['1', '2', '3'])


class SymbolSearchRegressionTests(TestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        self.user = User.objects.create_user(username='u', password='p')
        self.client.force_login(self.user)
        self.aapl = Symbol.objects.create(ticker='AAPL', name='Apple', exchange='NASDAQ', active=True)
        self.msft = Symbol.objects.create(ticker='MSFT', name='Microsoft', exchange='NASDAQ', active=True)
        self.nvda = Symbol.objects.create(ticker='NVDA', name='NVIDIA', exchange='NASDAQ', active=True)

    def test_multi_token_search_returns_exact_matches(self):
        resp = self.client.get(reverse('symbol_search'), {'q': 'AAPL,MSFT,NVDA', 'limit': 500})
        self.assertEqual(resp.status_code, 200)
        tickers = {item['ticker'] for item in resp.json()}
        self.assertTrue({'AAPL', 'MSFT', 'NVDA'}.issubset(tickers))

    def test_universe_symbols_json_still_returns_symbols(self):
        u = Universe.objects.create(name='U1')
        u.symbols.set([self.aapl, self.msft])
        resp = self.client.get(reverse('universe_symbols_json', args=[u.pk]))
        self.assertEqual(resp.status_code, 200)
        body = resp.json()
        self.assertEqual(set(body['ids']), {self.aapl.id, self.msft.id})
        self.assertEqual({s['ticker'] for s in body['symbols']}, {'AAPL', 'MSFT'})


class MixedLegacyAndCsvSubmissionRegressionTests(TestCase):
    def setUp(self):
        from django.contrib.auth import get_user_model
        User = get_user_model()
        self.user = User.objects.create_user(username='mix', password='p')
        self.client.force_login(self.user)
        self.symbols = [
            Symbol.objects.create(ticker=f'MX{i}', name=f'Mix {i}', exchange='NASDAQ', active=True)
            for i in range(1, 4)
        ]

    def test_widget_normalizes_mixed_csv_and_repeated_values(self):
        widget = SymbolPickerWidget()

        class Dummy(dict):
            def getlist(self, key):
                return ['1,2,3', '1', '2', '3']

        self.assertEqual(widget.value_from_datadict(Dummy(), {}, 'symbols'), ['1', '2', '3'])

    def test_universe_edit_accepts_mixed_csv_and_repeated_values(self):
        universe = Universe.objects.create(name='Existing U', active=True)
        universe.symbols.set(self.symbols[:2])
        payload = {
            'name': 'Existing U',
            'description': '',
            'active': 'on',
            'symbols': [','.join(str(s.id) for s in self.symbols), *(str(s.id) for s in self.symbols[:2])],
        }
        resp = self.client.post(reverse('universe_edit', args=[universe.pk]), payload, follow=True)
        self.assertEqual(resp.status_code, 200)
        self.assertNotContains(resp, 'n’est pas une valeur correcte', status_code=200)
        universe.refresh_from_db()
        self.assertEqual(set(universe.symbols.values_list('id', flat=True)), {s.id for s in self.symbols})

    def test_scenario_create_accepts_mixed_csv_and_repeated_values(self):
        payload = {
            'name': 'Mixed Scenario',
            'description': '',
            'is_default': '',
            'a': 1, 'b': 1, 'c': 1, 'd': 1, 'e': '0.01',
            'n1': 20, 'n2': 50, 'npente': 100, 'slope_threshold': '0',
            'npente_basse': 20, 'slope_threshold_basse': '0',
            'nglobal': 20, 'history_years': 10, 'active': 'on',
            'symbols': [','.join(str(s.id) for s in self.symbols), str(self.symbols[0].id)],
        }
        resp = self.client.post(reverse('scenario_create'), payload, follow=True)
        self.assertEqual(resp.status_code, 200)
        self.assertNotContains(resp, 'n’est pas une valeur correcte', status_code=200)
