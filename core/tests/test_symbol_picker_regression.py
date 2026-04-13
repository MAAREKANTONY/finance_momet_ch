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
