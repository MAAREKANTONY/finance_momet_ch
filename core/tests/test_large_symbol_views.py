from django.contrib.auth import get_user_model
from django.test import Client, TestCase
from django.urls import reverse

from core.models import Scenario, Study, Symbol, Universe


class LargeSymbolFormViewTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.user = get_user_model().objects.create_user(username="tester", password="secret123")
        cls.symbols = [
            Symbol(
                ticker=f"SYM{i:04d}",
                exchange="NASDAQ" if i % 2 else "NYSE",
                name=f"Company {i}",
                sector="Technology" if i % 3 == 0 else "Finance",
                country="US",
                active=True,
            )
            for i in range(1, 181)
        ]
        Symbol.objects.bulk_create(cls.symbols)
        cls.symbol_ids_csv = ",".join(str(pk) for pk in Symbol.objects.order_by("id").values_list("id", flat=True))

    def setUp(self):
        self.client = Client()
        self.client.force_login(self.user)

    def _scenario_payload(self, **overrides):
        data = {
            "name": "Scenario mass tickers",
            "description": "bulk",
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
            "nglobal": "20",
            "history_years": "2",
            "active": "on",
            "symbols": self.symbol_ids_csv,
        }
        data.update(overrides)
        return data

    def test_symbol_search_returns_sector_and_respects_exclude(self):
        first_two = list(Symbol.objects.order_by("id").values_list("id", flat=True)[:2])
        response = self.client.get(reverse("symbol_search"), {"q": "SYM", "exclude": ",".join(map(str, first_two)), "limit": 10})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertTrue(payload)
        self.assertNotIn(first_two[0], [row["id"] for row in payload])
        self.assertIn("sector", payload[0])
        self.assertIn("country", payload[0])

    def test_universe_create_accepts_large_symbol_csv(self):
        response = self.client.post(reverse("universe_create"), {
            "name": "All US",
            "description": "Large selection",
            "active": "on",
            "symbols": self.symbol_ids_csv,
        })
        self.assertEqual(response.status_code, 302)
        universe = Universe.objects.get(name="All US")
        self.assertEqual(universe.symbols.count(), Symbol.objects.count())

    def test_scenario_create_accepts_large_symbol_csv(self):
        response = self.client.post(reverse("scenario_create"), self._scenario_payload())
        self.assertEqual(response.status_code, 302)
        scenario = Scenario.objects.get(name="Scenario mass tickers")
        self.assertEqual(scenario.symbols.count(), Symbol.objects.count())

    def test_universe_symbols_json_returns_metadata(self):
        universe = Universe.objects.create(name="Selection", active=True)
        selected = list(Symbol.objects.order_by("ticker")[:5])
        universe.symbols.set(selected)

        response = self.client.get(reverse("universe_symbols_json", args=[universe.pk]))
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["ids"]), 5)
        self.assertEqual(len(payload["symbols"]), 5)
        self.assertEqual(sorted(payload["ids"]), sorted([s.id for s in selected]))
        self.assertIn("sector", payload["symbols"][0])
        self.assertIn("exchange", payload["symbols"][0])

    def test_study_edit_accepts_large_symbol_csv(self):
        scenario = Scenario.objects.create(
            name="Study source",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            npente=100,
            slope_threshold=0.1,
            npente_basse=20,
            slope_threshold_basse=0.02,
            nglobal=20,
            history_years=2,
        )
        study = Study.objects.create(name="Study Alpha", scenario=scenario, created_by=self.user)

        response = self.client.post(reverse("study_edit", args=[study.pk]), {
            "study-name": "Study Alpha",
            "study-description": "updated",
            "sc-a": "1",
            "sc-b": "1",
            "sc-c": "1",
            "sc-d": "1",
            "sc-e": "1",
            "sc-n1": "5",
            "sc-n2": "3",
            "sc-npente": "100",
            "sc-slope_threshold": "0.1",
            "sc-npente_basse": "20",
            "sc-slope_threshold_basse": "0.02",
            "sc-nglobal": "20",
            "sc-history_years": "2",
            "sc-symbols": self.symbol_ids_csv,
        })
        self.assertEqual(response.status_code, 302)
        study.refresh_from_db()
        self.assertEqual(study.scenario.symbols.count(), Symbol.objects.count())


    def test_symbol_filter_preview_returns_total_and_preview(self):
        response = self.client.get(reverse("symbol_filter_preview"), {"exchange": "NASDAQ", "sector": "Technology", "limit": 25})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        expected_total = Symbol.objects.filter(active=True, exchange="NASDAQ", sector="Technology").count()
        self.assertEqual(payload["total_count"], expected_total)
        self.assertLessEqual(payload["preview_count"], 25)
        self.assertEqual(len(payload["symbols"]), payload["preview_count"])

    def test_symbol_filter_preview_include_all_returns_full_population(self):
        response = self.client.get(reverse("symbol_filter_preview"), {"exchange": "NYSE", "include_all": "1"})
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        expected_total = Symbol.objects.filter(active=True, exchange="NYSE").count()
        self.assertEqual(payload["total_count"], expected_total)
        self.assertEqual(len(payload["symbols"]), expected_total)

    def test_universe_form_renders_new_bulk_selection_ui(self):
        response = self.client.get(reverse("universe_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Ajouter tous les résultats", body)
        self.assertIn("Recherche dans la sélection", body)

    def test_scenario_form_renders_bulk_selection_ui(self):
        Universe.objects.create(name="US Market", active=True)
        response = self.client.get(reverse("scenario_create"))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        self.assertIn("Ajouter tous les résultats", body)
        self.assertIn("Appliquer un univers existant", body)
        self.assertIn("Recherche dans la sélection", body)

    def test_scenario_duplicate_preloads_existing_symbols_in_hidden_picker_state(self):
        scenario = Scenario.objects.create(
            name="Base Scenario",
            active=True,
            a=1,
            b=1,
            c=1,
            d=1,
            e=1,
            n1=5,
            n2=3,
            npente=100,
            slope_threshold=0.1,
            npente_basse=20,
            slope_threshold_basse=0.02,
            nglobal=20,
            history_years=2,
        )
        selected = list(Symbol.objects.order_by("id")[:3])
        scenario.symbols.set(selected)
        response = self.client.get(reverse("scenario_duplicate", args=[scenario.pk]))
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        for sym in selected:
            self.assertIn(sym.ticker, body)
        self.assertIn('server-selected-bootstrap', body)


class SymbolCsvSubmissionRegressionTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.symbols = [Symbol.objects.create(ticker=f"TK{i}", exchange="NASDAQ", active=True) for i in range(1,4)]

    def test_universe_create_accepts_csv_symbols_once(self):
        resp = self.client.post("/universes/new/", {
            "name": "U CSV",
            "description": "",
            "active": "on",
            "symbols": ",".join(str(s.id) for s in self.symbols),
        }, follow=True)
        self.assertNotContains(resp, "n’est pas une valeur correcte", status_code=200)
        u = Universe.objects.get(name="U CSV")
        self.assertEqual(set(u.symbols.values_list("id", flat=True)), {s.id for s in self.symbols})

    def test_scenario_create_accepts_csv_symbols_once(self):
        payload = {
            "name": "S CSV",
            "description": "",
            "is_default": "",
            "a": 1, "b": 1, "c": 1, "d": 1, "e": "0.01",
            "n1": 20, "n2": 50, "npente": 100, "slope_threshold": "0",
            "npente_basse": 20, "slope_threshold_basse": "0",
            "nglobal": 20, "history_years": 10, "active": "on",
            "symbols": ",".join(str(s.id) for s in self.symbols),
        }
        resp = self.client.post("/scenarios/new/", payload, follow=True)
        self.assertNotContains(resp, "n’est pas une valeur correcte", status_code=200)
        scenario = Scenario.objects.get(name="S CSV")
        self.assertEqual(set(scenario.symbols.values_list("id", flat=True)), {s.id for s in self.symbols})
