from __future__ import annotations

from datetime import date
from decimal import Decimal

from django.db import IntegrityError
from django.test import TestCase

from core.models import HistoricalMarketCap, Symbol
from core.services.market_cap import get_market_cap_at_or_before, preload_market_cap_series


class HistoricalMarketCapLookupTests(TestCase):
    def setUp(self):
        self.symbol = Symbol.objects.create(ticker="AAA", exchange="NYSE", active=True)
        self.other_symbol = Symbol.objects.create(ticker="BBB", exchange="NASDAQ", active=True)

    def test_exact_date_market_cap_found(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 10),
            market_cap=Decimal("123456789.00"),
        )

        result = get_market_cap_at_or_before(self.symbol, date(2024, 1, 10))

        self.assertEqual(result, Decimal("123456789.00"))

    def test_latest_previous_market_cap_found(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 5),
            market_cap=Decimal("100.00"),
        )
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 9),
            market_cap=Decimal("200.00"),
        )

        result = get_market_cap_at_or_before(self.symbol, date(2024, 1, 10))

        self.assertEqual(result, Decimal("200.00"))

    def test_future_market_cap_is_not_used(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 15),
            market_cap=Decimal("300.00"),
        )

        result = get_market_cap_at_or_before(self.symbol, date(2024, 1, 10))

        self.assertIsNone(result)

    def test_no_previous_data_returns_none(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 15),
            market_cap=Decimal("300.00"),
        )

        result = get_market_cap_at_or_before(self.symbol, date(2024, 1, 14))

        self.assertIsNone(result)

    def test_provider_isolation_works(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 10),
            market_cap=Decimal("111.00"),
            provider="eodhd",
        )
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 10),
            market_cap=Decimal("222.00"),
            provider="otherfeed",
        )

        self.assertEqual(
            get_market_cap_at_or_before(self.symbol, date(2024, 1, 10), provider="eodhd"),
            Decimal("111.00"),
        )
        self.assertEqual(
            get_market_cap_at_or_before(self.symbol, date(2024, 1, 10), provider="otherfeed"),
            Decimal("222.00"),
        )

    def test_uniqueness_constraint_prevents_duplicate_provider_symbol_date(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 10),
            market_cap=Decimal("111.00"),
            provider="eodhd",
        )

        with self.assertRaises(IntegrityError):
            HistoricalMarketCap.objects.create(
                symbol=self.symbol,
                date=date(2024, 1, 10),
                market_cap=Decimal("222.00"),
                provider="eodhd",
            )

    def test_helper_returns_none_for_unknown_symbol_or_date(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 10),
            market_cap=Decimal("111.00"),
            provider="eodhd",
        )

        self.assertIsNone(get_market_cap_at_or_before(self.other_symbol, date(2024, 1, 10)))
        self.assertIsNone(get_market_cap_at_or_before(self.symbol, date(2023, 12, 31)))

    def test_preload_market_cap_series_respects_start_and_end_dates(self):
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 5),
            market_cap=Decimal("50.00"),
        )
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 10),
            market_cap=Decimal("100.00"),
        )
        HistoricalMarketCap.objects.create(
            symbol=self.symbol,
            date=date(2024, 1, 15),
            market_cap=Decimal("150.00"),
        )

        result = preload_market_cap_series(
            [self.symbol],
            start_date=date(2024, 1, 8),
            end_date=date(2024, 1, 12),
        )

        self.assertEqual(result[self.symbol.id], [(date(2024, 1, 10), Decimal("100.00"))])
