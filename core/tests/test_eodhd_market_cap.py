from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import StringIO
from unittest.mock import Mock, call, patch

import requests
from django.core.management import call_command
from django.test import TestCase, override_settings

from core.models import HistoricalMarketCap, Symbol
from core.services.market_cap_sync import sync_market_caps_for_symbols
from core.services.provider_eodhd import (
    EODHDClient,
    EODHDError,
    UnsupportedEODHDSymbolError,
    normalize_historical_ohlc_payload,
    normalize_historical_market_cap_payload,
    normalize_sp500_historical_components_payload,
    normalize_symbol_general_metadata_payload,
    sanitize_provider_error_message,
    to_eodhd_symbol,
)


class EODHDMarketCapClientTests(TestCase):
    def test_normalize_historical_ohlc_payload_handles_valid_rows(self):
        payload = {
            "0": {
                "date": "2024-01-02",
                "open": "10.1",
                "high": "11.2",
                "low": "9.8",
                "close": "10.9",
                "adjusted_close": "10.7",
                "volume": "12345",
            }
        }

        rows = normalize_historical_ohlc_payload(payload, "AAPL.US")

        self.assertEqual(rows[0]["date"], date(2024, 1, 2))
        self.assertEqual(rows[0]["open"], Decimal("10.1"))
        self.assertEqual(rows[0]["high"], Decimal("11.2"))
        self.assertEqual(rows[0]["low"], Decimal("9.8"))
        self.assertEqual(rows[0]["close"], Decimal("10.9"))
        self.assertEqual(rows[0]["adjusted_close"], Decimal("10.7"))
        self.assertEqual(rows[0]["volume"], 12345)
        self.assertEqual(rows[0]["provider_symbol"], "AAPL.US")

    def test_normalize_historical_ohlc_payload_skips_invalid_rows(self):
        payload = [
            {"date": "bad", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "100"},
            {"date": "2024-01-02", "open": "10", "high": "", "low": "9", "close": "10", "volume": "100"},
            {"date": "2024-01-03", "open": "10", "high": "11", "low": "9", "close": "10", "volume": "100"},
        ]

        rows = normalize_historical_ohlc_payload(payload, "AAPL.US")

        self.assertEqual([row["date"] for row in rows], [date(2024, 1, 3)])

    def test_normalize_historical_ohlc_payload_empty_payload_returns_empty_list(self):
        self.assertEqual(normalize_historical_ohlc_payload([], "AAPL.US"), [])
        self.assertEqual(normalize_historical_ohlc_payload({"data": []}, "AAPL.US"), [])

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_client_fetch_historical_ohlc_uses_eod_endpoint(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = [
            {"date": "2024-01-02", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "1000"},
        ]
        mock_get.return_value = response

        rows = EODHDClient().fetch_historical_ohlc("AAPL.US", date(2024, 1, 1), date(2024, 1, 31))

        self.assertEqual(rows[0]["date"], date(2024, 1, 2))
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(args[0], "https://example.test/api/eod/AAPL.US")
        self.assertEqual(kwargs["params"]["from"], "2024-01-01")
        self.assertEqual(kwargs["params"]["to"], "2024-01-31")
        self.assertEqual(kwargs["params"]["period"], "d")
        self.assertEqual(kwargs["params"]["fmt"], "json")

    def test_sanitize_provider_error_message_masks_api_keys(self):
        message = sanitize_provider_error_message(
            "GET /eod/AAPL.US?api_token=secret-token&fmt=json apikey=other-secret"
        )

        self.assertIn("api_token=***", message)
        self.assertIn("apikey=***", message)
        self.assertNotIn("secret-token", message)
        self.assertNotIn("other-secret", message)

    def test_normalize_sp500_historical_components_payload_handles_dict_indexed_payload(self):
        payload = {
            "0": {"Code": "AAPL", "Name": "Apple Inc", "StartDate": "1982-11-30", "EndDate": None, "IsActiveNow": 1, "IsDelisted": 0},
            "1": {"Code": "OLD", "Name": "Old Corp", "StartDate": "2020-01-01", "EndDate": "2020-01-02", "IsActiveNow": 0, "IsDelisted": 1},
        }

        rows = normalize_sp500_historical_components_payload(payload)

        self.assertEqual(rows[0]["Code"], "AAPL")
        self.assertEqual(rows[0]["Name"], "Apple Inc")
        self.assertEqual(rows[0]["StartDate"], "1982-11-30")
        self.assertIsNone(rows[0]["EndDate"])
        self.assertEqual(rows[1]["Code"], "OLD")

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_client_fetch_sp500_historical_components_uses_fundamentals_filter(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "0": {"Code": "AAPL", "Name": "Apple Inc", "StartDate": "1982-11-30", "EndDate": None, "IsActiveNow": 1, "IsDelisted": 0},
        }
        mock_get.return_value = response

        rows = EODHDClient().fetch_sp500_historical_components()

        self.assertEqual(rows[0]["Code"], "AAPL")
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(args[0], "https://example.test/api/fundamentals/GSPC.INDX")
        self.assertEqual(kwargs["params"]["filter"], "HistoricalTickerComponents")
        self.assertEqual(kwargs["params"]["fmt"], "json")
        self.assertNotIn("token", args[0])

    def test_normalize_symbol_general_metadata_payload_extracts_sector_and_industry(self):
        payload = {
            "Code": "600000",
            "Name": "SPD Bank",
            "Exchange": "SHG",
            "CurrencyCode": "CNY",
            "CountryName": "China",
            "Sector": "Financial Services",
            "Industry": "Banks - Regional",
            "GicSector": "Financials",
            "GicIndustry": "Banks",
        }

        row = normalize_symbol_general_metadata_payload(payload, "600000.SHG")

        self.assertEqual(row["provider_symbol"], "600000.SHG")
        self.assertEqual(row["name"], "SPD Bank")
        self.assertEqual(row["exchange"], "SHG")
        self.assertEqual(row["currency"], "CNY")
        self.assertEqual(row["country"], "China")
        self.assertEqual(row["sector"], "Financial Services")
        self.assertEqual(row["industry"], "Banks - Regional")
        self.assertEqual(row["gic_sector"], "Financials")

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_client_fetch_symbol_general_metadata_uses_fundamentals_general_filter(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "Code": "600000",
            "Name": "SPD Bank",
            "Exchange": "SHG",
            "CurrencyCode": "CNY",
            "CountryName": "China",
            "Sector": "Financial Services",
            "Industry": "Banks - Regional",
        }
        mock_get.return_value = response

        row = EODHDClient().fetch_symbol_general_metadata("600000.SHG")

        self.assertEqual(row["sector"], "Financial Services")
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(args[0], "https://example.test/api/fundamentals/600000.SHG")
        self.assertEqual(kwargs["params"]["filter"], "General")
        self.assertEqual(kwargs["params"]["fmt"], "json")
        self.assertNotIn("token", args[0])

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_client_fetch_sp500_historical_components_rejects_invalid_payload(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {"unexpected": {"foo": "bar"}}
        mock_get.return_value = response

        with self.assertRaises(EODHDError) as exc:
            EODHDClient().fetch_sp500_historical_components()

        self.assertIn("Unsupported EODHD S&P500 components payload shape", str(exc.exception))

    @override_settings(EODHD_API_KEY="", EODHD_BASE_URL="https://example.test/api")
    def test_client_fetch_sp500_historical_components_requires_api_key(self):
        with self.assertRaisesMessage(EODHDError, "EODHD_API_KEY is missing"):
            EODHDClient().fetch_sp500_historical_components()

    def test_normalize_historical_market_cap_payload_handles_dict_indexed_payload(self):
        payload = {
            "0": {"date": "2024-01-05", "value": 2801386317300},
            "1": {"date": "2024-01-12", "value": 2874675704300},
        }

        rows = normalize_historical_market_cap_payload(payload, "AAPL.US")

        self.assertEqual(
            rows,
            [
                {
                    "date": date(2024, 1, 5),
                    "market_cap": Decimal("2801386317300"),
                    "currency": "",
                    "provider_symbol": "AAPL.US",
                    "source_payload": {"date": "2024-01-05", "value": 2801386317300},
                },
                {
                    "date": date(2024, 1, 12),
                    "market_cap": Decimal("2874675704300"),
                    "currency": "",
                    "provider_symbol": "AAPL.US",
                    "source_payload": {"date": "2024-01-12", "value": 2874675704300},
                },
            ],
        )

    def test_client_normalizes_sample_payload(self):
        payload = [
            {"date": "2024-01-02", "market_cap": "1000000", "currency": "USD"},
            {"date": "2024-01-03", "value": 2000000},
        ]

        rows = normalize_historical_market_cap_payload(payload, "AAPL.US")

        self.assertEqual(rows[0]["date"], date(2024, 1, 2))
        self.assertEqual(rows[0]["market_cap"], Decimal("1000000"))
        self.assertEqual(rows[0]["currency"], "USD")
        self.assertEqual(rows[0]["provider_symbol"], "AAPL.US")
        self.assertEqual(rows[1]["market_cap"], Decimal("2000000"))

    def test_client_normalized_rows_use_python_date_objects(self):
        rows = normalize_historical_market_cap_payload(
            [{"date": "2024-01-02", "market_cap": "1000000"}],
            "AAPL.US",
        )

        self.assertIsInstance(rows[0]["date"], date)

    def test_client_empty_payload_returns_empty_list(self):
        self.assertEqual(normalize_historical_market_cap_payload([], "AAPL.US"), [])
        self.assertEqual(normalize_historical_market_cap_payload({"data": []}, "AAPL.US"), [])

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_client_fetch_historical_market_cap_supports_dict_indexed_payload(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "0": {"date": "2024-01-05", "value": 2801386317300},
            "1": {"date": "2024-01-12", "value": 2874675704300},
        }
        mock_get.return_value = response

        rows = EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 31))

        self.assertEqual([row["date"] for row in rows], [date(2024, 1, 5), date(2024, 1, 12)])
        self.assertEqual(rows[0]["market_cap"], Decimal("2801386317300"))
        self.assertEqual(rows[1]["market_cap"], Decimal("2874675704300"))

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_provider_error_payload_raises_controlled_error(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {"status": "error", "message": "No access"}
        mock_get.return_value = response

        with self.assertRaises(EODHDError):
            EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api")
    @patch("core.services.provider_eodhd.requests.get")
    def test_malformed_non_empty_payload_raises_controlled_error(self, mock_get):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = {"unexpected": {"foo": "bar"}}
        mock_get.return_value = response

        with self.assertRaises(EODHDError) as exc:
            EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        self.assertIn("Unsupported EODHD historical market cap payload shape", str(exc.exception))
        self.assertIn("unexpected", str(exc.exception))

    @override_settings(
        EODHD_API_KEY="token",
        EODHD_BASE_URL="https://example.test/api",
        EODHD_MAX_RETRIES=3,
        EODHD_BACKOFF_SECONDS=1,
    )
    @patch("core.services.provider_eodhd.time.sleep")
    @patch("core.services.provider_eodhd.requests.get")
    def test_retry_on_http_429_succeeds_after_retry(self, mock_get, mock_sleep):
        first = Mock()
        first.status_code = 429
        first.raise_for_status.side_effect = requests.HTTPError("rate limited")
        second = Mock()
        second.status_code = 200
        second.raise_for_status.return_value = None
        second.json.return_value = [{"date": "2024-01-02", "market_cap": "1000000"}]
        mock_get.side_effect = [first, second]

        rows = EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        self.assertEqual(len(rows), 1)
        mock_sleep.assert_called_once_with(1.0)
        self.assertEqual(mock_get.call_count, 2)

    @override_settings(
        EODHD_API_KEY="token",
        EODHD_BASE_URL="https://example.test/api",
        EODHD_MAX_RETRIES=3,
        EODHD_BACKOFF_SECONDS=1,
    )
    @patch("core.services.provider_eodhd.time.sleep")
    @patch("core.services.provider_eodhd.requests.get")
    def test_retry_on_http_5xx_succeeds_after_retry(self, mock_get, mock_sleep):
        first = Mock()
        first.status_code = 503
        first.raise_for_status.side_effect = requests.HTTPError("temporary outage")
        second = Mock()
        second.status_code = 200
        second.raise_for_status.return_value = None
        second.json.return_value = [{"date": "2024-01-02", "market_cap": "1000000"}]
        mock_get.side_effect = [first, second]

        rows = EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        self.assertEqual(len(rows), 1)
        mock_sleep.assert_called_once_with(1.0)
        self.assertEqual(mock_get.call_count, 2)

    @override_settings(EODHD_API_KEY="token", EODHD_BASE_URL="https://example.test/api", EODHD_MAX_RETRIES=3)
    @patch("core.services.provider_eodhd.time.sleep")
    @patch("core.services.provider_eodhd.requests.get")
    def test_no_retry_on_non_429_4xx(self, mock_get, mock_sleep):
        response = Mock()
        response.status_code = 404
        response.raise_for_status.side_effect = requests.HTTPError("not found")
        mock_get.return_value = response

        with self.assertRaises(EODHDError):
            EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        mock_sleep.assert_not_called()
        self.assertEqual(mock_get.call_count, 1)

    @override_settings(EODHD_API_KEY="secret-token", EODHD_BASE_URL="https://example.test/api", EODHD_MAX_RETRIES=0)
    @patch("core.services.provider_eodhd.requests.get")
    def test_http_error_masks_api_token_from_exception_text(self, mock_get):
        response = Mock()
        response.status_code = 403
        response.raise_for_status.side_effect = requests.HTTPError(
            "403 Client Error for url: https://eodhd.com/api/eod/AAPL.US?api_token=secret-token&fmt=json"
        )
        mock_get.return_value = response

        with self.assertRaises(EODHDError) as ctx:
            EODHDClient().fetch_historical_ohlc("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        self.assertIn("api_token=***", str(ctx.exception))
        self.assertNotIn("secret-token", str(ctx.exception))

    @override_settings(EODHD_API_KEY="secret-token", EODHD_BASE_URL="https://example.test/api", EODHD_MAX_RETRIES=0)
    @patch("core.services.provider_eodhd.requests.get")
    def test_network_error_masks_api_token_from_exception_text(self, mock_get):
        mock_get.side_effect = requests.ConnectionError(
            "Failed to establish connection to https://eodhd.com/api/eod/AAPL.US?api_token=secret-token&fmt=json"
        )

        with self.assertRaises(EODHDError) as ctx:
            EODHDClient().fetch_historical_ohlc("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        self.assertIn("api_token=***", str(ctx.exception))
        self.assertNotIn("secret-token", str(ctx.exception))

    @override_settings(
        EODHD_API_KEY="token",
        EODHD_BASE_URL="https://example.test/api",
        EODHD_MAX_RETRIES=3,
        EODHD_BACKOFF_SECONDS=2,
    )
    @patch("core.services.provider_eodhd.time.sleep")
    @patch("core.services.provider_eodhd.requests.get")
    def test_timeout_and_connection_errors_retry_with_exponential_backoff(self, mock_get, mock_sleep):
        response = Mock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.return_value = [{"date": "2024-01-02", "market_cap": "1000000"}]
        mock_get.side_effect = [requests.Timeout("slow"), requests.ConnectionError("offline"), response]

        rows = EODHDClient().fetch_historical_market_cap("AAPL.US", date(2024, 1, 1), date(2024, 1, 2))

        self.assertEqual(len(rows), 1)
        mock_sleep.assert_has_calls([call(2.0), call(4.0)])
        self.assertEqual(mock_get.call_count, 3)


class EODHDSymbolMappingTests(TestCase):
    def test_us_exchange_maps_to_dot_us(self):
        self.assertEqual(
            to_eodhd_symbol(Symbol(ticker="AAPL", exchange="NASDAQ")),
            "AAPL.US",
        )
        self.assertEqual(
            to_eodhd_symbol(Symbol(ticker="MSFT", exchange="NYSE")),
            "MSFT.US",
        )

    def test_unsupported_exchange_is_handled_explicitly(self):
        with self.assertRaises(UnsupportedEODHDSymbolError):
            to_eodhd_symbol(Symbol(ticker="SHOP", exchange="TSX"))


class MarketCapSyncServiceTests(TestCase):
    def setUp(self):
        self.aapl = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", active=True)
        self.msft = Symbol.objects.create(ticker="MSFT", exchange="NYSE", active=True)

    def _rows(self, provider_symbol: str, market_cap: str = "1000000"):
        return [
            {
                "date": date(2024, 1, 2),
                "market_cap": Decimal(market_cap),
                "currency": "USD",
                "provider_symbol": provider_symbol,
                "source_payload": {"date": "2024-01-02", "market_cap": market_cap, "currency": "USD"},
            }
        ]

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_creates_historical_market_cap_rows(self, mock_client_cls):
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = self._rows("AAPL.US")

        stats = sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31))

        row = HistoricalMarketCap.objects.get(symbol=self.aapl, date=date(2024, 1, 2))
        self.assertEqual(row.market_cap, Decimal("1000000.00"))
        self.assertEqual(row.provider_symbol, "AAPL.US")
        self.assertEqual(stats["inserted"], 1)

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_inserts_rows_from_dict_indexed_payload_shape(self, mock_client_cls):
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = normalize_historical_market_cap_payload(
            {
                "0": {"date": "2024-01-05", "value": 2801386317300},
                "1": {"date": "2024-01-12", "value": 2874675704300},
            },
            "AAPL.US",
        )

        stats = sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31))

        self.assertEqual(stats["inserted"], 2)
        self.assertEqual(
            list(HistoricalMarketCap.objects.filter(symbol=self.aapl).order_by("date").values_list("date", flat=True)),
            [date(2024, 1, 5), date(2024, 1, 12)],
        )

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_is_idempotent(self, mock_client_cls):
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = self._rows("AAPL.US")

        stats = [
            sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31)),
            sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31)),
        ]

        self.assertEqual(HistoricalMarketCap.objects.filter(symbol=self.aapl).count(), 1)
        self.assertEqual(stats[1]["updated"], 0)
        self.assertEqual(stats[1]["existing"], 1)

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_updates_changed_values(self, mock_client_cls):
        HistoricalMarketCap.objects.create(
            symbol=self.aapl,
            date=date(2024, 1, 2),
            market_cap=Decimal("1.00"),
            provider_symbol="AAPL.US",
        )
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = self._rows("AAPL.US", "2500000")

        stats = sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31))

        row = HistoricalMarketCap.objects.get(symbol=self.aapl, date=date(2024, 1, 2))
        self.assertEqual(row.market_cap, Decimal("2500000.00"))
        self.assertEqual(stats["updated"], 1)

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_reports_existing_when_nothing_changed(self, mock_client_cls):
        HistoricalMarketCap.objects.create(
            symbol=self.aapl,
            date=date(2024, 1, 2),
            market_cap=Decimal("1000000.00"),
            provider_symbol="AAPL.US",
            currency="USD",
            source_payload={"date": "2024-01-02", "market_cap": "1000000", "currency": "USD"},
        )
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = self._rows("AAPL.US")

        stats = sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31))

        self.assertEqual(stats["existing"], 1)
        self.assertEqual(stats["updated"], 0)

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_dry_run_writes_nothing(self, mock_client_cls):
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = self._rows("AAPL.US")

        stats = sync_market_caps_for_symbols([self.aapl], date(2024, 1, 1), date(2024, 1, 31), dry_run=True)

        self.assertFalse(HistoricalMarketCap.objects.exists())
        self.assertEqual(stats["fetched"], 1)
        self.assertEqual(stats["inserted"], 0)

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_unsupported_exchange_is_skipped(self, mock_client_cls):
        shop = Symbol.objects.create(ticker="SHOP", exchange="TSX", active=True)

        stats = sync_market_caps_for_symbols([shop], date(2024, 1, 1), date(2024, 1, 31))

        self.assertEqual(stats["skipped"], 1)
        mock_client_cls.return_value.fetch_historical_market_cap.assert_not_called()

    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_one_symbol_failure_does_not_stop_others(self, mock_client_cls):
        def fake_fetch(provider_symbol, from_date, to_date):
            if provider_symbol == "AAPL.US":
                raise EODHDError("temporary provider error")
            return self._rows(provider_symbol)

        mock_client_cls.return_value.fetch_historical_market_cap.side_effect = fake_fetch

        stats = sync_market_caps_for_symbols([self.aapl, self.msft], date(2024, 1, 1), date(2024, 1, 31))

        self.assertFalse(HistoricalMarketCap.objects.filter(symbol=self.aapl).exists())
        self.assertTrue(HistoricalMarketCap.objects.filter(symbol=self.msft).exists())
        self.assertEqual(stats["errors"], 1)

    @patch("core.services.market_cap_sync.time.sleep")
    @patch("core.services.market_cap_sync.EODHDClient")
    def test_service_respects_request_delay_between_symbols(self, mock_client_cls, mock_sleep):
        mock_client_cls.return_value.fetch_historical_market_cap.return_value = self._rows("AAPL.US")

        sync_market_caps_for_symbols(
            [self.aapl, self.msft],
            date(2024, 1, 1),
            date(2024, 1, 31),
            request_delay_seconds=0.5,
        )

        mock_sleep.assert_called_once_with(0.5)


class SyncMarketCapsEODHDCommandTests(TestCase):
    def setUp(self):
        self.aapl = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", active=True)

    @patch("core.management.commands.sync_market_caps_eodhd.sync_market_caps_for_symbols")
    def test_command_delegates_to_shared_service(self, mock_sync):
        mock_sync.return_value = {
            "fetched": 1,
            "inserted": 1,
            "updated": 0,
            "existing": 0,
            "skipped": 0,
            "errors": 0,
            "per_symbol": [{
                "symbol": str(self.aapl),
                "provider_symbol": "AAPL.US",
                "fetched": 1,
                "inserted": 1,
                "updated": 0,
                "existing": 0,
                "skipped": False,
                "error": "",
                "dry_run": False,
            }],
        }

        out = StringIO()
        call_command(
            "sync_market_caps_eodhd",
            "--symbols", "AAPL",
            "--from", "2024-01-01",
            "--to", "2024-01-31",
            stdout=out,
        )

        self.assertTrue(mock_sync.called)
        self.assertIn("summary fetched=1 inserted=1 updated=0 existing=0 skipped=0 errors=0", out.getvalue())
