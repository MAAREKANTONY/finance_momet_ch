from __future__ import annotations

from io import StringIO
from unittest.mock import patch

from django.core.management import call_command
from django.test import TestCase

from core.models import DailyBar, Symbol
from core.services.benchmark_etf_sync import required_benchmark_tickers_for_symbols, sync_benchmark_etfs_for_symbols
from core.services.provider_twelvedata import sanitize_provider_error_message
from core.tasks import _fetch_daily_bars_for_symbols, _sanitize_provider_error_message


class BenchmarkEtfSyncServiceTests(TestCase):
    def setUp(self):
        self.us_symbol = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", country="US", sector="Technology", active=True)

    def test_detects_spy_for_us_stocks(self):
        tickers = required_benchmark_tickers_for_symbols([self.us_symbol])

        self.assertIn("SPY", tickers)

    def test_detects_sector_etf_from_sector(self):
        tickers = required_benchmark_tickers_for_symbols([self.us_symbol])

        self.assertIn("XLK", tickers)

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_creates_missing_etf_symbol(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}
        fetch_mock.return_value = {"symbols": 2, "bars": 0}

        totals = sync_benchmark_etfs_for_symbols([self.us_symbol], skip_ohlc=True)

        self.assertTrue(Symbol.objects.filter(ticker="SPY").exists())
        self.assertTrue(Symbol.objects.filter(ticker="XLK").exists())
        spy = Symbol.objects.get(ticker="SPY")
        self.assertTrue(spy.active)
        self.assertEqual(spy.instrument_type, "ETF")
        self.assertEqual(totals["created"], 2)

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_does_not_duplicate_existing_etf_symbol(self, enrich_mock, fetch_mock):
        Symbol.objects.create(ticker="SPY", exchange="", active=False)
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}
        fetch_mock.return_value = {"symbols": 2, "bars": 0}

        totals = sync_benchmark_etfs_for_symbols([self.us_symbol], skip_ohlc=True)

        self.assertEqual(Symbol.objects.filter(ticker="SPY").count(), 1)
        self.assertEqual(Symbol.objects.get(ticker="SPY").active, True)
        self.assertEqual(totals["existing"], 1)

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_calls_enrichment_only_when_not_skipped(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}
        fetch_mock.return_value = {"symbols": 2, "bars": 0}

        sync_benchmark_etfs_for_symbols([self.us_symbol], skip_ohlc=True)
        self.assertTrue(enrich_mock.called)
        enrich_mock.reset_mock()

        sync_benchmark_etfs_for_symbols([self.us_symbol], skip_enrichment=True, skip_ohlc=True)
        enrich_mock.assert_not_called()

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_calls_ohlc_fetch_only_when_not_skipped(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}
        fetch_mock.return_value = {"symbols": 2, "bars": 0}

        sync_benchmark_etfs_for_symbols([self.us_symbol])
        self.assertTrue(fetch_mock.called)
        fetch_mock.reset_mock()

        sync_benchmark_etfs_for_symbols([self.us_symbol], skip_ohlc=True)
        fetch_mock.assert_not_called()

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_dry_run_creates_nothing(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}

        totals = sync_benchmark_etfs_for_symbols([self.us_symbol], dry_run=True)

        self.assertFalse(Symbol.objects.filter(ticker="SPY").exists())
        self.assertFalse(Symbol.objects.filter(ticker="XLK").exists())
        fetch_mock.assert_not_called()
        self.assertEqual(totals["created"], 2)
        self.assertTrue(enrich_mock.called)

    def test_many_same_sector_symbols_resolve_to_small_unique_benchmark_set(self):
        extra_symbols = [
            Symbol(ticker=f"SYM{i:04d}", exchange="NASDAQ", country="US", sector="Technology", active=True)
            for i in range(7000)
        ]
        Symbol.objects.bulk_create(extra_symbols)

        tickers = required_benchmark_tickers_for_symbols(Symbol.objects.filter(active=True).only("ticker", "exchange", "country", "sector"))

        self.assertEqual(tickers, {"SPY", "XLK"})

    @patch("core.tasks.TwelveDataClient.time_series_daily")
    def test_etf_fetch_falls_back_to_ticker_only_when_exchange_fetch_returns_empty(self, time_series_mock):
        etf = Symbol.objects.create(
            ticker="XLK",
            exchange="NYSE ARCA",
            country="US",
            instrument_type="ETF",
            active=True,
        )
        time_series_mock.side_effect = [
            [],
            [{"datetime": "2024-01-02", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "1000"}],
        ]

        stats = _fetch_daily_bars_for_symbols(symbol_qs=[etf], outputsize=30)

        self.assertEqual(stats["bars"], 1)
        self.assertEqual(DailyBar.objects.filter(symbol=etf).count(), 1)
        first_call = time_series_mock.call_args_list[0]
        second_call = time_series_mock.call_args_list[1]
        self.assertEqual(first_call.args[0], "XLK")
        self.assertEqual(first_call.kwargs["exchange"], "NYSE ARCA")
        self.assertEqual(second_call.kwargs["exchange"], "")

    @patch("core.tasks.TwelveDataClient.time_series_daily")
    def test_normal_stock_fetch_behavior_is_unchanged_without_ticker_only_fallback(self, time_series_mock):
        stock = Symbol.objects.create(
            ticker="MSFT",
            exchange="NASDAQ",
            country="US",
            instrument_type="Common Stock",
            active=True,
        )
        time_series_mock.return_value = []

        stats = _fetch_daily_bars_for_symbols(symbol_qs=[stock], outputsize=30)

        self.assertEqual(stats["bars"], 0)
        self.assertEqual(time_series_mock.call_count, 1)
        self.assertEqual(time_series_mock.call_args.kwargs["exchange"], "NASDAQ")

    @patch("core.tasks.TwelveDataClient.time_series_daily")
    def test_us_exchange_common_stock_uses_ticker_only_directly(self, time_series_mock):
        stock = Symbol.objects.create(
            ticker="AAPL",
            exchange="US",
            country="US",
            instrument_type="Common Stock",
            active=True,
        )
        time_series_mock.return_value = [
            {"datetime": "2024-01-02", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "1000"}
        ]

        stats = _fetch_daily_bars_for_symbols(symbol_qs=[stock], outputsize=30)

        self.assertEqual(stats["bars"], 1)
        self.assertEqual(DailyBar.objects.filter(symbol=stock).count(), 1)
        self.assertEqual(time_series_mock.call_count, 1)
        self.assertEqual(time_series_mock.call_args.kwargs["exchange"], "")

    def test_provider_error_sanitizers_mask_api_keys(self):
        error = (
            "404 Client Error for url: "
            "https://api.twelvedata.com/time_series?symbol=AAPL&apikey=secret123&api_token=secret456"
        )

        task_message = _sanitize_provider_error_message(error)
        provider_message = sanitize_provider_error_message(error)

        self.assertNotIn("secret123", task_message)
        self.assertNotIn("secret456", task_message)
        self.assertIn("apikey=***", task_message)
        self.assertIn("api_token=***", task_message)
        self.assertEqual(task_message, provider_message)

    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    @patch("core.tasks.TwelveDataClient.time_series_daily")
    def test_sync_benchmark_etfs_inserts_sector_etf_bars_when_ticker_only_succeeds(self, time_series_mock, enrich_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}

        def _fake_values(symbol, exchange="", outputsize=10, start_date=None, end_date=None):
            if symbol == "SPY":
                return [{"datetime": "2024-01-02", "open": "100", "high": "101", "low": "99", "close": "100.5", "volume": "1000"}]
            if symbol == "XLK" and exchange == "NYSE ARCA":
                return []
            if symbol == "XLK" and exchange == "":
                return [{"datetime": "2024-01-02", "open": "10", "high": "11", "low": "9", "close": "10.5", "volume": "1000"}]
            return []

        time_series_mock.side_effect = _fake_values

        totals = sync_benchmark_etfs_for_symbols([self.us_symbol], skip_ohlc=False)

        xlk = Symbol.objects.get(ticker="XLK")
        self.assertEqual(DailyBar.objects.filter(symbol=xlk).count(), 1)
        self.assertEqual(totals["ohlc"]["bars"], 2)


class BenchmarkEtfSyncCommandTests(TestCase):
    def setUp(self):
        self.aapl = Symbol.objects.create(ticker="AAPL", exchange="NASDAQ", country="US", sector="Technology", active=True)
        self.xom = Symbol.objects.create(ticker="XOM", exchange="NYSE", country="US", sector="Energy", active=True)

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_command_supports_explicit_source_tickers(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}
        fetch_mock.return_value = {"symbols": 2, "bars": 0}
        out = StringIO()

        call_command("sync_benchmark_etfs", "--symbols=AAPL", "--skip-ohlc", stdout=out)

        body = out.getvalue()
        self.assertIn("SPY: created", body)
        self.assertIn("XLK: created", body)
        self.assertNotIn("XLE: created", body)

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_command_dry_run_creates_nothing(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 2, "unchanged": 2, "skipped": 0, "errors": 0, "per_symbol": []}
        out = StringIO()

        call_command("sync_benchmark_etfs", "--symbols=AAPL", "--dry-run", stdout=out)

        self.assertFalse(Symbol.objects.filter(ticker="SPY").exists())
        self.assertIn("dry_run=1", out.getvalue())
        fetch_mock.assert_not_called()

    @patch("core.tasks._fetch_daily_bars_for_symbols")
    @patch("core.services.benchmark_etf_sync.enrich_symbols_metadata")
    def test_existing_fetch_daily_bars_behavior_is_unchanged_by_skip_flag_usage(self, enrich_mock, fetch_mock):
        enrich_mock.return_value = {"updated": 0, "processed": 3, "unchanged": 3, "skipped": 0, "errors": 0, "per_symbol": []}
        out = StringIO()

        call_command("sync_benchmark_etfs", "--skip-ohlc", stdout=out)

        fetch_mock.assert_not_called()
        self.assertIn("skip_ohlc=1", out.getvalue())

    @patch("core.management.commands.sync_benchmark_etfs.sync_benchmark_etfs_for_symbols")
    def test_command_calls_shared_benchmark_service(self, sync_mock):
        sync_mock.return_value = {
            "source_symbols": 1,
            "benchmark_tickers": ["SPY", "XLK"],
            "created": 2,
            "existing": 0,
            "dry_run": False,
            "skip_enrichment": False,
            "skip_ohlc": True,
            "enrichment": {"updated": 0},
            "ohlc": None,
            "per_symbol": [{"ticker": "SPY", "status": "created"}, {"ticker": "XLK", "status": "created"}],
        }
        out = StringIO()

        call_command("sync_benchmark_etfs", "--symbols=AAPL", "--skip-ohlc", stdout=out)

        self.assertTrue(sync_mock.called)
        self.assertEqual(sync_mock.call_args.kwargs["skip_ohlc"], True)
