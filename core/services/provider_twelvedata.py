import logging
import time

import requests
from django.conf import settings

from .twelvedata_rate_limiter import get_twelvedata_rate_limiter

logger = logging.getLogger(__name__)


class TwelveDataRateLimitError(RuntimeError):
    pass


class TwelveDataClient:
    BASE_URL = "https://api.twelvedata.com"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or getattr(settings, "TWELVE_DATA_API_KEY", "")
        self.max_retries = max(0, int(getattr(settings, "TWELVEDATA_MAX_RETRIES", 3)))
        self.backoff_seconds = max(1, int(getattr(settings, "TWELVEDATA_BACKOFF_SECONDS", 65)))

    @staticmethod
    def _is_rate_limit_error_message(message: str) -> bool:
        msg = (message or "").lower()
        return (
            "run out of api credits for the current minute" in msg
            or "current minute" in msg and "api credits" in msg
            or "too many requests" in msg
            or "rate limit" in msg
        )

    @staticmethod
    def _is_no_data_for_dates_error_message(message: str) -> bool:
        msg = (message or "").lower()
        return (
            "no data is available on the specified dates" in msg
            or "try setting different start/end dates" in msg
        )

    def _request_once(self, path: str, params: dict):
        if not self.api_key:
            raise RuntimeError("TWELVE_DATA_API_KEY is missing. Set it in .env")

        # Global throttle before each provider call.
        get_twelvedata_rate_limiter().wait_for_slot()

        req_params = {**params, "apikey": self.api_key}
        r = requests.get(f"{self.BASE_URL}{path}", params=req_params, timeout=30)

        # Twelve Data can reply with HTTP 429 or with a JSON payload carrying the error.
        if r.status_code == 429:
            raise TwelveDataRateLimitError("HTTP 429 from Twelve Data")

        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("status") == "error":
            message = data.get("message") or "Unknown TwelveData error"
            if self._is_rate_limit_error_message(message):
                raise TwelveDataRateLimitError(message)
            raise RuntimeError(message)
        return data

    def _get(self, path: str, params: dict):
        attempts = self.max_retries + 1
        last_error = None
        for attempt in range(1, attempts + 1):
            try:
                return self._request_once(path, params)
            except TwelveDataRateLimitError as e:
                last_error = e
                if attempt >= attempts:
                    raise
                logger.warning(
                    "[twelvedata] rate limit hit (attempt %s/%s). sleeping %ss before retry. error=%s",
                    attempt,
                    attempts,
                    self.backoff_seconds,
                    e,
                )
                time.sleep(self.backoff_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("Unexpected TwelveData client state")

    def time_series_daily(
        self,
        symbol: str,
        exchange: str = "",
        outputsize: int = 10,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        """Fetch daily time series.

        Twelve Data supports optional start_date / end_date filters. When provided,
        the API returns values within the requested range.

        Provider quirk handled here:
        an incremental request with start_date/end_date can sometimes fail with
        "No data is available on the specified dates" even for symbols that are
        otherwise valid. In that case we transparently retry once without the date
        filters and let the caller deduplicate / upsert.
        """
        params = {"symbol": symbol, "interval": "1day", "outputsize": outputsize, "format": "JSON"}
        if exchange:
            params["exchange"] = exchange
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        try:
            data = self._get("/time_series", params)
        except RuntimeError as e:
            if (start_date or end_date) and self._is_no_data_for_dates_error_message(str(e)):
                fallback_params = {
                    "symbol": symbol,
                    "interval": "1day",
                    "outputsize": outputsize,
                    "format": "JSON",
                }
                if exchange:
                    fallback_params["exchange"] = exchange
                logger.warning(
                    "[twelvedata] incremental fetch returned no data for %s%s; retrying once without date filters",
                    symbol,
                    f":{exchange}" if exchange else "",
                )
                data = self._get("/time_series", fallback_params)
            else:
                raise
        return data.get("values") or []

    def symbol_search(self, query: str, limit: int = 12, instrument_type: str = ""):
        params = {"symbol": query, "outputsize": limit}
        if instrument_type:
            params["instrument_type"] = instrument_type
        data = self._get("/symbol_search", params)
        return (data.get("data") or []) if isinstance(data, dict) else []
