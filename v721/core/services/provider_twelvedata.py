import requests
from django.conf import settings

class TwelveDataClient:
    BASE_URL = "https://api.twelvedata.com"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or getattr(settings, "TWELVE_DATA_API_KEY", "")

    def _get(self, path: str, params: dict):
        if not self.api_key:
            raise RuntimeError("TWELVE_DATA_API_KEY is missing. Set it in .env")
        params = {**params, "apikey": self.api_key}
        r = requests.get(f"{self.BASE_URL}{path}", params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, dict) and data.get("status") == "error":
            raise RuntimeError(data.get("message") or "Unknown TwelveData error")
        return data

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
        """
        params = {"symbol": symbol, "interval": "1day", "outputsize": outputsize, "format": "JSON"}
        if exchange:
            params["exchange"] = exchange
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        data = self._get("/time_series", params)
        return data.get("values") or []

    def symbol_search(self, query: str, limit: int = 12, instrument_type: str = ""):
        params = {"symbol": query, "outputsize": limit}
        if instrument_type:
            params["instrument_type"] = instrument_type
        data = self._get("/symbol_search", params)
        return (data.get("data") or []) if isinstance(data, dict) else []
