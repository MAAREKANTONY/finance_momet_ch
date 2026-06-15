from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
import time
from typing import Any

import requests
from django.conf import settings

from core.models import Symbol


class EODHDError(RuntimeError):
    pass


class UnsupportedEODHDSymbolError(ValueError):
    pass


US_EODHD_EXCHANGES = {
    "AMEX",
    "ARCA",
    "BATS",
    "NASDAQ",
    "NYSE",
    "NYSEARCA",
    "NYSEMKT",
    "OTC",
    "OTCQB",
    "OTCQX",
    "PINK",
    "US",
}


def to_eodhd_symbol(symbol: Symbol) -> str:
    ticker = str(getattr(symbol, "ticker", "") or "").strip().upper()
    exchange = str(getattr(symbol, "exchange", "") or "").strip().upper()
    if not ticker:
        raise UnsupportedEODHDSymbolError("Symbol ticker is empty")
    if not exchange:
        return ticker
    if exchange in US_EODHD_EXCHANGES:
        return f"{ticker}.US"
    raise UnsupportedEODHDSymbolError(f"Unsupported EODHD exchange mapping: {exchange}")


class EODHDClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        max_retries: int | None = None,
        backoff_seconds: float | None = None,
    ):
        self.api_key = api_key or getattr(settings, "EODHD_API_KEY", "")
        self.base_url = (base_url or getattr(settings, "EODHD_BASE_URL", "https://eodhd.com/api")).rstrip("/")
        self.max_retries = int(
            max_retries if max_retries is not None else getattr(settings, "EODHD_MAX_RETRIES", 3)
        )
        self.backoff_seconds = float(
            backoff_seconds if backoff_seconds is not None else getattr(settings, "EODHD_BACKOFF_SECONDS", 1)
        )

    def _get(self, path: str, params: dict[str, Any]) -> Any:
        if not self.api_key:
            raise EODHDError("EODHD_API_KEY is missing. Set it in .env")
        request_params = {**params, "api_token": self.api_key, "fmt": "json"}

        for attempt in range(self.max_retries + 1):
            try:
                response = requests.get(
                    f"{self.base_url}{path}",
                    params=request_params,
                    timeout=30,
                )
                try:
                    response.raise_for_status()
                except requests.HTTPError as exc:
                    status_code = getattr(response, "status_code", None)
                    if self._should_retry_http_status(status_code) and attempt < self.max_retries:
                        self._sleep_before_retry(attempt)
                        continue
                    raise EODHDError(self._http_error_message(status_code, exc)) from exc
                payload = response.json()
                if isinstance(payload, dict) and (
                    payload.get("status") == "error"
                    or payload.get("error")
                    or payload.get("message") and not _records_from_payload(payload)
                ):
                    message = payload.get("message") or payload.get("error") or "Unknown EODHD error"
                    raise EODHDError(str(message))
                return payload
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt < self.max_retries:
                    self._sleep_before_retry(attempt)
                    continue
                raise EODHDError(str(exc)) from exc

        raise EODHDError("EODHD request failed after retries")

    def _sleep_before_retry(self, attempt: int) -> None:
        time.sleep(self.backoff_seconds * (2 ** attempt))

    def _should_retry_http_status(self, status_code: int | None) -> bool:
        return status_code == 429 or bool(status_code and 500 <= status_code <= 599)

    def _http_error_message(self, status_code: int | None, exc: Exception) -> str:
        if status_code is None:
            return f"EODHD HTTP error: {exc}"
        return f"EODHD HTTP {status_code}: {exc}"

    def fetch_historical_market_cap(
        self,
        provider_symbol: str,
        from_date: date | str,
        to_date: date | str,
    ) -> list[dict[str, Any]]:
        payload = self._get(
            f"/historical-market-cap/{provider_symbol}",
            {"from": str(from_date), "to": str(to_date)},
        )
        rows = normalize_historical_market_cap_payload(payload, provider_symbol)
        if rows:
            return rows
        if payload in (None, "", [], {}):
            return []
        raise EODHDError(_unsupported_payload_message(payload))

    def fetch_sp500_historical_components(self) -> list[dict[str, Any]]:
        payload = self._get(
            "/fundamentals/GSPC.INDX",
            {"filter": "HistoricalTickerComponents"},
        )
        rows = normalize_sp500_historical_components_payload(payload)
        if rows:
            return rows
        if payload in (None, "", [], {}):
            return []
        raise EODHDError(_unsupported_sp500_payload_message(payload))


def _records_from_payload(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        numeric_key_items = [
            (int(key), value)
            for key, value in payload.items()
            if isinstance(key, str) and key.isdigit()
        ]
        if numeric_key_items:
            return [value for _idx, value in sorted(numeric_key_items, key=lambda item: item[0])]
        for key in ("data", "values", "results", "items", "market_cap", "market_caps"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                nested_records = _records_from_payload(value)
                if nested_records:
                    return nested_records
    return []


def _market_cap_from_record(record: dict[str, Any]) -> Decimal | None:
    for key in ("market_cap", "marketcap", "marketCapitalization", "value"):
        value = record.get(key)
        if value in (None, ""):
            continue
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    return None


def normalize_historical_market_cap_payload(payload: Any, provider_symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _records_from_payload(payload):
        if not isinstance(record, dict):
            continue
        row_date = _date_from_record(record.get("date"))
        market_cap = _market_cap_from_record(record)
        if not row_date or market_cap is None:
            continue
        rows.append({
            "date": row_date,
            "market_cap": market_cap,
            "currency": str(record.get("currency") or ""),
            "provider_symbol": provider_symbol,
            "source_payload": record,
        })
    return rows


def normalize_sp500_historical_components_payload(payload: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _records_from_payload(payload):
        if not isinstance(record, dict):
            continue
        code = str(record.get("Code") or "").strip().upper()
        name = str(record.get("Name") or "").strip()
        if not code:
            raise EODHDError("Unsupported EODHD S&P500 components payload: missing Code.")
        rows.append({
            "Code": code,
            "Name": name,
            "StartDate": record.get("StartDate"),
            "EndDate": record.get("EndDate"),
            "IsActiveNow": record.get("IsActiveNow"),
            "IsDelisted": record.get("IsDelisted"),
            "source_payload": record,
        })
    return rows


def _unsupported_payload_message(payload: Any) -> str:
    if isinstance(payload, dict):
        sample_keys = list(payload.keys())[:5]
        return (
            "Unsupported EODHD historical market cap payload shape: "
            f"dict keys={sample_keys}"
        )
    return (
        "Unsupported EODHD historical market cap payload shape: "
        f"type={type(payload).__name__}"
    )


def _unsupported_sp500_payload_message(payload: Any) -> str:
    if isinstance(payload, dict):
        sample_keys = list(payload.keys())[:5]
        return (
            "Unsupported EODHD S&P500 components payload shape: "
            f"dict keys={sample_keys}"
        )
    return (
        "Unsupported EODHD S&P500 components payload shape: "
        f"type={type(payload).__name__}"
    )


def _date_from_record(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        return None
