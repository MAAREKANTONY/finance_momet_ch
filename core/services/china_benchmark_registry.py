from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any


PROVIDER_EODHD = "EODHD"
STATUS_PROVIDER_VALIDATED = "PROVIDER_VALIDATED"
STATUS_UNSUPPORTED = "UNSUPPORTED"
VALIDATED_USABLE_FROM = date(2021, 8, 20)


RAW_SECTOR_TO_CANONICAL = {
    "Basic Materials": "Materials",
    "Materials": "Materials",
    "Communication Services": "Communication Services",
    "Consumer Cyclical": "Consumer Discretionary",
    "Consumer Goods": "Consumer Discretionary",
    "Consumer Defensive": "Consumer Staples",
    "Energy": "Energy",
    "Financial Services": "Financials",
    "Healthcare": "Health Care",
    "Industrials": "Industrials",
    "Real Estate": "Real Estate",
    "Technology": "Information Technology",
    "Utilities": "Utilities",
}


@dataclass(frozen=True)
class ChinaBenchmarkDefinition:
    canonical_sector: str
    provider_symbol: str | None
    exchange: str
    instrument_type: str
    provider: str
    usable_from: date | None
    status: str
    name: str
    is_market: bool = False
    is_fallback: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "canonical_sector": self.canonical_sector,
            "provider_symbol": self.provider_symbol,
            "exchange": self.exchange,
            "instrument_type": self.instrument_type,
            "provider": self.provider,
            "usable_from": self.usable_from.isoformat() if self.usable_from else None,
            "status": self.status,
            "name": self.name,
            "is_market": self.is_market,
            "is_fallback": self.is_fallback,
        }


CSI300_MARKET_BENCHMARK = ChinaBenchmarkDefinition(
    canonical_sector="MARKET_CSI300",
    provider_symbol="000300.SHG",
    exchange="SHG",
    instrument_type="INDEX",
    provider=PROVIDER_EODHD,
    usable_from=VALIDATED_USABLE_FROM,
    status=STATUS_PROVIDER_VALIDATED,
    name="CSI 300 Index",
    is_market=True,
)

CSI300_MARKET_FALLBACK = ChinaBenchmarkDefinition(
    canonical_sector="MARKET_CSI300",
    provider_symbol="510300.SHG",
    exchange="SHG",
    instrument_type="ETF",
    provider=PROVIDER_EODHD,
    usable_from=VALIDATED_USABLE_FROM,
    status=STATUS_PROVIDER_VALIDATED,
    name="Huatai-PB CSI 300 ETF",
    is_market=True,
    is_fallback=True,
)

CSI300_SECTOR_BENCHMARKS = {
    "Materials": ChinaBenchmarkDefinition("Materials", "159944.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Materials ETF"),
    "Consumer Discretionary": ChinaBenchmarkDefinition("Consumer Discretionary", "159936.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Consumer Discretionary ETF"),
    "Consumer Staples": ChinaBenchmarkDefinition("Consumer Staples", "159928.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Consumer ETF"),
    "Health Care": ChinaBenchmarkDefinition("Health Care", "159929.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Health Care ETF"),
    "Financials": ChinaBenchmarkDefinition("Financials", "159931.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Financial ETF"),
    "Information Technology": ChinaBenchmarkDefinition("Information Technology", "159939.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Information Technology ETF"),
    "Communication Services": ChinaBenchmarkDefinition("Communication Services", "515880.SHG", "SHG", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Communications ETF"),
    "Energy": ChinaBenchmarkDefinition("Energy", "159930.SHE", "SHE", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Energy ETF"),
    "Real Estate": ChinaBenchmarkDefinition("Real Estate", "512200.SHG", "SHG", "ETF", PROVIDER_EODHD, VALIDATED_USABLE_FROM, STATUS_PROVIDER_VALIDATED, "Real Estate ETF"),
    "Industrials": ChinaBenchmarkDefinition("Industrials", None, "", "", PROVIDER_EODHD, None, STATUS_UNSUPPORTED, "Unsupported"),
    "Utilities": ChinaBenchmarkDefinition("Utilities", None, "", "", PROVIDER_EODHD, None, STATUS_UNSUPPORTED, "Unsupported"),
}


def canonical_sector_for_raw(raw_sector: str | None) -> str:
    return RAW_SECTOR_TO_CANONICAL.get(str(raw_sector or "").strip(), "")


def provider_symbol_parts(provider_symbol: str | None) -> tuple[str, str]:
    raw = str(provider_symbol or "").strip()
    if "." not in raw:
        return raw, ""
    ticker, exchange = raw.rsplit(".", 1)
    return ticker.strip(), exchange.strip()


def csi300_market_benchmark_ticker() -> str:
    return provider_symbol_parts(CSI300_MARKET_BENCHMARK.provider_symbol)[0]


def csi300_market_benchmark_exchange() -> str:
    return provider_symbol_parts(CSI300_MARKET_BENCHMARK.provider_symbol)[1]


def supported_sector_benchmarks() -> list[ChinaBenchmarkDefinition]:
    return [
        definition
        for definition in CSI300_SECTOR_BENCHMARKS.values()
        if definition.status != STATUS_UNSUPPORTED and definition.provider_symbol
    ]


def unsupported_sector_benchmarks() -> list[ChinaBenchmarkDefinition]:
    return [
        definition
        for definition in CSI300_SECTOR_BENCHMARKS.values()
        if definition.status == STATUS_UNSUPPORTED
    ]


def expected_primary_benchmarks() -> list[ChinaBenchmarkDefinition]:
    return [CSI300_MARKET_BENCHMARK, *supported_sector_benchmarks()]
