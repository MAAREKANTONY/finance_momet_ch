from __future__ import annotations

from datetime import date
from typing import Any


CSI300_SUPPORTED_HISTORY_START = date(2023, 1, 3)
CSI300_SUPPORTED_HISTORY_START_ISO = CSI300_SUPPORTED_HISTORY_START.isoformat()
CSI300_UNIVERSE_CODE = "CSI300"
CSI300_UNIVERSE_MODE = "CSI300_HISTORICAL_DYNAMIC"
CSI300_SUPPORTED_HISTORY_MESSAGE = (
    "L’historique CSI300 est supporté à partir du 3 janvier 2023, en cohérence "
    "avec la couverture OHLC disponible. Choisissez une date de début égale ou postérieure."
)


class CSI300SupportedHistoryError(ValueError):
    pass


def is_csi300_universe(*, universe_code: Any = "", universe_mode: Any = "") -> bool:
    return (
        str(universe_code or "").strip().upper() == CSI300_UNIVERSE_CODE
        or str(universe_mode or "").strip() == CSI300_UNIVERSE_MODE
    )


def validate_csi300_supported_history_start(
    *,
    start_date: date | None,
    universe_code: Any = "",
    universe_mode: Any = "",
) -> None:
    if (
        start_date is not None
        and is_csi300_universe(universe_code=universe_code, universe_mode=universe_mode)
        and start_date < CSI300_SUPPORTED_HISTORY_START
    ):
        raise CSI300SupportedHistoryError(CSI300_SUPPORTED_HISTORY_MESSAGE)
