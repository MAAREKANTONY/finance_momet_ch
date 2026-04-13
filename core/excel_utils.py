from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def normalize_excel_cell(value: Any) -> Any:
    """Convert Python values to Excel-safe scalar values.

    openpyxl cannot write lists/dicts directly to cells. This helper keeps native
    scalars when possible and stringifies complex values in a readable way.
    """
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool, Decimal, date, datetime)):
        return value
    if isinstance(value, (list, tuple, set)):
        items = [normalize_excel_cell(v) for v in value]
        return ", ".join("" if v is None else str(v) for v in items)
    if isinstance(value, Mapping):
        try:
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        except Exception:
            return str(dict(value))
    return str(value)


def normalize_excel_row(values: list[Any] | tuple[Any, ...]) -> list[Any]:
    return [normalize_excel_cell(v) for v in values]


def normalize_excel_mapping_row(row: Mapping[str, Any], headers: list[str]) -> list[Any]:
    return [normalize_excel_cell(row.get(h)) for h in headers]


def append_excel_row(ws, values: list[Any] | tuple[Any, ...]) -> None:
    ws.append(normalize_excel_row(values))
