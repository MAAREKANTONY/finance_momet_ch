from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any


def excel_safe_value(value: Any):
    """Convert Python values to types accepted by openpyxl cells.

    Lists/dicts/sets are serialized to compact JSON to avoid
    ValueError: Cannot convert [...] to Excel.
    """
    if value is None or isinstance(value, (str, int, float, bool, date, datetime, Decimal)):
        return value
    if isinstance(value, tuple):
        value = list(value)
    if isinstance(value, (list, dict, set)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            if isinstance(value, set):
                return ", ".join(sorted(str(v) for v in value))
            return str(value)
    return str(value)


def append_excel_row(ws, values):
    ws.append([excel_safe_value(v) for v in values])
