from __future__ import annotations

from typing import Any, Optional

from django import forms


def normalize_picker_values(raw_values: Any) -> list[str]:
    """Normalize picker payloads into a flat list of string IDs.

    Accepts either:
    - a single CSV string: ``"1,2,3"``
    - repeated field values: ``["1", "2", "3"]``
    - mixed legacy/new payloads: ``["1,2,3", "1", "2", "3"]``

    Returns a de-duplicated list preserving the first-seen order.
    """
    if raw_values in (None, ""):
        return []

    if isinstance(raw_values, str):
        candidates = [raw_values]
    else:
        try:
            candidates = list(raw_values)
        except TypeError:
            candidates = [raw_values]

    cleaned: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if candidate in (None, ""):
            continue
        parts = str(candidate).split(",")
        for part in parts:
            value = part.strip()
            if not value or value in seen:
                continue
            seen.add(value)
            cleaned.append(value)
    return cleaned


class SymbolPickerWidget(forms.Widget):
    """Two-column symbol picker with server-side search.

    Renders a hidden <select multiple> that is the real form field value.
    JS updates that select as user adds/removes symbols.
    """

    template_name = "widgets/symbol_picker.html"

    def __init__(self, attrs: Optional[dict[str, Any]] = None):
        base = {"class": "symbol-picker"}
        if attrs:
            base.update(attrs)
        super().__init__(base)

    def value_from_datadict(self, data, files, name):
        # Accept both the new CSV hidden input and legacy repeated field submissions.
        if hasattr(data, "getlist"):
            return normalize_picker_values(data.getlist(name))
        raw = data.get(name) if hasattr(data, "get") else None
        return normalize_picker_values(raw)

    def format_value(self, value: Any):
        if value is None:
            return []
        # value can be a queryset, list of PKs, etc.
        try:
            return [str(getattr(v, "pk", v)) for v in value]
        except TypeError:
            return [str(value)]
