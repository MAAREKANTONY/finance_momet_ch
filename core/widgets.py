from __future__ import annotations

import json
from typing import Any, Iterable, Optional

from django import forms


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
        # Hidden input carries a CSV list of selected IDs.
        if hasattr(data, 'getlist'):
            raw_list = data.getlist(name)
            if len(raw_list) > 1:
                out = []
                for raw in raw_list:
                    if raw in (None, ""):
                        continue
                    if isinstance(raw, str):
                        out.extend(x.strip() for x in raw.split(",") if x.strip())
                    else:
                        try:
                            out.extend(str(x).strip() for x in raw if str(x).strip())
                        except TypeError:
                            value = str(raw).strip()
                            if value:
                                out.append(value)
                return out
        raw = data.get(name) if hasattr(data, "get") else None
        if raw in (None, ""):
            return []
        if isinstance(raw, str):
            return [x.strip() for x in raw.split(",") if x.strip()]
        try:
            return [str(x).strip() for x in raw if str(x).strip()]
        except TypeError:
            return []

    def format_value(self, value: Any):
        if value is None:
            return []
        # value can be a queryset, list of PKs, etc.
        try:
            return [str(getattr(v, "pk", v)) for v in value]
        except TypeError:
            return [str(value)]
