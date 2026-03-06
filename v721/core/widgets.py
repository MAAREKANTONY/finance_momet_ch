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
        # We use a hidden <select multiple name="..."> so Django will pass a list.
        # But some browsers may submit a single string; normalize to list.
        v = data.getlist(name) if hasattr(data, "getlist") else data.get(name)
        if v is None:
            return []
        if isinstance(v, str):
            return [x for x in v.split(",") if x.strip()]
        return v

    def format_value(self, value: Any):
        if value is None:
            return []
        # value can be a queryset, list of PKs, etc.
        try:
            return [str(getattr(v, "pk", v)) for v in value]
        except TypeError:
            return [str(value)]
