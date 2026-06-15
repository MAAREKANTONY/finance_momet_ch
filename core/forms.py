from django import forms
import json
from decimal import Decimal

BACKTEST_SIGNAL_CHOICES = [
    ("A1", "A1 (K1 croise 0 vers le haut)"),
    ("B1", "B1 (K1 croise 0 vers le bas)"),
    ("C1", "C1 (K2 croise 0 vers le haut)"),
    ("D1", "D1 (K2 croise 0 vers le bas)"),
    ("E1", "E1 (K3 croise 0 vers le haut)"),
    ("F1", "F1 (K3 croise 0 vers le bas)"),
    ("G1", "G1 (K4 croise 0 vers le haut)"),
    ("H1", "H1 (K4 croise 0 vers le bas)"),
    ("Af", "Af (P croise Kf de bas en haut)"),
    ("Bf", "Bf (P croise Kf de haut en bas)"),
    ("SPa", "SPa (SUM_SLOPE croise le seuil de pente vers le haut)"),
    ("SPv", "SPv (SUM_SLOPE croise le seuil de pente vers le bas)"),
    ("SPVa", "SPVa (SLOPE_VRAI croise le seuil de pente vers le haut)"),
    ("SPVv", "SPVv (SLOPE_VRAI croise le seuil de pente vers le bas)"),
    ("SPa_basse", "SPa_basse (SUM_SLOPE_BASSE croise le seuil de pente basse vers le haut)"),
    ("SPv_basse", "SPv_basse (SUM_SLOPE_BASSE croise le seuil de pente basse vers le bas)"),
    ("SPVa_basse", "SPVa_basse (SLOPE_VRAI_BASSE croise le seuil de pente basse vers le haut)"),
    ("SPVv_basse", "SPVv_basse (SLOPE_VRAI_BASSE croise le seuil de pente basse vers le bas)"),
    ("RHD_OK", "Signal anti-chute RHD — Repli depuis haut récent OK"),
    ("RHD_FAIL", "Signal anti-chute RHD — Repli depuis haut récent excessif"),
    ("GM_POS", "GM_POS (momentum global positif)"),
    ("GM_NEG", "GM_NEG (momentum global négatif)"),
    ("GM_NEU", "GM_NEU (momentum global neutre)"),
]

GLOBAL_REGIME_FILTER_CHOICES = [
    ("IGNORE", "Ignorer"),
    ("GM_POS", "GM positif"),
    ("GM_NEG", "GM négatif"),
    ("GM_NEU", "GM neutre"),
    ("GM_POS_OR_NEU", "GM positif ou neutre"),
    ("GM_NEG_OR_NEU", "GM négatif ou neutre"),
]

GLOBAL_REGIME_FILTER_CODES = {code for code, _label in GLOBAL_REGIME_FILTER_CHOICES}
SUPPORTED_TREND_GM_CURRENT_CODES = {"GM_POS", "GM_NEG", "GM_NEU", "GM_POS_OR_NEU", "GM_NEG_OR_NEU"}

MARKET_CAP_MIN_KEY = "market_cap_min"
MARKET_CAP_MAX_KEY = "market_cap_max"
MARKET_CAP_MISSING_POLICY_KEY = "market_cap_missing_policy"
MARKET_CAP_POLICY_BLOCK = "BLOCK"
MARKET_CAP_POLICY_ALLOW = "ALLOW"
TREND_FILTER_CHOICES = [
    ("IGNORE", "Ignore"),
    ("GM_POS", "GM positive"),
    ("GM_NEG", "GM negative"),
    ("GM_NEU", "GM neutral"),
]
LEGACY_TREND_FILTER_CHOICES = [
    ("GM_POS_OR_NEU", "GM positive or neutral (legacy)"),
    ("GM_NEG_OR_NEU", "GM negative or neutral (legacy)"),
]
TREND_FILTER_OPERATOR_CHOICES = [
    ("AND", "ALL / AND"),
    ("OR", "ANY / OR"),
]

from .models import EmailRecipient, EmailSettings, Scenario, Symbol, Backtest, AlertDefinition, Universe, Study
from .services.trend_filters import (
    TREND_FILTER_GM_CURRENT_KEY,
    TREND_FILTER_GM_MARKET_KEY,
    TREND_FILTER_GM_SECTOR_KEY,
    TREND_FILTER_OPERATOR_KEY,
    normalize_trend_filter_code,
    normalize_trend_filter_operator,
)
from .trading_model_config import (
    TRADING_MODEL_AUTO_SELL_VALUES,
    TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL,
    resolve_trading_model,
    validate_explicit_latch_config,
    validate_progressive_explicit_sell_config,
)
from .widgets import SymbolPickerWidget


def _configure_slope_threshold_fields(form):
    field_config = {
        "slope_threshold": (
            "Seuil de déclenchement achat",
            "Utilisé par SPa/SPVa.",
        ),
        "slope_sell_threshold": (
            "Seuil de déclenchement vente",
            "Utilisé par SPv/SPVv. Si vide, le seuil d'achat est réutilisé. Le seuil de vente permet de sortir plus tôt ou plus tard que le seuil d'achat.",
        ),
        "slope_threshold_basse": (
            "Seuil de déclenchement achat — pente basse",
            "Utilisé par SPa_basse/SPVa_basse.",
        ),
        "slope_sell_threshold_basse": (
            "Seuil de déclenchement vente — pente basse",
            "Utilisé par SPv_basse/SPVv_basse. Si vide, le seuil d'achat est réutilisé. Le seuil de vente permet de sortir plus tôt ou plus tard que le seuil d'achat.",
        ),
    }
    for field_name, (label, help_text) in field_config.items():
        if field_name in form.fields:
            form.fields[field_name].label = label
            form.fields[field_name].help_text = help_text


def _configure_recent_high_drawdown_fields(form):
    field_config = {
        "recent_high_drawdown_lookback_days": (
            "Fenêtre du plus haut récent",
            "Nombre de jours de cotation précédents utilisés par le signal RHD. Le jour courant est exclu du calcul du plus haut récent.",
        ),
        "recent_high_drawdown_max_drop_pct": (
            "Repli maximal RHD",
            "Repli maximal toléré par rapport au plus haut récent. Exemple : -10 % signifie que RHD_OK est actif tant que le prix du jour reste au-dessus de 90 % du plus haut récent.",
        ),
    }
    for field_name, (label, help_text) in field_config.items():
        if field_name in form.fields:
            form.fields[field_name].label = label
            form.fields[field_name].help_text = help_text




def _normalize_signal_code_list(value):
    if value in (None, ""):
        return []
    if isinstance(value, str):
        code = value.strip()
        return [code] if code else []
    if isinstance(value, (list, tuple)):
        out = []
        for item in value:
            if item in (None, ""):
                continue
            if not isinstance(item, str):
                raise forms.ValidationError("Signal conditions must be strings")
            code = item.strip()
            if code:
                out.append(code)
        return out
    raise forms.ValidationError("Signal conditions must be a string or a JSON list of strings")


def _normalize_logic(value, default):
    logic = str(value or default).strip().upper()
    return logic if logic in {"AND", "OR"} else default


def _normalize_global_regime_filter(value):
    code = str(value or "IGNORE").strip().upper()
    return code if code in GLOBAL_REGIME_FILTER_CODES else "IGNORE"


def _normalize_gm_condition_mode(value):
    code = str(value or "IGNORE").strip().upper()
    if code.startswith("GM_"):
        code = code[3:]
    mapping = {
        "POSITIVE": "POS",
        "POSITIF": "POS",
        "NEGATIVE": "NEG",
        "NEGATIF": "NEG",
        "NEUTRAL": "NEU",
        "NEUTRE": "NEU",
        "POS_OR_NEU": "POS_OR_NEU",
        "NEG_OR_NEU": "NEG_OR_NEU",
    }
    code = mapping.get(code, code)
    return code if code in {"IGNORE", "POS", "NEG", "NEU", "POS_OR_NEU", "NEG_OR_NEU"} else "IGNORE"


def _normalize_gm_condition_entry(raw=None, *, legacy_code=None):
    if isinstance(raw, dict):
        mode = _normalize_gm_condition_mode(raw.get("mode") or raw.get("direction") or raw.get("code"))
        threshold = raw.get("threshold")
        explicit_raw = raw.get("explicit_threshold")
    else:
        mode = _normalize_gm_condition_mode(raw if raw not in (None, "") else legacy_code)
        threshold = None
        explicit_raw = False
    threshold_str = None
    explicit_threshold = bool(explicit_raw) or threshold not in (None, "")
    if threshold not in (None, ""):
        try:
            threshold_str = str(Decimal(str(threshold)))
        except Exception as exc:
            raise forms.ValidationError("Le seuil GM doit être un nombre.") from exc
    if threshold_str is None:
        explicit_threshold = False
    return {
        "mode": mode,
        "threshold": threshold_str,
        "explicit_threshold": bool(explicit_threshold),
    }


def _normalize_gm_conditions_config(raw=None, *, operator=None, current=None, market=None, sector=None):
    payload = raw if isinstance(raw, dict) else {}
    out = {
        "operator": _normalize_logic(payload.get("operator", operator), "AND"),
    }
    legacy = {"current": current, "market": market, "sector": sector}
    for family in ("current", "market", "sector"):
        out[family] = _normalize_gm_condition_entry(payload.get(family), legacy_code=legacy.get(family))
    return out


def _normalize_gm_push_condition_entry(raw=None):
    raw = raw if isinstance(raw, dict) else {}
    mode = _normalize_gm_condition_mode(raw.get("mode") or raw.get("direction") or raw.get("code"))
    normalized_mode = mode if mode in {"IGNORE", "POS", "NEG"} else "IGNORE"
    threshold = raw.get("threshold")
    buy_threshold = raw.get("buy_threshold")
    sell_threshold = raw.get("sell_threshold")

    def _threshold_str(value):
        if value in (None, ""):
            return None
        try:
            return str(Decimal(str(value)))
        except Exception as exc:
            raise forms.ValidationError("Le seuil GM_push doit être un nombre.") from exc

    threshold_dec = Decimal(_threshold_str(threshold)) if _threshold_str(threshold) is not None else None
    buy_threshold_dec = Decimal(_threshold_str(buy_threshold)) if _threshold_str(buy_threshold) is not None else None
    sell_threshold_dec = Decimal(_threshold_str(sell_threshold)) if _threshold_str(sell_threshold) is not None else None
    if threshold_dec is not None:
        buy_threshold_dec = threshold_dec
        sell_threshold_dec = threshold_dec
    elif buy_threshold_dec is not None and sell_threshold_dec is None:
        sell_threshold_dec = buy_threshold_dec
    elif sell_threshold_dec is not None and buy_threshold_dec is None:
        buy_threshold_dec = sell_threshold_dec

    buy_threshold_str = None if buy_threshold_dec is None else str(buy_threshold_dec)
    sell_threshold_str = None if sell_threshold_dec is None else str(sell_threshold_dec)
    threshold_str = None if threshold_dec is None else str(threshold_dec)
    explicit_threshold = bool(raw.get("explicit_threshold")) or any(
        value not in (None, "")
        for value in (raw.get("threshold"), raw.get("buy_threshold"), raw.get("sell_threshold"))
    )
    if buy_threshold_str is None or sell_threshold_str is None:
        explicit_threshold = False
    if normalized_mode in {"POS", "NEG"} and buy_threshold_str is None and sell_threshold_str is None:
        buy_threshold_str = "0"
        sell_threshold_str = "0"
    return {
        "mode": normalized_mode,
        "threshold": threshold_str,
        "buy_threshold": buy_threshold_str,
        "sell_threshold": sell_threshold_str,
        "explicit_threshold": bool(explicit_threshold),
    }


def _normalize_gm_push_conditions_config(raw=None, *, operator=None):
    payload = raw if isinstance(raw, dict) else {}
    out = {
        "operator": _normalize_logic(payload.get("operator", operator), "AND"),
    }
    for family in ("current", "market", "sector"):
        out[family] = _normalize_gm_push_condition_entry(payload.get(family))
    return out


def _gm_conditions_has_active(config):
    if not isinstance(config, dict):
        return False
    return any(
        _normalize_gm_condition_mode((config.get(family) or {}).get("mode")) != "IGNORE"
        for family in ("current", "market", "sector")
    )


def _gm_push_conditions_has_active(config):
    if not isinstance(config, dict):
        return False
    return any(
        _normalize_gm_condition_mode((config.get(family) or {}).get("mode")) in {"POS", "NEG"}
        for family in ("current", "market", "sector")
    )


def _normalize_line_market_conditions(item):
    legacy_current = _normalize_global_regime_filter(item.get("buy_gm_filter"))
    return {
        "buy_market_gm_current": _normalize_global_regime_filter(item.get("buy_market_gm_current", legacy_current)),
        "buy_market_gm_market": _normalize_global_regime_filter(item.get("buy_market_gm_market")),
        "buy_market_gm_sector": _normalize_global_regime_filter(item.get("buy_market_gm_sector")),
        "buy_market_operator": _normalize_logic(item.get("buy_market_operator"), "AND"),
    }


def _clean_signal_lines_json(value):
    if value in (None, ""):
        return []
    if not isinstance(value, list):
        raise forms.ValidationError("signal_lines must be a JSON list")
    cleaned = []
    for item in value:
        if not isinstance(item, dict):
            raise forms.ValidationError("Each signal line must be an object")
        mode = str(item.get("mode") or "standard").strip() or "standard"
        buy = _normalize_signal_code_list(item.get("buy") or item.get("buy_conditions"))
        sell = _normalize_signal_code_list(item.get("sell") or item.get("sell_conditions"))
        try:
            trading_model, explicit_trading_model = resolve_trading_model(item.get("trading_model"), buy)
        except ValueError as exc:
            raise forms.ValidationError(str(exc)) from exc
        payload = {
            "mode": mode,
            "trading_model": trading_model,
            "buy": buy,
            "sell": sell,
            "buy_logic": _normalize_logic(item.get("buy_logic"), "AND"),
            "sell_logic": _normalize_logic(item.get("sell_logic"), "OR"),
            "buy_gm_filter": _normalize_global_regime_filter(item.get("buy_gm_filter")),
            "buy_gm_operator": _normalize_logic(item.get("buy_gm_operator"), "AND"),
            "sell_gm_filter": _normalize_global_regime_filter(item.get("sell_gm_filter")),
            "sell_gm_operator": _normalize_logic(item.get("sell_gm_operator"), "AND"),
        }
        payload.update(_normalize_line_market_conditions(item))
        payload["gm_buy_conditions"] = _normalize_gm_conditions_config(
            item.get("gm_buy_conditions"),
            operator=payload["buy_market_operator"],
            current=payload["buy_market_gm_current"],
            market=payload["buy_market_gm_market"],
            sector=payload["buy_market_gm_sector"],
        )
        payload["gm_sell_market_exit_conditions"] = _normalize_gm_conditions_config(
            item.get("gm_sell_market_exit_conditions"),
            operator=item.get("gm_sell_market_exit_operator"),
        )
        payload["gm_push_buy_conditions"] = _normalize_gm_push_conditions_config(item.get("gm_push_buy_conditions"))
        payload["gm_push_sell_market_exit_conditions"] = _normalize_gm_push_conditions_config(
            item.get("gm_push_sell_market_exit_conditions"),
            operator=item.get("gm_push_sell_market_exit_operator"),
        )
        has_line_market_conditions = any(
            payload[key] != "IGNORE"
            for key in ("buy_market_gm_current", "buy_market_gm_market", "buy_market_gm_sector")
        )
        has_gm_buy_conditions = _gm_conditions_has_active(payload["gm_buy_conditions"])
        has_gm_sell_market_exit = _gm_conditions_has_active(payload["gm_sell_market_exit_conditions"])
        has_gm_push_buy_conditions = _gm_push_conditions_has_active(payload["gm_push_buy_conditions"])
        has_gm_push_sell_market_exit = _gm_push_conditions_has_active(payload["gm_push_sell_market_exit_conditions"])
        if (has_line_market_conditions or has_gm_buy_conditions or has_gm_push_buy_conditions) and not buy:
            raise forms.ValidationError(
                "Chaque ligne avec des conditions de marché doit contenir au moins un signal BUY."
            )
        if explicit_trading_model and trading_model in TRADING_MODEL_AUTO_SELL_VALUES:
            try:
                validate_explicit_latch_config(
                    buy_codes=buy,
                    buy_logic=payload["buy_logic"],
                    sell_codes=sell,
                    sell_gm_filter=payload["sell_gm_filter"],
                )
            except ValueError as exc:
                raise forms.ValidationError(str(exc)) from exc
        if explicit_trading_model and trading_model == TRADING_MODEL_PROGRESSIVE_EXPLICIT_SELL:
            try:
                validate_progressive_explicit_sell_config(
                    buy_codes=buy,
                    buy_logic=payload["buy_logic"],
                    sell_codes=sell,
                    sell_gm_filter=payload["sell_gm_filter"],
                    has_gm_sell_market_exit=has_gm_sell_market_exit or has_gm_push_sell_market_exit,
                )
            except ValueError as exc:
                raise forms.ValidationError(str(exc)) from exc
        if (
            buy
            or sell
            or payload["buy_gm_filter"] != "IGNORE"
            or payload["sell_gm_filter"] != "IGNORE"
            or has_line_market_conditions
            or has_gm_buy_conditions
            or has_gm_sell_market_exit
            or has_gm_push_buy_conditions
            or has_gm_push_sell_market_exit
        ):
            cleaned.append(payload)
    return cleaned


def _normalize_legacy_gm_for_trend_filters(signal_lines, trend_filter_gm_current):
    normalized_current = normalize_trend_filter_code(trend_filter_gm_current)
    normalized_lines = []
    for item in signal_lines or []:
        if not isinstance(item, dict):
            continue
        payload = dict(item)
        legacy_buy = _normalize_global_regime_filter(payload.get("buy_gm_filter"))
        if normalized_current == "IGNORE" and legacy_buy in SUPPORTED_TREND_GM_CURRENT_CODES:
            normalized_current = legacy_buy
        payload["buy_gm_filter"] = "IGNORE"
        payload["buy_gm_operator"] = "AND"
        payload["sell_gm_filter"] = "IGNORE"
        payload["sell_gm_operator"] = "AND"
        line_current = _normalize_global_regime_filter(payload.get("buy_market_gm_current"))
        if line_current == "IGNORE" and legacy_buy in SUPPORTED_TREND_GM_CURRENT_CODES:
            line_current = legacy_buy
        payload["buy_market_gm_current"] = line_current
        payload.setdefault("buy_market_gm_market", "IGNORE")
        payload.setdefault("buy_market_gm_sector", "IGNORE")
        payload.setdefault("buy_market_operator", "AND")
        payload.setdefault("gm_buy_conditions", _normalize_gm_conditions_config(
            operator=payload.get("buy_market_operator"),
            current=payload.get("buy_market_gm_current"),
            market=payload.get("buy_market_gm_market"),
            sector=payload.get("buy_market_gm_sector"),
        ))
        payload.setdefault("gm_sell_market_exit_conditions", _normalize_gm_conditions_config())
        payload.setdefault("gm_push_buy_conditions", _normalize_gm_push_conditions_config())
        payload.setdefault("gm_push_sell_market_exit_conditions", _normalize_gm_push_conditions_config())
        normalized_lines.append(payload)
    return normalized_lines, normalized_current


def _ensure_legacy_trend_choice(field, value):
    code = str(value or "").strip().upper()
    if code not in {choice[0] for choice in LEGACY_TREND_FILTER_CHOICES}:
        return
    existing = {choice[0] for choice in field.choices}
    if code not in existing:
        field.choices = list(field.choices) + [choice for choice in LEGACY_TREND_FILTER_CHOICES if choice[0] == code]


def _trend_filter_fields_were_submitted(form) -> bool:
    if not getattr(form, "is_bound", False):
        return False
    return any(
        form.add_prefix(field_name) in form.data
        for field_name in (
            "trend_filter_operator",
            "trend_filter_gm_current",
            "trend_filter_gm_market",
            "trend_filter_gm_sector",
        )
    )


class AlertDefinitionForm(forms.ModelForm):
    """CRUD form for user-defined alert definitions.

    Stored alert codes are comma-separated in the DB for portability.
    The UI uses the same BACKTEST_SIGNAL_CHOICES list as backtests.
    """

    scenarios = forms.ModelMultipleChoiceField(
        queryset=Scenario.objects.filter(active=True).order_by("name"),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 8}),
        help_text="Scénarios ciblés par cette alerte (laisser vide = tous).",
    )
    alert_codes_multi = forms.MultipleChoiceField(
        choices=BACKTEST_SIGNAL_CHOICES,
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 10, "style": "min-width:260px;"}),
        help_text="Codes d'alerte à inclure (laisser vide = tous).",
        label="Lignes (codes)",
    )
    recipients = forms.ModelMultipleChoiceField(
        queryset=EmailRecipient.objects.filter(active=True).order_by("email"),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 6}),
        help_text="Destinataires (laisser vide = aucun envoi).",
    )

    class Meta:
        model = AlertDefinition
        fields = [
            "name",
            "description",
            "scenarios",
            "recipients",
            "send_hour",
            "send_minute",
            "timezone",
            "is_active",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "send_hour": forms.NumberInput(attrs={"min": 0, "max": 23}),
            "send_minute": forms.NumberInput(attrs={"min": 0, "max": 59}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.pk:
            self.fields["alert_codes_multi"].initial = self.instance.get_codes_list()

    def save(self, commit=True):
        obj: AlertDefinition = super().save(commit=False)
        codes = self.cleaned_data.get("alert_codes_multi") or []
        obj.alert_codes = ",".join(codes)
        if commit:
            obj.save()
            self.save_m2m()
        return obj



def _symbol_picker_payload(symbols):
    return json.dumps([
        {
            "id": s.id,
            "ticker": s.ticker,
            "name": s.name or "",
            "exchange": s.exchange or "",
            "sector": getattr(s, "sector", "") or "",
            "country": getattr(s, "country", "") or "",
        }
        for s in symbols
    ])


def _configure_symbol_picker(field, selected_symbols):
    field.widget = SymbolPickerWidget(attrs={
        "data_search_url": "/symbols/search/",
        "data_selected_json": _symbol_picker_payload(selected_symbols),
    })

class ScenarioForm(forms.ModelForm):
    symbols = forms.ModelMultipleChoiceField(
        queryset=Symbol.objects.none(),
        required=False,
        widget=SymbolPickerWidget(),
        help_text="Tickers associés à ce scénario (en plus du scénario par défaut si activé).",
    )

    class Meta:
        model = Scenario
        fields = [
            "name",
            "description",
            "is_default",
            "universe_mode",
            "a",
            "b",
            "c",
            "d",
            "e",
            "n1",
            "n2",
            "npente",
            "slope_threshold",
            "slope_sell_threshold",
            "npente_basse",
            "slope_threshold_basse",
            "slope_sell_threshold_basse",
            "recent_high_drawdown_lookback_days",
            "recent_high_drawdown_max_drop_pct",
            "nglobal",
            "history_years",
            "active",
            "symbols",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "e": forms.NumberInput(attrs={"min": 0.0001, "step": 0.0001}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["universe_mode"].label = "Mode d’univers"
        self.fields["universe_mode"].required = False
        self.fields["universe_mode"].initial = Scenario.UniverseMode.STATIC_TICKERS
        self.fields["universe_mode"].help_text = (
            "La sélection statique utilise les tickers choisis dans le scénario. "
            "Le mode S&P500 historique dynamique détermine automatiquement les actions à partir de l’historique du S&P 500 pour les backtests."
        )
        self.fields["symbols"].queryset = Symbol.objects.filter(active=True).order_by("ticker", "exchange")
        # Make the option label searchable (ticker + exchange + name)
        def _label(sym: Symbol) -> str:
            base = f"{sym.ticker}{(':'+sym.exchange) if sym.exchange else ''}"
            extras = []
            if sym.name:
                extras.append(sym.name)
            if getattr(sym, "sector", ""):
                extras.append(f"Secteur: {sym.sector}")
            return f"{base} — {' | '.join(extras)}" if extras else base

        self.fields["symbols"].label_from_instance = _label
        selected_symbols = list(self.instance.symbols.all()) if self.instance.pk else []
        if not selected_symbols:
            initial_symbols = self.initial.get("symbols") if hasattr(self, "initial") else None
            if initial_symbols:
                try:
                    selected_symbols = list(initial_symbols)
                except TypeError:
                    selected_symbols = [initial_symbols]
        if selected_symbols:
            self.fields["symbols"].initial = selected_symbols
        _configure_symbol_picker(self.fields["symbols"], selected_symbols)
        _configure_slope_threshold_fields(self)
        _configure_recent_high_drawdown_fields(self)

    def clean_universe_mode(self):
        return self.cleaned_data.get("universe_mode") or Scenario.UniverseMode.STATIC_TICKERS


class UniverseForm(forms.ModelForm):
    """ModelForm compatible with slightly different Universe schemas across versions.

    We render all available fields and drop technical timestamps/user fields if present.
    """

    class Meta:
        model = Universe
        fields = "__all__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Remove technical fields if they exist on the model
        for fname in ["created_by", "created_at", "updated_at"]:
            if fname in self.fields:
                self.fields.pop(fname)

        # Nice widgets
        if "description" in self.fields:
            self.fields["description"].required = False
            self.fields["description"].widget.attrs.setdefault("rows", 3)
            self.fields["description"].widget.attrs.setdefault("style", "width:100%;")

        # Ensure symbols selection stays usable with hundreds/thousands of tickers.
        if "symbols" in self.fields:
            self.fields["symbols"].required = False
            self.fields["symbols"].queryset = Symbol.objects.filter(active=True).order_by("ticker", "exchange")

            def _label(sym: Symbol) -> str:
                base = f"{sym.ticker}{(':'+sym.exchange) if sym.exchange else ''}"
                extras = []
                if sym.name:
                    extras.append(sym.name)
                if getattr(sym, "sector", ""):
                    extras.append(f"Secteur: {sym.sector}")
                return f"{base} — {' | '.join(extras)}" if extras else base

            self.fields["symbols"].label_from_instance = _label
            selected_symbols = list(self.instance.symbols.all()) if self.instance.pk else []
            if not selected_symbols:
                initial_symbols = self.initial.get("symbols") if hasattr(self, "initial") else None
                if initial_symbols:
                    try:
                        selected_symbols = list(initial_symbols)
                    except TypeError:
                        selected_symbols = [initial_symbols]
            if selected_symbols:
                self.fields["symbols"].initial = selected_symbols
            _configure_symbol_picker(self.fields["symbols"], selected_symbols)


class StudyCreateForm(forms.Form):
    """Create a Study by cloning an existing Scenario or starting from scratch.

    Sprint 1: only Scenario clone is created and attached.
    """

    name = forms.CharField(max_length=120)
    description = forms.CharField(required=False, widget=forms.Textarea(attrs={"rows": 3}))

    source_scenario = forms.ModelChoiceField(
        queryset=Scenario.objects.filter(active=True, is_study_clone=False).order_by("name"),
        required=False,
        help_text="Optionnel : importer les paramètres + tickers depuis un scénario existant.",
    )
    universe = forms.ModelChoiceField(
        queryset=Universe.objects.all().order_by("name"),
        required=False,
        help_text="Optionnel : ajouter un groupe d'actions (univers).",
    )

    universe_mode = forms.ChoiceField(
        choices=[("add", "Ajouter"), ("replace", "Remplacer")],
        required=False,
        initial="add",
        help_text="Si un univers est choisi : ajouter à la liste (Add) ou remplacer complètement (Replace).",
    )

    create_alert = forms.BooleanField(
        required=False,
        initial=True,
        label="Créer une configuration d'alertes",
        help_text="Crée une AlertDefinition dédiée à cette Study (indépendante).",
    )
    create_backtest = forms.BooleanField(
        required=False,
        initial=True,
        label="Créer une configuration de backtest",
        help_text="Crée un Backtest dédié à cette Study (indépendant).",
    )


class StudyAlertDefinitionForm(AlertDefinitionForm):
    """Edit the AlertDefinition attached to a Study.

    The Study owns a single Scenario clone, so we hide the 'scenarios' selector and force it.
    """

    class Meta(AlertDefinitionForm.Meta):
        fields = ["name", "description", "recipients", "send_hour", "send_minute", "timezone", "is_active"]

    def __init__(self, *args, study_scenario: Scenario, **kwargs):
        self._study_scenario = study_scenario
        super().__init__(*args, **kwargs)
        # Remove scenarios field from parent form if present
        if "scenarios" in self.fields:
            self.fields.pop("scenarios")

    def save(self, commit=True):
        # AlertDefinitionForm.save already handles alert_codes + recipients M2M.
        obj: AlertDefinition = super().save(commit=commit)
        # Force scenario membership (requires PK). If commit=False, caller must save & call this again.
        if obj.pk:
            obj.scenarios.set([self._study_scenario])
        return obj


class StudyBacktestForm(forms.ModelForm):
    """Edit the Backtest attached to a Study (scenario is fixed)."""

    class Meta:
        model = Backtest
        fields = [
            "name",
            "description",
            "start_date",
            "end_date",
            "capital_total",
            "capital_per_ticker",
            "ratio_threshold",
            "include_all_tickers",
            "signal_lines",
            "warmup_days",
            "close_positions_at_end",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "start_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
            "end_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
        }

    def clean_signal_lines(self):
        return _clean_signal_lines_json(self.cleaned_data.get("signal_lines"))


class StudyMetaForm(forms.ModelForm):
    class Meta:
        model = Study
        fields = ["name", "description"]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
        }


class StudyScenarioForm(forms.ModelForm):
    """Edit the Scenario attached to a Study.

    We intentionally hide Scenario naming/default flags; Study owns the user-facing name.
    """

    symbols = forms.ModelMultipleChoiceField(
        queryset=Symbol.objects.none(),
        required=False,
        widget=SymbolPickerWidget(),
        label="Tickers",
        help_text="Tickers associés à cette Study.",
    )

    class Meta:
        model = Scenario
        fields = [
            "a",
            "b",
            "c",
            "d",
            "e",
            "n1",
            "n2",
            "npente",
            "slope_threshold",
            "slope_sell_threshold",
            "npente_basse",
            "slope_threshold_basse",
            "slope_sell_threshold_basse",
            "recent_high_drawdown_lookback_days",
            "recent_high_drawdown_max_drop_pct",
            "nglobal",
            "history_years",
            "symbols",
        ]
        widgets = {
            "e": forms.NumberInput(attrs={"min": 0.0001, "step": 0.0001}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["symbols"].queryset = Symbol.objects.filter(active=True).order_by("ticker", "exchange")

        def _label(sym: Symbol) -> str:
            base = f"{sym.ticker}{(':'+sym.exchange) if sym.exchange else ''}"
            extras = []
            if sym.name:
                extras.append(sym.name)
            if getattr(sym, "sector", ""):
                extras.append(f"Secteur: {sym.sector}")
            return f"{base} — {' | '.join(extras)}" if extras else base

        self.fields["symbols"].label_from_instance = _label
        selected_symbols = list(self.instance.symbols.all()) if self.instance.pk else []
        if selected_symbols:
            self.fields["symbols"].initial = selected_symbols
        _configure_symbol_picker(self.fields["symbols"], selected_symbols)
        _configure_slope_threshold_fields(self)
        _configure_recent_high_drawdown_fields(self)


class EmailRecipientForm(forms.ModelForm):
    class Meta:
        model = EmailRecipient
        fields = ["email", "active"]

class SymbolManualForm(forms.ModelForm):
    scenarios = forms.ModelMultipleChoiceField(
        queryset=Scenario.objects.none(),
        required=False,
        widget=SymbolPickerWidget(),
        help_text="Scénarios associés à ce ticker (le scénario par défaut sera ajouté automatiquement).",
    )

    class Meta:
        model = Symbol
        fields = ["ticker", "exchange", "name", "instrument_type", "country", "currency", "sector", "active", "scenarios"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["scenarios"].queryset = Scenario.objects.filter(active=True).order_by("name")
        if self.instance.pk:
            self.fields["scenarios"].initial = self.instance.scenarios.all()


class EmailSettingsForm(forms.ModelForm):
    class Meta:
        model = EmailSettings
        fields = ["send_hour", "send_minute", "timezone"]
        widgets = {
            "send_hour": forms.NumberInput(attrs={"min": 0, "max": 23}),
            "send_minute": forms.NumberInput(attrs={"min": 0, "max": 59}),
        }


class SymbolScenariosForm(forms.Form):
    """Assign one or many scenarios to an existing ticker."""

    scenarios = forms.ModelMultipleChoiceField(
        queryset=Scenario.objects.filter(active=True),
        required=False,
        widget=SymbolPickerWidget(),
        label="Scénarios",
    )


class SymbolImportForm(forms.Form):
    """Import tickers from CSV/XLSX."""

    file = forms.FileField(label="Fichier (CSV ou Excel .xlsx)")


class BacktestForm(forms.ModelForm):
    """Create/Edit a Backtest configuration (engine results will be computed later)."""

    min_price = forms.DecimalField(required=False, min_value=0, label="Prix minimum")
    max_price = forms.DecimalField(required=False, min_value=0, label="Prix maximum")
    market_cap_min = forms.DecimalField(
        required=False,
        min_value=0,
        label="Min Market Cap",
        help_text="Minimum historical company market capitalization required to allow BUY.",
    )
    market_cap_max = forms.DecimalField(
        required=False,
        min_value=0,
        label="Max Market Cap",
        help_text="Maximum historical company market capitalization allowed for BUY.",
    )
    market_cap_missing_policy = forms.ChoiceField(
        required=False,
        choices=[
            (MARKET_CAP_POLICY_BLOCK, "Block BUY (recommended)"),
            (MARKET_CAP_POLICY_ALLOW, "Allow BUY"),
        ],
        initial=MARKET_CAP_POLICY_BLOCK,
        label="If Market Cap Missing",
        help_text="What to do when no historical market capitalization exists at or before the BUY date.",
    )
    trend_filter_operator = forms.ChoiceField(
        required=False,
        choices=TREND_FILTER_OPERATOR_CHOICES,
        initial="AND",
        label="Combiner les filtres de tendance avec",
    )
    trend_filter_gm_current = forms.ChoiceField(required=False, choices=TREND_FILTER_CHOICES, initial="IGNORE", label="GM current")
    trend_filter_gm_market = forms.ChoiceField(required=False, choices=TREND_FILTER_CHOICES, initial="IGNORE", label="GM_market")
    trend_filter_gm_sector = forms.ChoiceField(required=False, choices=TREND_FILTER_CHOICES, initial="IGNORE", label="GM_sector")

    class Meta:
        model = Backtest
        fields = [
            "name",
            "description",
            "scenario",
            "start_date",
            "end_date",
            "capital_total",
            "capital_per_ticker",
            "capital_mode",
            "ratio_threshold",
            "include_all_tickers",
            "min_price",
            "max_price",
            "market_cap_min",
            "market_cap_max",
            "market_cap_missing_policy",
            "trend_filter_operator",
            "trend_filter_gm_current",
            "trend_filter_gm_market",
            "trend_filter_gm_sector",
            "signal_lines",
            "warmup_days",
            "close_positions_at_end",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "start_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
            "end_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        settings = getattr(self.instance, "settings", None) or {}
        if isinstance(settings, dict):
            self.fields["min_price"].initial = settings.get("min_price")
            self.fields["max_price"].initial = settings.get("max_price")
            self.fields["market_cap_min"].initial = settings.get(MARKET_CAP_MIN_KEY)
            self.fields["market_cap_max"].initial = settings.get(MARKET_CAP_MAX_KEY)
            self.fields["market_cap_missing_policy"].initial = (
                settings.get(MARKET_CAP_MISSING_POLICY_KEY) or MARKET_CAP_POLICY_BLOCK
            )
            self.fields["trend_filter_operator"].initial = normalize_trend_filter_operator(settings.get(TREND_FILTER_OPERATOR_KEY))
            self.fields["trend_filter_gm_current"].initial = normalize_trend_filter_code(settings.get(TREND_FILTER_GM_CURRENT_KEY))
            self.fields["trend_filter_gm_market"].initial = normalize_trend_filter_code(settings.get(TREND_FILTER_GM_MARKET_KEY))
            self.fields["trend_filter_gm_sector"].initial = normalize_trend_filter_code(settings.get(TREND_FILTER_GM_SECTOR_KEY))
        if not self.is_bound:
            initial_signal_lines = self.initial.get("signal_lines", getattr(self.instance, "signal_lines", None) or [])
            normalized_lines, normalized_current = _normalize_legacy_gm_for_trend_filters(
                initial_signal_lines,
                self.fields["trend_filter_gm_current"].initial,
            )
            self.initial["signal_lines"] = normalized_lines
            self.fields["signal_lines"].initial = normalized_lines
            self.fields["trend_filter_gm_current"].initial = normalized_current
        current_trend_choice = (
            self.data.get(self.add_prefix("trend_filter_gm_current"))
            if self.is_bound
            else self.fields["trend_filter_gm_current"].initial
        )
        _ensure_legacy_trend_choice(self.fields["trend_filter_gm_current"], current_trend_choice)

    def clean_signal_lines(self):
        return _clean_signal_lines_json(self.cleaned_data.get("signal_lines"))

    def clean(self):
        cleaned = super().clean()
        min_price = cleaned.get("min_price")
        max_price = cleaned.get("max_price")
        if min_price is not None and max_price is not None and min_price > max_price:
            self.add_error("max_price", "Le prix maximum doit être supérieur ou égal au prix minimum.")
        market_cap_min = cleaned.get("market_cap_min")
        market_cap_max = cleaned.get("market_cap_max")
        if market_cap_min is not None and market_cap_max is not None and market_cap_min > market_cap_max:
            self.add_error("market_cap_max", "Max Market Cap must be greater than or equal to Min Market Cap.")
        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        settings = dict(obj.settings or {})
        min_price = self.cleaned_data.get("min_price")
        max_price = self.cleaned_data.get("max_price")
        market_cap_min = self.cleaned_data.get("market_cap_min")
        market_cap_max = self.cleaned_data.get("market_cap_max")
        market_cap_missing_policy = self.cleaned_data.get("market_cap_missing_policy") or MARKET_CAP_POLICY_BLOCK
        trend_filter_operator = normalize_trend_filter_operator(self.cleaned_data.get("trend_filter_operator"))
        trend_filter_gm_current = normalize_trend_filter_code(self.cleaned_data.get("trend_filter_gm_current"))
        trend_filter_gm_market = normalize_trend_filter_code(self.cleaned_data.get("trend_filter_gm_market"))
        trend_filter_gm_sector = normalize_trend_filter_code(self.cleaned_data.get("trend_filter_gm_sector"))
        normalized_signal_lines, trend_filter_gm_current = _normalize_legacy_gm_for_trend_filters(
            self.cleaned_data.get("signal_lines") or [],
            trend_filter_gm_current,
        )
        obj.signal_lines = normalized_signal_lines
        if min_price is None:
            settings.pop("min_price", None)
        else:
            settings["min_price"] = str(min_price)
        if max_price is None:
            settings.pop("max_price", None)
        else:
            settings["max_price"] = str(max_price)
        if market_cap_min is None:
            settings.pop(MARKET_CAP_MIN_KEY, None)
        else:
            settings[MARKET_CAP_MIN_KEY] = str(market_cap_min)
        if market_cap_max is None:
            settings.pop(MARKET_CAP_MAX_KEY, None)
        else:
            settings[MARKET_CAP_MAX_KEY] = str(market_cap_max)
        if market_cap_min is None and market_cap_max is None:
            settings.pop(MARKET_CAP_MISSING_POLICY_KEY, None)
        else:
            settings[MARKET_CAP_MISSING_POLICY_KEY] = market_cap_missing_policy
        if _trend_filter_fields_were_submitted(self):
            if all(code == "IGNORE" for code in (trend_filter_gm_current, trend_filter_gm_market, trend_filter_gm_sector)):
                settings.pop(TREND_FILTER_OPERATOR_KEY, None)
                settings.pop(TREND_FILTER_GM_CURRENT_KEY, None)
                settings.pop(TREND_FILTER_GM_MARKET_KEY, None)
                settings.pop(TREND_FILTER_GM_SECTOR_KEY, None)
            else:
                settings[TREND_FILTER_OPERATOR_KEY] = trend_filter_operator
                if trend_filter_gm_current == "IGNORE":
                    settings.pop(TREND_FILTER_GM_CURRENT_KEY, None)
                else:
                    settings[TREND_FILTER_GM_CURRENT_KEY] = trend_filter_gm_current
                if trend_filter_gm_market == "IGNORE":
                    settings.pop(TREND_FILTER_GM_MARKET_KEY, None)
                else:
                    settings[TREND_FILTER_GM_MARKET_KEY] = trend_filter_gm_market
                if trend_filter_gm_sector == "IGNORE":
                    settings.pop(TREND_FILTER_GM_SECTOR_KEY, None)
                else:
                    settings[TREND_FILTER_GM_SECTOR_KEY] = trend_filter_gm_sector
        obj.settings = settings
        if commit:
            obj.save()
            self.save_m2m()
        return obj


class GameScenarioForm(forms.ModelForm):
    """CRUD form for GameScenario."""

    min_price = forms.DecimalField(
        required=False,
        min_value=0,
        label="Prix minimum",
        help_text="Une action ne pourra etre achetee que si son prix du jour est compris dans cette plage.",
    )
    max_price = forms.DecimalField(
        required=False,
        min_value=0,
        label="Prix maximum",
        help_text="Ce filtre s'applique uniquement a l'achat. La vente reste toujours possible.",
    )
    market_cap_min = forms.DecimalField(
        required=False,
        min_value=0,
        label="Min Market Cap",
        help_text="Minimum historical company market capitalization required to allow BUY.",
    )
    market_cap_max = forms.DecimalField(
        required=False,
        min_value=0,
        label="Max Market Cap",
        help_text="Maximum historical company market capitalization allowed for BUY.",
    )
    market_cap_missing_policy = forms.ChoiceField(
        required=False,
        choices=[
            (MARKET_CAP_POLICY_BLOCK, "Block BUY (recommended)"),
            (MARKET_CAP_POLICY_ALLOW, "Allow BUY"),
        ],
        initial=MARKET_CAP_POLICY_BLOCK,
        label="If Market Cap Missing",
        help_text="What to do when no historical market capitalization exists at or before the BUY date.",
    )
    trend_filter_operator = forms.ChoiceField(
        required=False,
        choices=TREND_FILTER_OPERATOR_CHOICES,
        initial="AND",
        label="Combiner les filtres de tendance avec",
    )
    trend_filter_gm_current = forms.ChoiceField(required=False, choices=TREND_FILTER_CHOICES, initial="IGNORE", label="GM current")
    trend_filter_gm_market = forms.ChoiceField(required=False, choices=TREND_FILTER_CHOICES, initial="IGNORE", label="GM_market")
    trend_filter_gm_sector = forms.ChoiceField(required=False, choices=TREND_FILTER_CHOICES, initial="IGNORE", label="GM_sector")

    class Meta:
        from .models import GameScenario

        model = GameScenario
        fields = [
            "name",
            "description",
            "active",
            "study_days",
            "tradability_threshold",
            "npente",
            "slope_threshold",
            "slope_sell_threshold",
            "npente_basse",
            "slope_threshold_basse",
            "slope_sell_threshold_basse",
            "recent_high_drawdown_lookback_days",
            "recent_high_drawdown_max_drop_pct",
            "nglobal",
            "presence_threshold_pct",
            "email_recipients",
            "a",
            "b",
            "c",
            "d",
            "e",
            "n1",
            "n2",
            "capital_total",
            "capital_per_ticker",
            "capital_mode",
            "min_price",
            "max_price",
            "market_cap_min",
            "market_cap_max",
            "market_cap_missing_policy",
            "trend_filter_operator",
            "trend_filter_gm_current",
            "trend_filter_gm_market",
            "trend_filter_gm_sector",
            "signal_lines",
            "warmup_days",
            "close_positions_at_end",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "email_recipients": forms.Textarea(
                attrs={
                    "rows": 3,
                    "placeholder": "Emails séparés par virgule, point-virgule ou retour ligne",
                }
            ),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        settings = getattr(self.instance, "settings", None) or {}
        if isinstance(settings, dict):
            self.fields["min_price"].initial = settings.get("min_price")
            self.fields["max_price"].initial = settings.get("max_price")
            self.fields["market_cap_min"].initial = settings.get(MARKET_CAP_MIN_KEY)
            self.fields["market_cap_max"].initial = settings.get(MARKET_CAP_MAX_KEY)
            self.fields["market_cap_missing_policy"].initial = (
                settings.get(MARKET_CAP_MISSING_POLICY_KEY) or MARKET_CAP_POLICY_BLOCK
            )
            self.fields["trend_filter_operator"].initial = normalize_trend_filter_operator(settings.get(TREND_FILTER_OPERATOR_KEY))
            self.fields["trend_filter_gm_current"].initial = normalize_trend_filter_code(settings.get(TREND_FILTER_GM_CURRENT_KEY))
            self.fields["trend_filter_gm_market"].initial = normalize_trend_filter_code(settings.get(TREND_FILTER_GM_MARKET_KEY))
            self.fields["trend_filter_gm_sector"].initial = normalize_trend_filter_code(settings.get(TREND_FILTER_GM_SECTOR_KEY))
        if not self.is_bound:
            initial_signal_lines = self.initial.get("signal_lines", getattr(self.instance, "signal_lines", None) or [])
            normalized_lines, normalized_current = _normalize_legacy_gm_for_trend_filters(
                initial_signal_lines,
                self.fields["trend_filter_gm_current"].initial,
            )
            self.initial["signal_lines"] = normalized_lines
            self.fields["signal_lines"].initial = normalized_lines
            self.fields["trend_filter_gm_current"].initial = normalized_current
        current_trend_choice = (
            self.data.get(self.add_prefix("trend_filter_gm_current"))
            if self.is_bound
            else self.fields["trend_filter_gm_current"].initial
        )
        _ensure_legacy_trend_choice(self.fields["trend_filter_gm_current"], current_trend_choice)
        _configure_slope_threshold_fields(self)
        _configure_recent_high_drawdown_fields(self)

    def clean_signal_lines(self):
        return _clean_signal_lines_json(self.cleaned_data.get("signal_lines"))

    def clean(self):
        cleaned = super().clean()
        min_price = cleaned.get("min_price")
        max_price = cleaned.get("max_price")
        if min_price is not None and max_price is not None and min_price > max_price:
            self.add_error("max_price", "Le prix maximum doit être supérieur ou égal au prix minimum.")
        market_cap_min = cleaned.get("market_cap_min")
        market_cap_max = cleaned.get("market_cap_max")
        if market_cap_min is not None and market_cap_max is not None and market_cap_min > market_cap_max:
            self.add_error("market_cap_max", "Max Market Cap must be greater than or equal to Min Market Cap.")
        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        settings = dict(obj.settings or {})
        min_price = self.cleaned_data.get("min_price")
        max_price = self.cleaned_data.get("max_price")
        market_cap_min = self.cleaned_data.get("market_cap_min")
        market_cap_max = self.cleaned_data.get("market_cap_max")
        market_cap_missing_policy = self.cleaned_data.get("market_cap_missing_policy") or MARKET_CAP_POLICY_BLOCK
        trend_filter_operator = normalize_trend_filter_operator(self.cleaned_data.get("trend_filter_operator"))
        trend_filter_gm_current = normalize_trend_filter_code(self.cleaned_data.get("trend_filter_gm_current"))
        trend_filter_gm_market = normalize_trend_filter_code(self.cleaned_data.get("trend_filter_gm_market"))
        trend_filter_gm_sector = normalize_trend_filter_code(self.cleaned_data.get("trend_filter_gm_sector"))
        normalized_signal_lines, trend_filter_gm_current = _normalize_legacy_gm_for_trend_filters(
            self.cleaned_data.get("signal_lines") or [],
            trend_filter_gm_current,
        )
        obj.signal_lines = normalized_signal_lines
        if min_price is None:
            settings.pop("min_price", None)
        else:
            settings["min_price"] = str(min_price)
        if max_price is None:
            settings.pop("max_price", None)
        else:
            settings["max_price"] = str(max_price)
        if market_cap_min is None:
            settings.pop(MARKET_CAP_MIN_KEY, None)
        else:
            settings[MARKET_CAP_MIN_KEY] = str(market_cap_min)
        if market_cap_max is None:
            settings.pop(MARKET_CAP_MAX_KEY, None)
        else:
            settings[MARKET_CAP_MAX_KEY] = str(market_cap_max)
        if market_cap_min is None and market_cap_max is None:
            settings.pop(MARKET_CAP_MISSING_POLICY_KEY, None)
        else:
            settings[MARKET_CAP_MISSING_POLICY_KEY] = market_cap_missing_policy
        if _trend_filter_fields_were_submitted(self):
            if all(code == "IGNORE" for code in (trend_filter_gm_current, trend_filter_gm_market, trend_filter_gm_sector)):
                settings.pop(TREND_FILTER_OPERATOR_KEY, None)
                settings.pop(TREND_FILTER_GM_CURRENT_KEY, None)
                settings.pop(TREND_FILTER_GM_MARKET_KEY, None)
                settings.pop(TREND_FILTER_GM_SECTOR_KEY, None)
            else:
                settings[TREND_FILTER_OPERATOR_KEY] = trend_filter_operator
                if trend_filter_gm_current == "IGNORE":
                    settings.pop(TREND_FILTER_GM_CURRENT_KEY, None)
                else:
                    settings[TREND_FILTER_GM_CURRENT_KEY] = trend_filter_gm_current
                if trend_filter_gm_market == "IGNORE":
                    settings.pop(TREND_FILTER_GM_MARKET_KEY, None)
                else:
                    settings[TREND_FILTER_GM_MARKET_KEY] = trend_filter_gm_market
                if trend_filter_gm_sector == "IGNORE":
                    settings.pop(TREND_FILTER_GM_SECTOR_KEY, None)
                else:
                    settings[TREND_FILTER_GM_SECTOR_KEY] = trend_filter_gm_sector
        obj.settings = settings
        if commit:
            obj.save()
            self.save_m2m()
        return obj
