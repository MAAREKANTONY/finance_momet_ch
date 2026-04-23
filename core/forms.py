from django import forms
import json

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

from .models import EmailRecipient, EmailSettings, Scenario, Symbol, Backtest, AlertDefinition, Universe, Study
from .trading_model_config import (
    TRADING_MODEL_LATCH_STATEFUL,
    resolve_trading_model,
    validate_explicit_latch_config,
)
from .widgets import SymbolPickerWidget




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
        if explicit_trading_model and trading_model == TRADING_MODEL_LATCH_STATEFUL:
            try:
                validate_explicit_latch_config(
                    buy_codes=buy,
                    buy_logic=payload["buy_logic"],
                    sell_codes=sell,
                    sell_gm_filter=payload["sell_gm_filter"],
                )
            except ValueError as exc:
                raise forms.ValidationError(str(exc)) from exc
        if buy or sell or payload["buy_gm_filter"] != "IGNORE" or payload["sell_gm_filter"] != "IGNORE":
            cleaned.append(payload)
    return cleaned
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
            "a",
            "b",
            "c",
            "d",
            "e",
            "n1",
            "n2",
            "npente",
            "slope_threshold",
            "npente_basse",
            "slope_threshold_basse",
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
            "npente_basse",
            "slope_threshold_basse",
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

    min_price = forms.DecimalField(required=False, min_value=0)
    max_price = forms.DecimalField(required=False, min_value=0)

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

    def clean_signal_lines(self):
        return _clean_signal_lines_json(self.cleaned_data.get("signal_lines"))

    def clean(self):
        cleaned = super().clean()
        min_price = cleaned.get("min_price")
        max_price = cleaned.get("max_price")
        if min_price is not None and max_price is not None and min_price > max_price:
            self.add_error("max_price", "Le prix maximum doit être supérieur ou égal au prix minimum.")
        return cleaned

    def save(self, commit=True):
        obj = super().save(commit=False)
        settings = dict(obj.settings or {})
        min_price = self.cleaned_data.get("min_price")
        max_price = self.cleaned_data.get("max_price")
        if min_price is None:
            settings.pop("min_price", None)
        else:
            settings["min_price"] = str(min_price)
        if max_price is None:
            settings.pop("max_price", None)
        else:
            settings["max_price"] = str(max_price)
        obj.settings = settings
        if commit:
            obj.save()
            self.save_m2m()
        return obj


class GameScenarioForm(forms.ModelForm):
    """CRUD form for GameScenario."""

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
            "npente_basse",
            "slope_threshold_basse",
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

    def clean_signal_lines(self):
        return _clean_signal_lines_json(self.cleaned_data.get("signal_lines"))
