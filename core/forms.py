from django import forms
from django.utils import timezone

BACKTEST_SIGNAL_CHOICES = [
    ("A1", "A1 (K1 croise 0 vers le haut)"),
    ("B1", "B1 (K1 croise 0 vers le bas)"),
    ("A1f", "A1f (K1f croise 0 vers le haut)"),
    ("B1f", "B1f (K1f croise 0 vers le bas)"),
    ("C1", "C1 (K2 croise 0 vers le haut)"),
    ("D1", "D1 (K2 croise 0 vers le bas)"),
    ("E1", "E1 (K3 croise 0 vers le haut)"),
    ("F1", "F1 (K3 croise 0 vers le bas)"),
    ("G1", "G1 (K4 croise 0 vers le haut)"),
    ("H1", "H1 (K4 croise 0 vers le bas)"),

    # K2f (floating line derived from K1)
    ("A2f", "A2f (K1 croise K2f de bas en haut)"),
    ("B2f", "B2f (K1 croise K2f de haut en bas OU pente négative)"),

    # V line (rolling max-high then rolling mean)
    ("I1", "I1 (High croise V de bas en haut)"),
    ("J1", "J1 (High croise V de haut en bas)"),
]


def _parse_symbols_text(text: str) -> list[Symbol]:
    """Parse a user-entered tickers list into Symbol instances.

    Accepted formats (one per line or separated by comma/semicolon/space):
      - AAPL
      - AAPL:NASDAQ
      - AIR:EPA

    Behavior:
      - Creates missing Symbol rows (ticker + optional exchange).
      - Keeps exchange optional.
      - Strips/uppercases tickers.

    NOTE: Intentionally lightweight (no external validation against providers).
    """
    if not text:
        return []

    raw = text.replace(";", "\n").replace(",", "\n")
    parts: list[str] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        # Allow space-separated entries in a line
        for tok in line.split():
            tok = tok.strip()
            if tok:
                parts.append(tok)

    symbols: list[Symbol] = []
    seen = set()
    for tok in parts:
        tok = tok.strip()
        if not tok:
            continue
        if ":" in tok:
            ticker, exchange = tok.split(":", 1)
        else:
            ticker, exchange = tok, ""
        ticker = ticker.strip().upper()
        exchange = exchange.strip().upper()
        if not ticker:
            continue
        key = (ticker, exchange)
        if key in seen:
            continue
        seen.add(key)
        sym, _ = Symbol.objects.get_or_create(
            ticker=ticker,
            exchange=exchange,
            defaults={"active": True},
        )
        if not sym.active:
            sym.active = True
            sym.save(update_fields=["active"])
        symbols.append(sym)
    return symbols

from .models import EmailRecipient, EmailSettings, Scenario, Symbol, Universe, Backtest, AlertDefinition


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


    SCOPE_ALL = "all"
    SCOPE_UNIVERSES = "universes"
    SCOPE_CUSTOM = "custom"

    scope_mode = forms.ChoiceField(
        choices=[
            (SCOPE_ALL, "Toutes les actions (pas de filtre)"),
            (SCOPE_UNIVERSES, "Choisir un univers existant"),
            (SCOPE_CUSTOM, "Définir une liste personnalisée"),
        ],
        required=False,
        initial=SCOPE_ALL,
        widget=forms.RadioSelect,
        label="Actions concernées",
    )

    universes = forms.ModelMultipleChoiceField(
        queryset=Universe.objects.filter(active=True).order_by("name"),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 8}),
        help_text="Univers (tickers) ciblés par cette alerte.",
        label="Univers",
    )

    custom_symbols_text = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 6, "placeholder": "Ex: AAPL\nMSFT\nAIR:EPA"}),
        label="Liste d'actions",
        help_text="Une action par ligne. Format optionnel: TICKER:EXCHANGE.",
    )
    save_custom_as_universe = forms.BooleanField(
        required=False,
        initial=False,
        label="Enregistrer comme univers réutilisable",
        help_text="Si activé, la liste sera enregistrée comme un univers visible dans le menu 'Univers'.",
    )
    custom_universe_name = forms.CharField(
        required=False,
        max_length=120,
        label="Nom de l'univers",
        help_text="Optionnel. Si vide, un nom auto sera utilisé.",
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
            "universes",
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
            # Initialize scope
            if self.instance.universes.exists():
                self.fields["scope_mode"].initial = self.SCOPE_UNIVERSES
            else:
                self.fields["scope_mode"].initial = self.SCOPE_ALL

        # Include selected universes even if inactive
        qs = Universe.objects.filter(active=True)
        if self.instance and self.instance.pk:
            selected = self.instance.universes.all()
            if selected.exists():
                qs = (qs | selected).distinct()
        self.fields["universes"].queryset = qs.order_by("name")

    def clean(self):
        cleaned = super().clean()
        mode = cleaned.get("scope_mode") or self.SCOPE_ALL
        if mode == self.SCOPE_UNIVERSES:
            universes = cleaned.get("universes")
            if not universes or universes.count() == 0:
                self.add_error("universes", "Choisis au moins un univers, ou sélectionne un autre mode de scope.")
        if mode == self.SCOPE_CUSTOM:
            txt = (cleaned.get("custom_symbols_text") or "").strip()
            if not txt:
                self.add_error("custom_symbols_text", "Colle une liste d'actions (au moins 1 ticker).")
        return cleaned

    def _create_universe_from_custom(self, name_hint: str | None, active: bool) -> Universe:
        symbols = _parse_symbols_text(self.cleaned_data.get("custom_symbols_text") or "")
        if not symbols:
            raise forms.ValidationError("Liste d'actions vide ou invalide.")

        base_name = (name_hint or "").strip()
        if not base_name:
            base_name = f"[auto] Scope {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}"
        name = base_name
        i = 2
        while Universe.objects.filter(name=name).exists():
            name = f"{base_name} ({i})"
            i += 1
        uni = Universe.objects.create(name=name, description="Univers créé depuis une alerte.", active=active)
        uni.symbols.set(symbols)
        return uni

    def save(self, commit=True):
        obj: AlertDefinition = super().save(commit=False)
        codes = self.cleaned_data.get("alert_codes_multi") or []
        obj.alert_codes = ",".join(codes)
        if commit:
            obj.save()
            self.save_m2m()

            # Apply scope_mode post-save (needs obj.pk)
            mode = self.cleaned_data.get("scope_mode") or self.SCOPE_ALL
            if mode == self.SCOPE_ALL:
                obj.universes.clear()
            elif mode == self.SCOPE_UNIVERSES:
                # Already set by form m2m
                pass
            elif mode == self.SCOPE_CUSTOM:
                active = bool(self.cleaned_data.get("save_custom_as_universe"))
                name_hint = self.cleaned_data.get("custom_universe_name")
                uni = self._create_universe_from_custom(name_hint=name_hint, active=active)
                obj.universes.set([uni])
        return obj

class ScenarioForm(forms.ModelForm):
    symbols = forms.ModelMultipleChoiceField(
        queryset=Symbol.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
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
            "vc",
            "fl",
            "n1",
            "n2",
            "n3",
            "n4",
            "n5",
            "k2j",
            "cr",
            "m_v",
            "history_years",
            "active",
            "symbols",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            # Prevent entering 0 (division by zero risk)
            "e": forms.NumberInput(attrs={"min": 0.0001, "step": 0.0001}),
            "vc": forms.NumberInput(attrs={"min": 0, "max": 1, "step": 0.0001}),
            "fl": forms.NumberInput(attrs={"min": 0, "max": 1, "step": 0.0001}),
            "n5": forms.NumberInput(attrs={"min": 1, "step": 1}),
            "k2j": forms.NumberInput(attrs={"min": 1, "step": 1}),
            "cr": forms.NumberInput(attrs={"min": 0, "step": 0.0001}),
            "m_v": forms.NumberInput(attrs={"min": 2, "step": 1}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["symbols"].queryset = Symbol.objects.filter(active=True).order_by("ticker", "exchange")
        if self.instance.pk:
            self.fields["symbols"].initial = self.instance.symbols.all()

class EmailRecipientForm(forms.ModelForm):
    class Meta:
        model = EmailRecipient
        fields = ["email", "active"]

class SymbolManualForm(forms.ModelForm):
    scenarios = forms.ModelMultipleChoiceField(
        queryset=Scenario.objects.none(),
        required=False,
        widget=forms.CheckboxSelectMultiple,
        help_text="Scénarios associés à ce ticker (le scénario par défaut sera ajouté automatiquement).",
    )

    class Meta:
        model = Symbol
        fields = ["ticker", "exchange", "name", "instrument_type", "country", "currency", "active", "scenarios"]

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
        widget=forms.CheckboxSelectMultiple,
        label="Scénarios",
    )


class SymbolImportForm(forms.Form):
    """Import tickers from CSV/XLSX."""

    file = forms.FileField(label="Fichier (CSV ou Excel .xlsx)")



class UniverseForm(forms.ModelForm):
    """Create/Edit a Universe (watchlist)."""

    symbols = forms.ModelMultipleChoiceField(
        queryset=Symbol.objects.filter(active=True).order_by("ticker", "exchange"),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 14, "style": "min-width:320px;"}),
        help_text="Tickers inclus dans cet univers.",
        label="Tickers",
    )

    class Meta:
        model = Universe
        fields = ["name", "description", "symbols", "active"]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
        }


class BacktestForm(forms.ModelForm):
    """Create/Edit a Backtest configuration (engine results will be computed later)."""

    SCOPE_SCENARIO = "scenario"
    SCOPE_UNIVERSE = "universe"
    SCOPE_CUSTOM = "custom"

    scope_mode = forms.ChoiceField(
        choices=[
            (SCOPE_SCENARIO, "Utiliser les actions du scénario"),
            (SCOPE_UNIVERSE, "Choisir un univers existant"),
            (SCOPE_CUSTOM, "Définir une liste personnalisée"),
        ],
        required=False,
        initial=SCOPE_SCENARIO,
        widget=forms.RadioSelect,
        label="Actions concernées",
    )
    custom_symbols_text = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 6, "placeholder": "Ex: AAPL\nMSFT\nAIR:EPA"}),
        label="Liste d'actions",
        help_text="Une action par ligne. Format optionnel: TICKER:EXCHANGE.",
    )
    save_custom_as_universe = forms.BooleanField(
        required=False,
        initial=False,
        label="Enregistrer comme univers réutilisable",
        help_text="Si activé, la liste sera enregistrée comme un univers visible dans le menu 'Univers'.",
    )
    custom_universe_name = forms.CharField(
        required=False,
        max_length=120,
        label="Nom de l'univers",
        help_text="Optionnel. Si vide, un nom auto sera utilisé.",
    )

    class Meta:
        model = Backtest
        fields = [
            "name",
            "description",
            "scenario",
            "universe",
            "start_date",
            "end_date",
            "capital_total",
            "capital_per_ticker",
            "ratio_threshold",
            "include_all_tickers",
            "signal_lines",
            "close_positions_at_end",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "start_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
            "end_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
        }


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Only propose active universes by default; include current selection even if inactive.
        if "universe" in self.fields:
            qs = Universe.objects.filter(active=True)
            if self.instance and getattr(self.instance, "universe_id", None):
                qs = (qs | Universe.objects.filter(pk=self.instance.universe_id)).distinct()
            self.fields["universe"].queryset = qs.order_by("name")
            self.fields["universe"].required = False
            self.fields["universe"].help_text = "Univers à utiliser pour ce backtest (optionnel)."

        # Initialize scope_mode for edit
        if self.instance and getattr(self.instance, "pk", None):
            if self.instance.universe_id:
                self.fields["scope_mode"].initial = self.SCOPE_UNIVERSE
            else:
                self.fields["scope_mode"].initial = self.SCOPE_SCENARIO

    def clean_signal_lines(self):
        """Accept JSON list; keep validation minimal for Feature 1."""
        value = self.cleaned_data.get("signal_lines")
        if value in (None, ""):
            return []
        if not isinstance(value, list):
            raise forms.ValidationError("signal_lines must be a JSON list")
        # Lightweight sanity check: each item should at least be a dict with buy/sell keys (optional at this stage).
        for item in value:
            if not isinstance(item, dict):
                raise forms.ValidationError("Each signal line must be an object")
        return value

    def clean(self):
        cleaned = super().clean()
        mode = cleaned.get("scope_mode") or self.SCOPE_SCENARIO

        if mode == self.SCOPE_UNIVERSE:
            if not cleaned.get("universe"):
                self.add_error("universe", "Choisis un univers, ou sélectionne un autre mode de scope.")

        if mode == self.SCOPE_CUSTOM:
            txt = (cleaned.get("custom_symbols_text") or "").strip()
            if not txt:
                self.add_error("custom_symbols_text", "Colle une liste d'actions (au moins 1 ticker).")
        return cleaned

    def _create_universe_from_custom(self, name_hint: str | None, active: bool) -> Universe:
        symbols = _parse_symbols_text(self.cleaned_data.get("custom_symbols_text") or "")
        if not symbols:
            raise forms.ValidationError("Liste d'actions vide ou invalide.")

        base_name = (name_hint or "").strip()
        if not base_name:
            base_name = f"[auto] Scope {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # Ensure unique name
        name = base_name
        i = 2
        while Universe.objects.filter(name=name).exists():
            name = f"{base_name} ({i})"
            i += 1

        uni = Universe.objects.create(name=name, description="Univers créé depuis un backtest.", active=active)
        uni.symbols.set(symbols)
        return uni

    def save(self, commit=True):
        obj: Backtest = super().save(commit=False)
        mode = self.cleaned_data.get("scope_mode") or self.SCOPE_SCENARIO

        if mode == self.SCOPE_SCENARIO:
            obj.universe = None
        elif mode == self.SCOPE_UNIVERSE:
            obj.universe = self.cleaned_data.get("universe")
        elif mode == self.SCOPE_CUSTOM:
            active = bool(self.cleaned_data.get("save_custom_as_universe"))
            name_hint = self.cleaned_data.get("custom_universe_name")
            obj.universe = self._create_universe_from_custom(name_hint=name_hint, active=active)

        if commit:
            obj.save()
            self.save_m2m()
        return obj
