from django import forms

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

    # K2f (floating price line)
    ("A2f", "A2f (P croise K2f de bas en haut)"),
    ("B2f", "B2f (P croise K2f de haut en bas)"),

    # V line (rolling max-high then rolling mean)
    ("I1", "I1 (High croise V de bas en haut)"),
    ("J1", "J1 (High croise V de haut en bas)"),
]

from .models import EmailRecipient, EmailSettings, Scenario, Symbol, Backtest, AlertDefinition, Universe, Study


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
        fields = [
            "name",
            "description",
            "recipients",
            "send_hour",
            "send_minute",
            "timezone",
            "is_active",
        ]

    def __init__(self, *args, study_scenario: Scenario, **kwargs):
        self._study_scenario = study_scenario
        super().__init__(*args, **kwargs)
        # Remove scenarios field from parent form if present
        if "scenarios" in self.fields:
            self.fields.pop("scenarios")

    def save(self, commit=True):
        obj: AlertDefinition = super().save(commit=commit)
        # Force scenario membership
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
            "close_positions_at_end",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "start_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
            "end_date": forms.DateInput(format="%Y-%m-%d", attrs={"type": "date"}),
        }

    def clean_signal_lines(self):
        value = self.cleaned_data.get("signal_lines")
        if value in (None, ""):
            return []
        if not isinstance(value, list):
            raise forms.ValidationError("signal_lines must be a JSON list")
        for item in value:
            if not isinstance(item, dict):
                raise forms.ValidationError("Each signal line must be an object")
        return value


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
        widget=forms.CheckboxSelectMultiple,
        label="Tickers",
        help_text="Tickers associés à cette Study.",
    )

    class Meta:
        model = Scenario
        fields = [
            # indicator params
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
            "symbols",
        ]
        widgets = {
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


class BacktestForm(forms.ModelForm):
    """Create/Edit a Backtest configuration (engine results will be computed later)."""

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
