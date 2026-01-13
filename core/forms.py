from django import forms

BACKTEST_SIGNAL_CHOICES = [
    ("A1", "A1 (K1 croise 0 vers le haut)"),
    ("B1", "B1 (K1 croise 0 vers le bas)"),
    ("C1", "C1 (K2 croise 0 vers le haut)"),
    ("D1", "D1 (K2 croise 0 vers le bas)"),
    ("E1", "E1 (K3 croise 0 vers le haut)"),
    ("F1", "F1 (K3 croise 0 vers le bas)"),
    ("G1", "G1 (K4 croise 0 vers le haut)"),
    ("H1", "H1 (K4 croise 0 vers le bas)"),
]

from .models import EmailRecipient, EmailSettings, Scenario, Symbol, Backtest

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
            "n1",
            "n2",
            "n3",
            "n4",
            "history_years",
            "active",
            "symbols",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            # Prevent entering 0 (division by zero risk)
            "e": forms.NumberInput(attrs={"min": 0.0001, "step": 0.0001}),
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
            "signal_lines",
            "close_positions_at_end",
        ]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
            "start_date": forms.DateInput(attrs={"type": "date"}),
            "end_date": forms.DateInput(attrs={"type": "date"}),
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
