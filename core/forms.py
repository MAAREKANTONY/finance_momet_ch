from django import forms

from .models import EmailRecipient, EmailSettings, Scenario, Symbol

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
