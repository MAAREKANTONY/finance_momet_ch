from django import forms
from django.core.validators import MinValueValidator
from .models import Scenario, EmailRecipient, Symbol, EmailSettings, BacktestRun

class ScenarioForm(forms.ModelForm):
    def clean_e(self):
        e = self.cleaned_data.get('e')
        if e is not None and e <= 0:
            raise forms.ValidationError("La valeur e doit être strictement > 0.")
        return e

    class Meta:
        model = Scenario
        fields = [
            "name", "description", "is_default",
            "a", "b", "c", "d", "e",
            "n1", "n2", "n3", "n4",
            "history_years",
            "backtest_default_capital",
            "active",
        ]
        widgets = {"description": forms.Textarea(attrs={"rows": 3})}

class EmailRecipientForm(forms.ModelForm):
    class Meta:
        model = EmailRecipient
        fields = ["email", "active"]

class SymbolManualForm(forms.ModelForm):
    class Meta:
        model = Symbol
        fields = ["ticker", "exchange", "name", "instrument_type", "country", "currency", "active"]


class EmailSettingsForm(forms.ModelForm):
    class Meta:
        model = EmailSettings
        fields = ["send_hour", "send_minute", "timezone"]
        widgets = {
            "send_hour": forms.NumberInput(attrs={"min": 0, "max": 23}),
            "send_minute": forms.NumberInput(attrs={"min": 0, "max": 59}),
        }


class SymbolScenariosForm(forms.Form):
    scenarios = forms.ModelMultipleChoiceField(
        queryset=Scenario.objects.all().order_by("-active", "name"),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 10}),
        help_text="Scénarios associés à ce ticker."
    )


class SymbolImportForm(forms.Form):
    file = forms.FileField(help_text="CSV ou Excel (.xlsx) avec colonnes: ticker code, ticker market, scenario list")


class BacktestRunForm(forms.ModelForm):
    class Meta:
        model = BacktestRun
        fields = ["scenario", "strategy", "name", "description", "capital_total", "capital_per_symbol", "min_ratio_p"]
        widgets = {
            "description": forms.Textarea(attrs={"rows": 3}),
        }
