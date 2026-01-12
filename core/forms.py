from django import forms
from .models import Scenario, EmailRecipient, Symbol, EmailSettings

class ScenarioForm(forms.ModelForm):
    class Meta:
        model = Scenario
        fields = ["name", "description", "a", "b", "c", "d", "e", "n1", "n2", "n3", "n4", "history_years", "active"]
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
