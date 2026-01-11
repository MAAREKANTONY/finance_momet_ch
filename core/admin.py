from django.contrib import admin
from .models import Symbol, Scenario, DailyBar, DailyMetric, Alert, EmailRecipient, EmailSettings

@admin.register(Symbol)
class SymbolAdmin(admin.ModelAdmin):
    list_display = ("ticker", "exchange", "name", "instrument_type", "active")
    list_filter = ("active", "exchange", "instrument_type")
    search_fields = ("ticker", "name", "exchange")

@admin.register(Scenario)
class ScenarioAdmin(admin.ModelAdmin):
    list_display = ("name", "active", "history_years", "n1", "n2", "n3", "updated_at")
    list_filter = ("active",)
    search_fields = ("name",)

@admin.register(DailyBar)
class DailyBarAdmin(admin.ModelAdmin):
    list_display = ("symbol", "date", "open", "high", "low", "close", "change_pct")
    date_hierarchy = "date"
    list_filter = ("symbol",)

@admin.register(DailyMetric)
class DailyMetricAdmin(admin.ModelAdmin):
    list_display = ("symbol", "scenario", "date", "P", "M1", "X1", "K1", "K2", "K3", "K4")
    date_hierarchy = "date"
    list_filter = ("scenario", "symbol")

@admin.register(Alert)
class AlertAdmin(admin.ModelAdmin):
    list_display = ("date", "symbol", "scenario", "alerts")
    date_hierarchy = "date"
    list_filter = ("scenario", "symbol")

@admin.register(EmailRecipient)
class EmailRecipientAdmin(admin.ModelAdmin):
    list_display = ("email", "active")
    list_filter = ("active",)

admin.site.register(EmailSettings)
