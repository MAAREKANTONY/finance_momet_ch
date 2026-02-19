from django.contrib import admin
from .models import (
    Symbol,
    Scenario,
    DailyBar,
    DailyMetric,
    Alert,
    EmailRecipient,
    EmailSettings,
    Backtest,
    ProcessingJob,
    AlertDefinition,
    Universe,
    Study,
)

@admin.register(Symbol)
class SymbolAdmin(admin.ModelAdmin):
    list_display = ("ticker", "exchange", "name", "instrument_type", "active")
    list_filter = ("active", "exchange", "instrument_type")
    search_fields = ("ticker", "name", "exchange")

@admin.register(Scenario)
class ScenarioAdmin(admin.ModelAdmin):
    list_display = ("name", "active", "is_study_clone", "history_years", "n1", "n2", "n3", "updated_at")
    list_filter = ("active", "is_study_clone")
    search_fields = ("name",)


@admin.register(Universe)
class UniverseAdmin(admin.ModelAdmin):
    list_display = ("name", "active", "created_at")
    list_filter = ("active",)
    search_fields = ("name",)
    filter_horizontal = ("symbols",)


@admin.register(Study)
class StudyAdmin(admin.ModelAdmin):
    list_display = ("name", "created_by", "scenario", "created_at")
    search_fields = ("name",)
    list_filter = ("created_at",)

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
admin.site.register(AlertDefinition)


@admin.register(Backtest)
class BacktestAdmin(admin.ModelAdmin):
    list_display = ("name", "scenario", "status", "start_date", "end_date", "created_at")
    list_filter = ("status", "scenario")
    search_fields = ("name", "description")
    date_hierarchy = "created_at"


@admin.register(ProcessingJob)
class ProcessingJobAdmin(admin.ModelAdmin):
    list_display = ("id", "job_type", "status", "created_at", "started_at", "finished_at", "backtest", "scenario")
    list_filter = ("status", "job_type")
    search_fields = ("message", "error", "task_id")
    date_hierarchy = "created_at"
