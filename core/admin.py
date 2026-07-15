import json

from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html
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


def _text_summary(value, *, label: str, preview_chars: int = 160) -> str:
    text = (value or "").strip()
    if not text:
        return f"{label}: no"
    size = len(text.encode("utf-8", errors="ignore"))
    preview = text[:preview_chars]
    if len(text) > preview_chars:
        preview += "..."
    return f"{label}: yes | ~{size} bytes | {preview}"

@admin.register(Symbol)
class SymbolAdmin(admin.ModelAdmin):
    list_display = ("ticker", "exchange", "name", "name_en", "sector", "instrument_type", "active")
    list_filter = ("active", "exchange", "instrument_type", "sector")
    search_fields = ("ticker", "name", "name_en", "exchange", "sector")

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
    exclude = ("signal_lines", "settings", "universe_snapshot", "results")
    readonly_fields = (
        "created_at",
        "updated_at",
        "signal_lines_summary",
        "settings_summary",
        "universe_snapshot_summary",
        "results_summary",
        "results_page_link",
    )
    fields = (
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
        "warmup_days",
        "close_positions_at_end",
        "status",
        "error_message",
        "created_by",
        "signal_lines_summary",
        "settings_summary",
        "universe_snapshot_summary",
        "results_summary",
        "results_page_link",
        "created_at",
        "updated_at",
    )

    @staticmethod
    def _json_summary(value, *, label: str) -> str:
        if not value:
            return f"{label}: no"
        try:
            payload = json.dumps(value, ensure_ascii=False)
            size = len(payload.encode("utf-8"))
        except Exception:
            size = 0
        if isinstance(value, dict):
            details = f"{len(value)} keys"
        elif isinstance(value, list):
            details = f"{len(value)} items"
        else:
            details = value.__class__.__name__
        return f"{label}: yes | {details} | ~{size} bytes"

    @admin.display(description="Signal Lines")
    def signal_lines_summary(self, obj):
        return self._json_summary(obj.signal_lines, label="Signal lines")

    @admin.display(description="Settings")
    def settings_summary(self, obj):
        return self._json_summary(obj.settings, label="Settings")

    @admin.display(description="Universe Snapshot")
    def universe_snapshot_summary(self, obj):
        return self._json_summary(obj.universe_snapshot, label="Universe snapshot")

    @admin.display(description="Results")
    def results_summary(self, obj):
        return self._json_summary(obj.results, label="Results")

    @admin.display(description="Results Page")
    def results_page_link(self, obj):
        if not obj or not getattr(obj, "pk", None):
            return "Save first"
        url = reverse("backtest_results", args=[obj.pk])
        return format_html('<a href="{}" target="_blank" rel="noopener">Open backtest results</a>', url)


@admin.register(ProcessingJob)
class ProcessingJobAdmin(admin.ModelAdmin):
    list_display = ("id", "job_type", "status", "created_at", "started_at", "finished_at", "backtest", "scenario")
    list_filter = ("status", "job_type")
    # IMPORTANT (prod): avoid searching large TextFields (message/error) from the changelist.
    # It can trigger full table scans and huge memory usage.
    search_fields = ("task_id",)
    date_hierarchy = "created_at"

    # Keep the changelist responsive even with many rows.
    ordering = ("-id",)
    list_select_related = ("backtest", "scenario")
    list_per_page = 50
    show_full_result_count = False  # avoids expensive COUNT(*) on large tables
    exclude = ("message", "error")
    readonly_fields = (
        "created_at",
        "started_at",
        "finished_at",
        "heartbeat_at",
        "last_checkpoint",
        "worker_hostname",
        "message_summary",
        "error_summary",
    )
    fields = (
        "job_type",
        "status",
        "task_id",
        "backtest",
        "scenario",
        "game_scenario",
        "created_by",
        "output_file",
        "output_name",
        "cancel_requested",
        "kill_requested",
        "message_summary",
        "error_summary",
        "created_at",
        "started_at",
        "finished_at",
        "heartbeat_at",
        "last_checkpoint",
        "worker_hostname",
    )

    def get_queryset(self, request):
        qs = super().get_queryset(request)
        # Avoid loading heavy local text columns and huge related JSON payloads.
        return qs.defer(
            "message", "error",
            "backtest__results", "backtest__settings", "backtest__universe_snapshot", "backtest__signal_lines",
            "scenario__description",
        )

    @admin.display(description="Message")
    def message_summary(self, obj):
        return _text_summary(obj.message, label="Message")

    @admin.display(description="Error")
    def error_summary(self, obj):
        return _text_summary(obj.error, label="Error")
