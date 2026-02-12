from django.contrib import admin
from django.urls import path, include
from core import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("accounts/", include("django.contrib.auth.urls")),
    path("", views.dashboard, name="dashboard"),

    path("alerts/", views.alerts_table, name="alerts_table"),
    path("alerts/export.csv", views.alerts_export_csv, name="alerts_export_csv"),
    path("data/export.xlsx", views.data_export_xlsx, name="data_export_xlsx"),
    path("data/export/scenario/<int:scenario_id>.xlsx", views.data_export_scenario_xlsx, name="data_export_scenario_xlsx"),
    path("data/export/scenarios.zip", views.data_export_all_scenarios_zip, name="data_export_all_scenarios_zip"),

    path("symbols/", views.symbols_page, name="symbols_page"),
    path("symbols/add/", views.symbol_add, name="symbol_add"),
    path("symbols/import/", views.symbols_import, name="symbols_import"),
    path("symbols/<int:pk>/scenarios/", views.symbol_scenarios_edit, name="symbol_scenarios_edit"),
    path("symbols/<int:pk>/toggle/", views.symbol_toggle_active, name="symbol_toggle"),
    path("symbols/<int:pk>/delete/", views.symbol_delete, name="symbol_delete"),

    path("scenarios/", views.scenarios_page, name="scenarios_page"),
    path("scenarios/new/", views.scenario_create, name="scenario_create"),
    path("scenarios/<int:pk>/edit/", views.scenario_edit, name="scenario_edit"),
    path("scenarios/<int:pk>/delete/", views.scenario_delete, name="scenario_delete"),

    path("settings/email/", views.email_settings_page, name="email_settings"),
    path("settings/configuration/", views.email_settings_page, name="configuration_page"),

    # User-defined alerts (CRUD)
    path("settings/alerts/", views.alert_definitions_list, name="alert_definitions_list"),
    path("settings/alerts/new/", views.alert_definition_create, name="alert_definition_create"),
    path("settings/alerts/<int:pk>/edit/", views.alert_definition_edit, name="alert_definition_edit"),
    path("settings/alerts/<int:pk>/delete/", views.alert_definition_delete, name="alert_definition_delete"),
    path("settings/alerts/<int:pk>/send/", views.alert_definition_send_now, name="alert_definition_send_now"),
    path("settings/email/<int:pk>/toggle/", views.email_recipient_toggle, name="email_recipient_toggle"),
    path("settings/email/<int:pk>/delete/", views.email_recipient_delete, name="email_recipient_delete"),

    path("settings/email/run_compute/", views.run_compute_now, name="run_compute_now"),
    path("settings/email/recompute_all/", views.run_recompute_all_now, name="run_recompute_all_now"),
    path("settings/email/send_now/", views.send_mail_now, name="send_mail_now"),
    path("settings/email/fetch_now/", views.fetch_bars_now, name="fetch_bars_now"),

    path("api/symbol_search/", views.api_symbol_search, name="api_symbol_search"),

    path("logs/", views.logs_page, name="logs_page"),
    path("jobs/", views.jobs_page, name="jobs_page"),
    path("backtests/", views.backtests_page, name="backtests_page"),
    path("backtests/new/", views.backtest_create, name="backtest_create"),
    path("backtests/<int:pk>/edit/", views.backtest_update, name="backtest_update"),
    path("backtests/<int:pk>/", views.backtest_detail, name="backtest_detail"),
    path("backtests/<int:pk>/delete/", views.backtest_delete, name="backtest_delete"),
    path("backtests/<int:pk>/fetch_data/", views.backtest_fetch_data, name="backtest_fetch_data"),
    path("backtests/<int:pk>/compute_metrics/", views.backtest_compute_metrics, name="backtest_compute_metrics"),
    path("backtests/<int:pk>/recompute_metrics/", views.backtest_recompute_metrics, name="backtest_recompute_metrics"),
    path("backtests/<int:pk>/run/", views.backtest_run, name="backtest_run"),
    path("backtests/<int:pk>/results/", views.backtest_results, name="backtest_results"),
    path("backtests/<int:pk>/export_debug.csv", views.backtest_export_debug_csv, name="backtest_export_debug_csv"),
    path("backtests/<int:pk>/export.xlsx", views.backtest_export_excel, name="backtest_export_excel"),
    path("backtests/<int:pk>/export_compact.xlsx", views.backtest_export_excel_compact, name="backtest_export_excel_compact"),
    path("backtests/<int:pk>/export_details.zip", views.backtest_export_details, name="backtest_export_details"),

    # Helper / documentation
    path("help/indicators/", views.indicators_help, name="indicators_help"),
]
