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
    path("symbols/<int:pk>/toggle/", views.symbol_toggle_active, name="symbol_toggle"),
    path("symbols/<int:pk>/delete/", views.symbol_delete, name="symbol_delete"),

    path("scenarios/", views.scenarios_page, name="scenarios_page"),
    path("scenarios/new/", views.scenario_create, name="scenario_create"),
    path("scenarios/<int:pk>/edit/", views.scenario_edit, name="scenario_edit"),
    path("scenarios/<int:pk>/delete/", views.scenario_delete, name="scenario_delete"),

    path("settings/email/", views.email_settings_page, name="email_settings"),
    path("settings/email/<int:pk>/toggle/", views.email_recipient_toggle, name="email_recipient_toggle"),
    path("settings/email/<int:pk>/delete/", views.email_recipient_delete, name="email_recipient_delete"),

    path("settings/email/run_compute/", views.run_compute_now, name="run_compute_now"),
    path("settings/email/recompute_all/", views.run_recompute_all_now, name="run_recompute_all_now"),
    path("settings/email/send_now/", views.send_mail_now, name="send_mail_now"),

    path("api/symbol_search/", views.api_symbol_search, name="api_symbol_search"),
]
