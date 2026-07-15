from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0049_symbol_name_en"),
    ]

    operations = [
        migrations.AlterField(
            model_name="processingjob",
            name="job_type",
            field=models.CharField(
                choices=[
                    ("FETCH_BARS", "Fetch Daily Bars"),
                    ("COMPUTE_METRICS", "Compute Metrics"),
                    ("SYNC_MARKET_CAPS", "Sync Market Caps"),
                    ("ENRICH_METADATA", "Enrichissement des métadonnées"),
                    ("GENERATE_CSI300_CSV", "Génération du CSV CSI300"),
                    ("REFRESH_CSI300_DATA", "Rafraîchissement des données CSI300"),
                    ("RUN_BACKTEST", "Run Backtest"),
                    ("RUN_GAME", "Run Game Scenario"),
                    ("SEND_EMAILS", "Send Emails"),
                    ("EXPORT_ALERTS_CSV", "Export Alerts CSV"),
                    ("EXPORT_SCENARIO_XLSX", "Export Scenario XLSX"),
                    ("EXPORT_ALL_SCENARIOS_ZIP", "Export All Scenarios ZIP"),
                    ("EXPORT_DATA_XLSX", "Export Data XLSX"),
                    ("EXPORT_BACKTEST_DEBUG_CSV", "Export Backtest Debug CSV"),
                    ("EXPORT_BACKTEST_DEBUG_XLSX", "Export Backtest Debug XLSX"),
                    ("EXPORT_BACKTEST_XLSX", "Export Backtest XLSX"),
                    ("EXPORT_BACKTEST_XLSX_COMPACT", "Export Backtest XLSX Compact"),
                    ("EXPORT_GAME_SCENARIO_XLSX", "Export Game Scenario XLSX"),
                    ("EXPORT_BACKTEST_DETAILS_ZIP", "Export Backtest Details ZIP"),
                ],
                max_length=32,
            ),
        ),
    ]
