# Generated manually to regularize historical model drift.

import django.core.validators
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0043_rhd_ok_reactivation_mode"),
    ]

    operations = [
        migrations.RenameIndex(
            model_name="alert",
            new_name="core_alert_date_00d0fc_idx",
            old_name="core_alert_date_scenario_idx",
        ),
        migrations.RenameIndex(
            model_name="backtest",
            new_name="core_backte_scenari_c25832_idx",
            old_name="core_backte_scenario_0f3a3b_idx",
        ),
        migrations.RenameIndex(
            model_name="backtest",
            new_name="core_backte_status_ab28a7_idx",
            old_name="core_backte_status_5b87cb_idx",
        ),
        migrations.RenameIndex(
            model_name="backtestportfoliodaily",
            new_name="core_backte_backtes_a89e3e_idx",
            old_name="core_btpf_backte_5c2c9d_idx",
        ),
        migrations.RenameIndex(
            model_name="dailybar",
            new_name="core_dailyb_symbol__42318f_idx",
            old_name="core_dailybar_symbol_date_idx",
        ),
        migrations.RenameIndex(
            model_name="dailymetric",
            new_name="core_dailym_symbol__808431_idx",
            old_name="core_dailymetric_symbol_scenario_date_idx",
        ),
        migrations.RenameIndex(
            model_name="joblog",
            new_name="core_joblog_created_6e9cd8_idx",
            old_name="core_joblog_idx",
        ),
        migrations.RenameIndex(
            model_name="processingjob",
            new_name="core_proces_status_c7a5c5_idx",
            old_name="core_proces_status_0b44b8_idx",
        ),
        migrations.RenameIndex(
            model_name="processingjob",
            new_name="core_proces_job_typ_4dacbc_idx",
            old_name="core_proces_job_typ_4e2c89_idx",
        ),
        migrations.RenameIndex(
            model_name="processingjob",
            new_name="core_proces_backtes_08f0da_idx",
            old_name="core_proces_backtes_9b22d2_idx",
        ),
        migrations.RenameIndex(
            model_name="processingjob",
            new_name="core_proces_scenari_08bded_idx",
            old_name="core_proces_scenari_4db1b4_idx",
        ),
        migrations.RenameIndex(
            model_name="processingjob",
            new_name="core_proces_status_7ee9b5_idx",
            old_name="core_proces_status_heartb_9e2c9a_idx",
        ),
        migrations.RenameIndex(
            model_name="processingjob",
            new_name="core_proces_status_ab0ff5_idx",
            old_name="core_proces_status_2f3212_idx",
        ),
        migrations.RenameIndex(
            model_name="study",
            new_name="core_study_created_99a145_idx",
            old_name="core_study_created_by_created_at_idx",
        ),
        migrations.RenameIndex(
            model_name="symbol",
            new_name="core_symbol_ticker_a8b08b_idx",
            old_name="core_symbol_ticker_exchange_active_idx",
        ),
        migrations.RenameIndex(
            model_name="symbolscenario",
            new_name="core_symbol_scenari_2b8821_idx",
            old_name="core_symbscen_idx",
        ),
        migrations.RemoveField(
            model_name="alertdefinition",
            name="universes",
        ),
        migrations.RemoveField(
            model_name="backtest",
            name="universe",
        ),
        migrations.AlterField(
            model_name="alertdefinition",
            name="id",
            field=models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID"),
        ),
        migrations.AlterField(
            model_name="dailybar",
            name="close",
            field=models.DecimalField(
                decimal_places=6,
                max_digits=18,
                validators=[django.core.validators.MinValueValidator(0.0001)],
            ),
        ),
        migrations.AlterField(
            model_name="gamescenario",
            name="n2",
            field=models.PositiveIntegerField(
                default=3,
                help_text="Fenêtre N2 (jours), utilisée aussi pour la ligne flottante 2 bis.",
            ),
        ),
        migrations.AlterField(
            model_name="processingjob",
            name="job_type",
            field=models.CharField(
                choices=[
                    ("FETCH_BARS", "Fetch Daily Bars"),
                    ("COMPUTE_METRICS", "Compute Metrics"),
                    ("SYNC_MARKET_CAPS", "Sync Market Caps"),
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
        migrations.AlterField(
            model_name="processingjob",
            name="status",
            field=models.CharField(
                choices=[
                    ("PENDING", "Pending"),
                    ("RUNNING", "Running"),
                    ("DONE", "Done"),
                    ("FAILED", "Failed"),
                    ("CANCELLED", "Cancelled"),
                    ("KILLED", "Killed"),
                ],
                default="PENDING",
                max_length=16,
            ),
        ),
        migrations.AlterField(
            model_name="scenario",
            name="e",
            field=models.DecimalField(
                decimal_places=6,
                default=1,
                max_digits=18,
                validators=[django.core.validators.MinValueValidator(0.0001)],
            ),
        ),
        migrations.AlterField(
            model_name="scenario",
            name="n2",
            field=models.PositiveIntegerField(
                default=3,
                help_text="Fenêtre N2 (jours), utilisée aussi pour la ligne flottante 2 bis.",
            ),
        ),
        migrations.AlterField(
            model_name="study",
            name="name",
            field=models.CharField(max_length=120),
        ),
        migrations.AlterField(
            model_name="study",
            name="origin_scenario",
            field=models.ForeignKey(
                blank=True,
                help_text="Scénario source utilisé lors de la création (trace uniquement).",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="studies_origin",
                to="core.scenario",
            ),
        ),
        migrations.AlterField(
            model_name="study",
            name="origin_universe",
            field=models.ForeignKey(
                blank=True,
                help_text="Universe source utilisé lors de la création (trace uniquement).",
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="studies_origin",
                to="core.universe",
            ),
        ),
        migrations.AlterField(
            model_name="study",
            name="scenario",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name="studies",
                to="core.scenario",
            ),
        ),
        migrations.AddConstraint(
            model_name="scenario",
            constraint=models.CheckConstraint(condition=models.Q(("e__gt", 0)), name="scenario_e_gt_0"),
        ),
    ]
