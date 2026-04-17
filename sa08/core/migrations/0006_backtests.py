# Generated manually for V4_0_5 (Feature 1: Backtests models)
from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0005_scenario_symbols_default_and_logs"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="Backtest",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120)),
                ("description", models.TextField(blank=True, default="")),
                ("start_date", models.DateField(blank=True, null=True)),
                ("end_date", models.DateField(blank=True, null=True)),
                ("capital_total", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("capital_per_ticker", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("ratio_threshold", models.DecimalField(decimal_places=2, default=0, max_digits=6)),
                ("signal_lines", models.JSONField(blank=True, default=list)),
                ("close_positions_at_end", models.BooleanField(default=True)),
                ("settings", models.JSONField(blank=True, default=dict)),
                ("universe_snapshot", models.JSONField(blank=True, default=list)),
                ("results", models.JSONField(blank=True, default=dict)),
                ("status", models.CharField(choices=[("PENDING", "Pending"), ("RUNNING", "Running"), ("DONE", "Done"), ("FAILED", "Failed")], default="PENDING", max_length=10)),
                ("error_message", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="backtests", to=settings.AUTH_USER_MODEL)),
                ("scenario", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="backtests", to="core.scenario")),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="backtest",
            index=models.Index(fields=["scenario", "created_at"], name="core_backte_scenario_0f3a3b_idx"),
        ),
        migrations.AddIndex(
            model_name="backtest",
            index=models.Index(fields=["status", "created_at"], name="core_backte_status_5b87cb_idx"),
        ),
    ]
