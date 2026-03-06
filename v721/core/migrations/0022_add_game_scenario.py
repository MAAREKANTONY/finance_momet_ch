from decimal import Decimal

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0021_universe_add_description_updated_at_db"),
    ]

    operations = [
        migrations.CreateModel(
            name="GameScenario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120)),
                ("description", models.TextField(blank=True, default="")),
                ("study_days", models.PositiveIntegerField(default=1000)),
                ("active", models.BooleanField(default=True)),
                (
                    "tradability_threshold",
                    models.DecimalField(
                        decimal_places=6,
                        default=Decimal("0"),
                        help_text="Seuil de tradabilité (BMD >= seuil => OK).",
                        max_digits=12,
                        validators=[django.core.validators.MinValueValidator(Decimal("0"))],
                    ),
                ),
                ("email_recipients", models.TextField(blank=True, default="")),
                ("a", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("b", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("c", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("d", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                (
                    "e",
                    models.DecimalField(
                        decimal_places=6,
                        default=1,
                        max_digits=18,
                        validators=[django.core.validators.MinValueValidator(0.0001)],
                    ),
                ),
                ("vc", models.DecimalField(decimal_places=4, default=Decimal("0.5"), max_digits=6)),
                ("fl", models.DecimalField(decimal_places=4, default=Decimal("0.5"), max_digits=6)),
                ("n1", models.PositiveIntegerField(default=5)),
                ("n2", models.PositiveIntegerField(default=3)),
                ("n3", models.PositiveIntegerField(default=0)),
                ("n4", models.PositiveIntegerField(default=0)),
                ("n5", models.PositiveIntegerField(default=100)),
                ("k2j", models.PositiveIntegerField(default=10)),
                ("cr", models.DecimalField(decimal_places=4, default=Decimal("10"), max_digits=10)),
                ("m_v", models.PositiveIntegerField(default=20)),
                (
                    "engine_scenario",
                    models.OneToOneField(
                        blank=True,
                        null=True,
                        on_delete=models.deletion.SET_NULL,
                        related_name="game_scenario",
                        to="core.scenario",
                    ),
                ),
                ("capital_total", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("capital_per_ticker", models.DecimalField(decimal_places=2, default=0, max_digits=18)),
                ("signal_lines", models.JSONField(blank=True, default=list)),
                ("close_positions_at_end", models.BooleanField(default=True)),
                ("settings", models.JSONField(blank=True, default=dict)),
                ("last_run_at", models.DateTimeField(blank=True, null=True)),
                ("last_run_status", models.CharField(blank=True, default="", max_length=20)),
                ("last_run_message", models.TextField(blank=True, default="")),
                ("today_results", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="gamescenario",
            index=models.Index(fields=["active", "last_run_at"], name="core_gamesce_active_idx"),
        ),
    ]
