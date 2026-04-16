from decimal import Decimal

import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0022_add_game_scenario"),
    ]

    operations = [
        # Scenario: Kf3 params
        migrations.AddField(
            model_name="scenario",
            name="n5f3",
            field=models.PositiveIntegerField(
                default=100,
                help_text="Kf3: fenêtre N5f3 (jours) pour max/min flottants (défaut 100).",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="crf3",
            field=models.DecimalField(
                decimal_places=4,
                default=Decimal("10"),
                max_digits=10,
                help_text="Kf3: indice de correction CRf3 (défaut 10).",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="nampL3",
            field=models.PositiveIntegerField(
                default=100,
                help_text="Kf3: NampL3 (jours) pour la moyenne des variations absolues (défaut 100).",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="baseL3",
            field=models.DecimalField(
                decimal_places=6,
                default=Decimal("0.02"),
                max_digits=12,
                validators=[django.core.validators.MinValueValidator(0.000001)],
                help_text="Kf3: base (défaut 0.02).",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="periodeL3",
            field=models.PositiveIntegerField(
                default=100,
                help_text="Kf3: période nominale (défaut 100).",
            ),
        ),

        # GameScenario: same params
        migrations.AddField(model_name="gamescenario", name="n5f3", field=models.PositiveIntegerField(default=100)),
        migrations.AddField(
            model_name="gamescenario",
            name="crf3",
            field=models.DecimalField(decimal_places=4, default=Decimal("10"), max_digits=10),
        ),
        migrations.AddField(model_name="gamescenario", name="nampL3", field=models.PositiveIntegerField(default=100)),
        migrations.AddField(
            model_name="gamescenario",
            name="baseL3",
            field=models.DecimalField(
                decimal_places=6,
                default=Decimal("0.02"),
                max_digits=12,
                validators=[django.core.validators.MinValueValidator(Decimal("0.000001"))],
            ),
        ),
        migrations.AddField(model_name="gamescenario", name="periodeL3", field=models.PositiveIntegerField(default=100)),

        # DailyMetric: Kf3 line
        migrations.AddField(
            model_name="dailymetric",
            name="Kf3",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
    ]
