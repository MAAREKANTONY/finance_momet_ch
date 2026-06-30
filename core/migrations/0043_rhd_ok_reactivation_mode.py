# Generated manually for RHD_OK reactivation mode.

import decimal
import django.core.validators
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0042_universe_coverage_validation"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="rhd_ok_reactivation_mode",
            field=models.CharField(
                choices=[("classic", "Classique"), ("rebound_confirmed", "Rebond confirmé")],
                default="classic",
                help_text="Mode de réactivation de RHD_OK après RHD_FAIL.",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="rhd_ok_rebound_threshold",
            field=models.DecimalField(
                decimal_places=8,
                default=decimal.Decimal("0.08"),
                help_text="Rebond minimum depuis le point bas après RHD_FAIL (ratio brut, ex: 0.08 = 8%).",
                max_digits=18,
                validators=[django.core.validators.MinValueValidator(decimal.Decimal("0"))],
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="rhd_ok_confirmation_days",
            field=models.PositiveSmallIntegerField(
                default=2,
                help_text="Nombre de jours de cotation consécutifs requis pour confirmer le rebond RHD_OK.",
                validators=[django.core.validators.MinValueValidator(1)],
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="rhd_ok_reentry_max_drawdown",
            field=models.DecimalField(
                decimal_places=8,
                default=decimal.Decimal("0.40"),
                help_text="Drawdown maximum autorisé à la réentrée RHD_OK en mode rebond confirmé.",
                max_digits=18,
                validators=[django.core.validators.MinValueValidator(decimal.Decimal("0"))],
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="rhd_ok_reactivation_mode",
            field=models.CharField(
                choices=[("classic", "Classique"), ("rebound_confirmed", "Rebond confirmé")],
                default="classic",
                help_text="Mode de réactivation de RHD_OK après RHD_FAIL.",
                max_length=32,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="rhd_ok_rebound_threshold",
            field=models.DecimalField(
                decimal_places=8,
                default=decimal.Decimal("0.08"),
                help_text="Rebond minimum depuis le point bas après RHD_FAIL (ratio brut, ex: 0.08 = 8%).",
                max_digits=18,
                validators=[django.core.validators.MinValueValidator(decimal.Decimal("0"))],
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="rhd_ok_confirmation_days",
            field=models.PositiveSmallIntegerField(
                default=2,
                help_text="Nombre de jours de cotation consécutifs requis pour confirmer le rebond RHD_OK.",
                validators=[django.core.validators.MinValueValidator(1)],
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="rhd_ok_reentry_max_drawdown",
            field=models.DecimalField(
                decimal_places=8,
                default=decimal.Decimal("0.40"),
                help_text="Drawdown maximum autorisé à la réentrée RHD_OK en mode rebond confirmé.",
                max_digits=18,
                validators=[django.core.validators.MinValueValidator(decimal.Decimal("0"))],
            ),
        ),
    ]
