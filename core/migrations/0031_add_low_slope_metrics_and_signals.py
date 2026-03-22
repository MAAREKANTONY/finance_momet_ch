from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0030_add_slope_vrai_and_spv_signals"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="npente_basse",
            field=models.PositiveIntegerField(default=20, help_text="Nombre de jours utilisés pour calculer SUM_SLOPE_BASSE et SLOPE_VRAI_BASSE."),
        ),
        migrations.AddField(
            model_name="scenario",
            name="slope_threshold_basse",
            field=models.DecimalField(decimal_places=8, default=Decimal("0.02"), help_text="Seuil de pente basse utilisé par SPa_basse/SPv_basse et SPVa_basse/SPVv_basse (ratio brut, ex: 0.02 = 2%).", max_digits=18),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="npente_basse",
            field=models.PositiveIntegerField(default=20, help_text="Nombre de jours utilisés pour calculer SUM_SLOPE_BASSE et SLOPE_VRAI_BASSE."),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="slope_threshold_basse",
            field=models.DecimalField(decimal_places=8, default=Decimal("0.02"), help_text="Seuil de pente basse utilisé par SPa_basse/SPv_basse et SPVa_basse/SPVv_basse (ratio brut, ex: 0.02 = 2%).", max_digits=18),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="sum_slope_basse",
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="slope_vrai_basse",
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
    ]
