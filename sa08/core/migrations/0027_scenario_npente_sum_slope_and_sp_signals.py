from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0026_gamescenario_npente_and_thresholds"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="npente",
            field=models.PositiveIntegerField(default=100, help_text="Nombre de jours utilisés pour calculer SUM((P(t)-P(t-1))/P(t-1)) pour SPa/SPv."),
        ),
        migrations.AddField(
            model_name="scenario",
            name="slope_threshold",
            field=models.DecimalField(decimal_places=8, default=Decimal("0.1"), help_text="Seuil de pente utilisé par SPa/SPv (ratio brut, ex: 0.1 = 10%).", max_digits=18),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="sum_slope",
            field=models.DecimalField(blank=True, decimal_places=12, max_digits=18, null=True),
        ),
        migrations.AlterField(
            model_name="gamescenario",
            name="slope_threshold",
            field=models.DecimalField(decimal_places=8, default=Decimal("0.1"), help_text="Seuil de pente utilisé à la fois pour la tradabilité du Game et pour SPa/SPv (ratio brut, ex: 0.1 = 10%).", max_digits=18),
        ),
    ]
