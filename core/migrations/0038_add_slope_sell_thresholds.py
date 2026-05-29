from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0037_historicalmarketcap"),
    ]

    operations = [
        migrations.AlterField(
            model_name="gamescenario",
            name="slope_threshold",
            field=models.DecimalField(
                decimal_places=8,
                default=Decimal("0.1"),
                help_text="Seuil de déclenchement achat utilisé à la fois pour la tradabilité du Game, SPa et SPVa (ratio brut, ex: 0.1 = 10%).",
                max_digits=18,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="slope_sell_threshold",
            field=models.DecimalField(
                blank=True,
                decimal_places=8,
                help_text="Seuil de déclenchement vente utilisé par SPv/SPVv. Si vide, le seuil d'achat est réutilisé. N'affecte pas la tradabilité du Game.",
                max_digits=18,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="slope_sell_threshold_basse",
            field=models.DecimalField(
                blank=True,
                decimal_places=8,
                help_text="Seuil de déclenchement vente — pente basse, utilisé par SPv_basse/SPVv_basse. Si vide, le seuil d'achat est réutilisé.",
                max_digits=18,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="gamescenario",
            name="slope_threshold_basse",
            field=models.DecimalField(
                decimal_places=8,
                default=Decimal("0.02"),
                help_text="Seuil de déclenchement achat — pente basse, utilisé par SPa_basse/SPVa_basse (ratio brut, ex: 0.02 = 2%).",
                max_digits=18,
            ),
        ),
        migrations.AlterField(
            model_name="scenario",
            name="slope_threshold",
            field=models.DecimalField(
                decimal_places=8,
                default=Decimal("0.1"),
                help_text="Seuil de déclenchement achat utilisé par SPa/SPVa (ratio brut, ex: 0.1 = 10%).",
                max_digits=18,
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="slope_sell_threshold",
            field=models.DecimalField(
                blank=True,
                decimal_places=8,
                help_text="Seuil de déclenchement vente utilisé par SPv/SPVv. Si vide, le seuil d'achat est réutilisé.",
                max_digits=18,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="slope_sell_threshold_basse",
            field=models.DecimalField(
                blank=True,
                decimal_places=8,
                help_text="Seuil de déclenchement vente — pente basse, utilisé par SPv_basse/SPVv_basse. Si vide, le seuil d'achat est réutilisé.",
                max_digits=18,
                null=True,
            ),
        ),
        migrations.AlterField(
            model_name="scenario",
            name="slope_threshold_basse",
            field=models.DecimalField(
                decimal_places=8,
                default=Decimal("0.02"),
                help_text="Seuil de déclenchement achat — pente basse, utilisé par SPa_basse/SPVa_basse (ratio brut, ex: 0.02 = 2%).",
                max_digits=18,
            ),
        ),
    ]
