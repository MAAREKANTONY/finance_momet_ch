from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0038_add_slope_sell_thresholds"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="recent_high_drawdown_lookback_days",
            field=models.PositiveIntegerField(
                blank=True,
                help_text="Protection anti-chute : nombre de jours de cotation précédents utilisés pour calculer le plus haut récent. Le jour courant est exclu.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="recent_high_drawdown_max_drop_pct",
            field=models.DecimalField(
                blank=True,
                decimal_places=8,
                help_text="Protection anti-chute : pourcentage maximal de baisse autorisé par rapport au plus haut récent (ratio brut, ex: -0.10 = -10%).",
                max_digits=18,
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="recent_high_drawdown_lookback_days",
            field=models.PositiveIntegerField(
                blank=True,
                help_text="Protection anti-chute : nombre de jours de cotation précédents utilisés pour calculer le plus haut récent. Le jour courant est exclu.",
                null=True,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="recent_high_drawdown_max_drop_pct",
            field=models.DecimalField(
                blank=True,
                decimal_places=8,
                help_text="Protection anti-chute : pourcentage maximal de baisse autorisé par rapport au plus haut récent (ratio brut, ex: -0.10 = -10%). N'affecte la tradabilité du Game que via les alertes si utilisé dans les signaux.",
                max_digits=18,
                null=True,
            ),
        ),
    ]
