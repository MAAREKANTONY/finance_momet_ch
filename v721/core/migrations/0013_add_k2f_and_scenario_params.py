from decimal import Decimal

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0012_merge_0011_add_fl_0011_add_volume"),
    ]

    operations = [
        # Scenario parameters for K2f
        migrations.AddField(
            model_name="scenario",
            name="n5",
            field=models.PositiveIntegerField(
                default=100,
                help_text="K2f: fenêtre N5 (jours) pour la somme des variations journalières.",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="k2j",
            field=models.PositiveIntegerField(
                default=10,
                help_text="K2f: fenêtre K2J (jours) de lissage (moyenne mobile) de la pré-ligne K2f.",
            ),
        ),
        migrations.AddField(
            model_name="scenario",
            name="cr",
            field=models.DecimalField(
                default=Decimal("10"),
                decimal_places=4,
                max_digits=10,
                help_text="K2f: indice de correction CR (défaut 10).",
            ),
        ),
        # DailyMetric field for the floating line
        migrations.AddField(
            model_name="dailymetric",
            name="K2f",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="K2f_pre",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
    ]
