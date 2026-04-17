from decimal import Decimal

from django.core.validators import MinValueValidator
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0025_processingjob_output_file"),
    ]

    operations = [
        migrations.AddField(
            model_name="gamescenario",
            name="npente",
            field=models.PositiveIntegerField(
                default=100,
                help_text="Nombre de jours utilisés pour calculer la moyenne des pentes.",
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="slope_threshold",
            field=models.DecimalField(
                decimal_places=8,
                default=Decimal("0"),
                help_text="Seuil minimal de pente moyenne (ratio brut, ex: 0.001 = 0.1%).",
                max_digits=18,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="presence_threshold_pct",
            field=models.DecimalField(
                decimal_places=4,
                default=Decimal("30"),
                help_text="Seuil minimal de temps de présence en position (%).",
                max_digits=8,
                validators=[MinValueValidator(Decimal("0"))],
            ),
        ),
    ]
