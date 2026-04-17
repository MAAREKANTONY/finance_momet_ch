from decimal import Decimal
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0009_backtest_include_all_tickers"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="vc",
            field=models.DecimalField(decimal_places=4, default=Decimal("0.5"), max_digits=6),
        ),
        migrations.AddField(
            model_name="dailymetric",
            name="K1f",
            field=models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True),
        ),
    ]
