from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0005_scenarios_symbols_logs"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="backtest_default_capital",
            field=models.DecimalField(decimal_places=2, default=10000, max_digits=18),
        ),
    ]
