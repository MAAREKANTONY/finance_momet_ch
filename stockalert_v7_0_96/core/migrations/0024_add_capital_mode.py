from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0023_add_kf3_and_params"),
    ]

    operations = [
        migrations.AddField(
            model_name="backtest",
            name="capital_mode",
            field=models.CharField(
                choices=[("REINVEST", "Reinvest (capital évolutif)"), ("FIXED", "Fixed (capital initial constant)")],
                default="REINVEST",
                max_length=12,
            ),
        ),
        migrations.AddField(
            model_name="gamescenario",
            name="capital_mode",
            field=models.CharField(
                choices=[("REINVEST", "Reinvest (capital évolutif)"), ("FIXED", "Fixed (capital initial constant)")],
                default="REINVEST",
                max_length=12,
            ),
        ),
    ]
