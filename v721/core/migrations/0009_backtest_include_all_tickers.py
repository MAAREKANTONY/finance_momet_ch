from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0008_processing_jobs"),
    ]

    operations = [
        migrations.AddField(
            model_name="backtest",
            name="include_all_tickers",
            field=models.BooleanField(
                default=False,
                help_text="Backtest sur toutes les actions du scénario (ignore ratio_p >= X).",
            ),
        ),
    ]
