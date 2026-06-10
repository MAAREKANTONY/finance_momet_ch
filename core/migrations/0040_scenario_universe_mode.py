from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0039_add_recent_high_drawdown_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="universe_mode",
            field=models.CharField(
                choices=[
                    ("STATIC_TICKERS", "Sélection statique de tickers"),
                    ("SP500_HISTORICAL_DYNAMIC", "S&P500 historique dynamique"),
                ],
                default="STATIC_TICKERS",
                help_text="Mode d'univers du scénario. Phase 1 stocke uniquement ce choix.",
                max_length=32,
            ),
        ),
    ]
