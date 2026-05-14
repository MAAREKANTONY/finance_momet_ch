from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0036_processingjob_game_scenario"),
    ]

    operations = [
        migrations.CreateModel(
            name="HistoricalMarketCap",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField()),
                ("market_cap", models.DecimalField(decimal_places=2, max_digits=24)),
                ("currency", models.CharField(blank=True, default="", max_length=8)),
                ("provider", models.CharField(default="eodhd", max_length=32)),
                ("provider_symbol", models.CharField(blank=True, default="", max_length=64)),
                ("source_payload", models.JSONField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="historical_market_caps", to="core.symbol")),
            ],
            options={
                "indexes": [models.Index(fields=["provider", "symbol", "date"], name="core_hmcap_prov_sym_dt_idx")],
            },
        ),
        migrations.AddConstraint(
            model_name="historicalmarketcap",
            constraint=models.UniqueConstraint(fields=("provider", "symbol", "date"), name="historical_market_cap_unique_provider_symbol_date"),
        ),
    ]
