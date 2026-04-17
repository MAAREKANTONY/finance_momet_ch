from django.db import migrations, models
import django.db.models.deletion

class Migration(migrations.Migration):
    initial = True
    dependencies = []
    operations = [
        migrations.CreateModel(
            name="EmailRecipient",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("email", models.EmailField(max_length=254, unique=True)),
                ("active", models.BooleanField(default=True)),
            ],
        ),
        migrations.CreateModel(
            name="Scenario",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120)),
                ("description", models.TextField(blank=True, default="")),
                ("a", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("b", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("c", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("d", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("e", models.DecimalField(decimal_places=6, default=1, max_digits=18)),
                ("n1", models.PositiveIntegerField(default=5)),
                ("n2", models.PositiveIntegerField(default=3)),
                ("n3", models.PositiveIntegerField(default=0)),
                ("history_years", models.PositiveIntegerField(default=2)),
                ("active", models.BooleanField(default=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
        migrations.CreateModel(
            name="Symbol",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("ticker", models.CharField(max_length=64)),
                ("exchange", models.CharField(blank=True, default="", max_length=64)),
                ("name", models.CharField(blank=True, default="", max_length=200)),
                ("instrument_type", models.CharField(blank=True, default="", max_length=64)),
                ("country", models.CharField(blank=True, default="", max_length=64)),
                ("currency", models.CharField(blank=True, default="", max_length=16)),
                ("active", models.BooleanField(default=True)),
            ],
            options={"unique_together": {("ticker", "exchange")}},
        ),
        migrations.CreateModel(
            name="DailyBar",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField()),
                ("open", models.DecimalField(decimal_places=6, max_digits=18)),
                ("high", models.DecimalField(decimal_places=6, max_digits=18)),
                ("low", models.DecimalField(decimal_places=6, max_digits=18)),
                ("close", models.DecimalField(decimal_places=6, max_digits=18)),
                ("change_amount", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("change_pct", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("source", models.CharField(default="twelvedata", max_length=64)),
                ("ingested_at", models.DateTimeField(auto_now_add=True)),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
            options={"unique_together": {("symbol", "date")}},
        ),
        migrations.CreateModel(
            name="DailyMetric",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField()),
                ("P", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("M", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("M1", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("X", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("X1", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("T", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("Q", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("S", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("K1", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("K2", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("K3", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("K4", models.DecimalField(blank=True, decimal_places=6, max_digits=18, null=True)),
                ("computed_at", models.DateTimeField(auto_now=True)),
                ("scenario", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.scenario")),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
            options={"unique_together": {("symbol", "scenario", "date")}},
        ),
        migrations.CreateModel(
            name="Alert",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("date", models.DateField()),
                ("alerts", models.CharField(blank=True, default="", max_length=255)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("scenario", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.scenario")),
                ("symbol", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to="core.symbol")),
            ],
            options={"unique_together": {("symbol", "scenario", "date")}},
        ),
        migrations.AddIndex(
            model_name="symbol",
            index=models.Index(fields=["ticker", "exchange", "active"], name="core_symbol_ticker_exchange_active_idx"),
        ),
        migrations.AddIndex(
            model_name="dailybar",
            index=models.Index(fields=["symbol", "date"], name="core_dailybar_symbol_date_idx"),
        ),
        migrations.AddIndex(
            model_name="dailymetric",
            index=models.Index(fields=["symbol", "scenario", "date"], name="core_dailymetric_symbol_scenario_date_idx"),
        ),
        migrations.AddIndex(
            model_name="alert",
            index=models.Index(fields=["date", "scenario"], name="core_alert_date_scenario_idx"),
        ),
    ]
