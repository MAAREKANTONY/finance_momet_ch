from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0003_trend_indicators"),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="last_computed_config_hash",
            field=models.CharField(blank=True, default="", max_length=64),
        ),
        migrations.AddField(
            model_name="scenario",
            name="last_full_recompute_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
    ]
