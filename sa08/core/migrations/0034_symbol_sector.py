from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0033_add_warmup_days"),
    ]

    operations = [
        migrations.AddField(
            model_name="symbol",
            name="sector",
            field=models.CharField(blank=True, default="", max_length=120),
        ),
    ]
