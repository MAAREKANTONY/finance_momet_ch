from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0048_alter_universemembership_source"),
    ]

    operations = [
        migrations.AddField(
            model_name="symbol",
            name="name_en",
            field=models.CharField(blank=True, default="", max_length=200),
        ),
    ]
