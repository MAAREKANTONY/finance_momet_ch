from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0010_add_vc_and_k1f"),
    ]

    operations = [
        migrations.AddField(
            model_name="dailybar",
            name="volume",
            field=models.BigIntegerField(blank=True, null=True),
        ),
    ]
