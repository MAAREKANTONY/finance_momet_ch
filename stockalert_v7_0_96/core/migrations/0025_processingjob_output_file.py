from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0024_add_capital_mode"),
    ]

    operations = [
        migrations.AddField(
            model_name="processingjob",
            name="output_file",
            field=models.CharField(blank=True, default="", max_length=512),
        ),
        migrations.AddField(
            model_name="processingjob",
            name="output_name",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
    ]
