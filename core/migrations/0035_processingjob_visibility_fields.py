from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0034_symbol_sector"),
    ]

    operations = [
        migrations.AddField(
            model_name="processingjob",
            name="last_checkpoint",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="processingjob",
            name="worker_hostname",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["status", "worker_hostname"], name="core_proces_status_2f3212_idx"),
        ),
    ]
