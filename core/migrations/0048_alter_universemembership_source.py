from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0047_processingjob_enrich_metadata_job_type"),
    ]

    operations = [
        migrations.AlterField(
            model_name="universemembership",
            name="source",
            field=models.CharField(blank=True, default="", max_length=128),
        ),
    ]
