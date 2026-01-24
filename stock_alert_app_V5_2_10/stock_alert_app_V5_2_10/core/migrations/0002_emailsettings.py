from django.db import migrations, models

class Migration(migrations.Migration):
    dependencies = [
        ("core", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="EmailSettings",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("send_hour", models.PositiveIntegerField(default=18)),
                ("send_minute", models.PositiveIntegerField(default=0)),
                ("timezone", models.CharField(default="Asia/Jerusalem", max_length=64)),
                ("last_sent_date", models.DateField(blank=True, null=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
        ),
    ]
