from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0013_add_k2f_and_scenario_params"),
    ]

    operations = [
        migrations.CreateModel(
            name="AlertDefinition",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=120, unique=True)),
                ("description", models.TextField(blank=True, default="")),
                ("alert_codes", models.CharField(blank=True, default="", max_length=300)),
                ("send_hour", models.PositiveIntegerField(default=18)),
                ("send_minute", models.PositiveIntegerField(default=0)),
                ("timezone", models.CharField(default="Asia/Jerusalem", max_length=64)),
                ("is_active", models.BooleanField(default=True)),
                ("last_sent_date", models.DateField(blank=True, null=True)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "ordering": ["name"],
            },
        ),
        migrations.AddField(
            model_name="alertdefinition",
            name="recipients",
            field=models.ManyToManyField(blank=True, related_name="alert_definitions", to="core.emailrecipient"),
        ),
        migrations.AddField(
            model_name="alertdefinition",
            name="scenarios",
            field=models.ManyToManyField(blank=True, related_name="alert_definitions", to="core.scenario"),
        ),
    ]
