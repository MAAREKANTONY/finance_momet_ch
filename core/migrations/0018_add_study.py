from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0017_universe_and_scope_decoupling"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.AddField(
            model_name="scenario",
            name="is_study_clone",
            field=models.BooleanField(default=False),
        ),
        migrations.CreateModel(
            name="Study",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=160)),
                ("description", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("created_by", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="studies", to=settings.AUTH_USER_MODEL)),
                ("origin_scenario", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="+", to="core.scenario")),
                ("origin_universe", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="+", to="core.universe")),
                ("scenario", models.OneToOneField(on_delete=django.db.models.deletion.PROTECT, related_name="study_container", to="core.scenario")),
            ],
            options={
                "indexes": [
                    models.Index(fields=["created_by", "created_at"], name="core_study_created_by_created_at_idx")
                ],
                "ordering": ["-created_at"],
            },
        ),
    ]
