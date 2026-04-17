from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0007_backtest_portfolio"),
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name="ProcessingJob",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                (
                    "job_type",
                    models.CharField(
                        choices=[
                            ("FETCH_BARS", "Fetch Daily Bars"),
                            ("COMPUTE_METRICS", "Compute Metrics"),
                            ("RUN_BACKTEST", "Run Backtest"),
                        ],
                        max_length=32,
                    ),
                ),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("PENDING", "Pending"),
                            ("RUNNING", "Running"),
                            ("DONE", "Done"),
                            ("FAILED", "Failed"),
                        ],
                        default="PENDING",
                        max_length=16,
                    ),
                ),
                ("task_id", models.CharField(blank=True, default="", max_length=64)),
                ("message", models.TextField(blank=True, default="")),
                ("error", models.TextField(blank=True, default="")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("started_at", models.DateTimeField(blank=True, null=True)),
                ("finished_at", models.DateTimeField(blank=True, null=True)),
                (
                    "backtest",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="jobs",
                        to="core.backtest",
                    ),
                ),
                (
                    "scenario",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="jobs",
                        to="core.scenario",
                    ),
                ),
                (
                    "created_by",
                    models.ForeignKey(
                        blank=True,
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name="processing_jobs",
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
            ],
            options={
                "ordering": ["-created_at"],
            },
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["status", "created_at"], name="core_proces_status_0b44b8_idx"),
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["job_type", "created_at"], name="core_proces_job_typ_4e2c89_idx"),
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["backtest", "created_at"], name="core_proces_backtes_9b22d2_idx"),
        ),
        migrations.AddIndex(
            model_name="processingjob",
            index=models.Index(fields=["scenario", "created_at"], name="core_proces_scenari_4db1b4_idx"),
        ),
    ]
