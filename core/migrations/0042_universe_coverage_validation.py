from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ("core", "0041_universe_definition_membership"),
    ]

    operations = [
        migrations.CreateModel(
            name="UniverseImportBatch",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("provider", models.CharField(blank=True, default="", max_length=64)),
                ("source_name", models.CharField(blank=True, default="", max_length=120)),
                ("source_reference", models.CharField(blank=True, default="", max_length=255)),
                ("period_start", models.DateField()),
                ("period_end", models.DateField()),
                ("expected_member_count", models.PositiveIntegerField(default=500)),
                ("imported_member_count", models.PositiveIntegerField(default=0)),
                ("mapped_member_count", models.PositiveIntegerField(default=0)),
                ("unmapped_member_count", models.PositiveIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("IMPORTED", "Imported"),
                            ("VALIDATED", "Validated"),
                            ("PARTIAL", "Partial"),
                            ("FAILED", "Failed"),
                            ("STALE", "Stale"),
                        ],
                        default="IMPORTED",
                        max_length=16,
                    ),
                ),
                ("validated_at", models.DateTimeField(blank=True, null=True)),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "universe",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="import_batches",
                        to="core.universedefinition",
                    ),
                ),
            ],
            options={
                "ordering": ["universe__code", "period_start", "period_end", "id"],
            },
        ),
        migrations.CreateModel(
            name="UniverseCoverageSnapshot",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("coverage_date", models.DateField()),
                ("expected_member_count", models.PositiveIntegerField(default=500)),
                ("actual_member_count", models.PositiveIntegerField(default=0)),
                ("mapped_member_count", models.PositiveIntegerField(default=0)),
                ("unmapped_member_count", models.PositiveIntegerField(default=0)),
                (
                    "status",
                    models.CharField(
                        choices=[
                            ("IMPORTED", "Imported"),
                            ("VALIDATED", "Validated"),
                            ("PARTIAL", "Partial"),
                            ("FAILED", "Failed"),
                            ("STALE", "Stale"),
                        ],
                        default="IMPORTED",
                        max_length=16,
                    ),
                ),
                ("metadata", models.JSONField(blank=True, default=dict)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                (
                    "import_batch",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        related_name="coverage_snapshots",
                        to="core.universeimportbatch",
                    ),
                ),
                (
                    "universe",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name="coverage_snapshots",
                        to="core.universedefinition",
                    ),
                ),
            ],
            options={
                "ordering": ["universe__code", "coverage_date"],
            },
        ),
        migrations.AddConstraint(
            model_name="universeimportbatch",
            constraint=models.CheckConstraint(
                condition=models.Q(("period_end__gte", models.F("period_start"))),
                name="uib_period_end_gte_start",
            ),
        ),
        migrations.AddIndex(
            model_name="universeimportbatch",
            index=models.Index(fields=["universe", "period_start", "period_end"], name="core_uib_period_idx"),
        ),
        migrations.AddIndex(
            model_name="universeimportbatch",
            index=models.Index(fields=["universe", "status"], name="core_uib_status_idx"),
        ),
        migrations.AddConstraint(
            model_name="universecoveragesnapshot",
            constraint=models.UniqueConstraint(
                fields=("universe", "coverage_date"),
                name="uniq_universe_coverage_date",
            ),
        ),
        migrations.AddIndex(
            model_name="universecoveragesnapshot",
            index=models.Index(fields=["universe", "coverage_date"], name="core_ucs_date_idx"),
        ),
        migrations.AddIndex(
            model_name="universecoveragesnapshot",
            index=models.Index(fields=["universe", "status"], name="core_ucs_status_idx"),
        ),
    ]
