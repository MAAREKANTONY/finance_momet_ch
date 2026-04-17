from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0020_universe_state_align_description_updated_at"),
    ]

    operations = [
        migrations.RunSQL(
            sql=[
                # description
                "ALTER TABLE core_universe ADD COLUMN IF NOT EXISTS description text NOT NULL DEFAULT '';",
                # updated_at
                "ALTER TABLE core_universe ADD COLUMN IF NOT EXISTS updated_at timestamptz;",
                "UPDATE core_universe SET updated_at = NOW() WHERE updated_at IS NULL;",
                "ALTER TABLE core_universe ALTER COLUMN updated_at SET NOT NULL;",
            ],
            reverse_sql=[
                "ALTER TABLE core_universe DROP COLUMN IF EXISTS updated_at;",
                "ALTER TABLE core_universe DROP COLUMN IF EXISTS description;",
            ],
        ),
    ]
