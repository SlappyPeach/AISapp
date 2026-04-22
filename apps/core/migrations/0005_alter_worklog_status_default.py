from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0004_alter_primarydocument_status"),
    ]

    operations = [
        migrations.AlterField(
            model_name="worklog",
            name="status",
            field=models.CharField(default="Запланировано", max_length=64),
        ),
    ]
