from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0003_formdraft"),
    ]

    operations = [
        migrations.AlterField(
            model_name="primarydocument",
            name="status",
            field=models.CharField(
                choices=[
                    ("draft", "Черновик"),
                    ("approval", "На утверждении"),
                    ("approved", "Утвержден"),
                    ("sent_accounting", "Отправлен в бухгалтерию"),
                    ("accepted", "Принят"),
                    ("rework", "Возвращен на доработку"),
                    ("uploaded", "Загружен поставщиком"),
                ],
                default="draft",
                max_length=32,
            ),
        ),
    ]
