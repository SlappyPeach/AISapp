from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


def seed_document_types(apps, schema_editor):
    DocumentType = apps.get_model("core", "DocumentType")
    default_types = [
        {
            "code": "smr_contract",
            "name": "Договор СМР",
            "prefix": "SMR",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": False,
            "description": "Базовый договор на строительно-монтажные работы.",
        },
        {
            "code": "supply_contract",
            "name": "Договор поставки",
            "prefix": "SPC",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": False,
            "description": "Договор на поставку материалов с контрагентом.",
        },
        {
            "code": "procurement_request",
            "name": "Заявка поставщику",
            "prefix": "REQ",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": True,
            "description": "Внутренняя заявка на закупку материалов.",
        },
        {
            "code": "invoice",
            "name": "Счет",
            "prefix": "INV",
            "available_for_upload": True,
            "available_for_generation": True,
            "requires_items": True,
            "description": "Счет на оплату материалов.",
        },
        {
            "code": "vat_invoice",
            "name": "Счет-фактура",
            "prefix": "SF",
            "available_for_upload": True,
            "available_for_generation": True,
            "requires_items": True,
            "description": "Документ НДС по поставке материалов.",
        },
        {
            "code": "goods_waybill",
            "name": "Товарная накладная",
            "prefix": "TN",
            "available_for_upload": True,
            "available_for_generation": True,
            "requires_items": True,
            "description": "Отгрузочная товарная накладная.",
        },
        {
            "code": "receipt_order",
            "name": "Приходный ордер",
            "prefix": "REC",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": True,
            "description": "Внутренний ордер на прием материалов на склад.",
        },
        {
            "code": "receipt_invoice",
            "name": "Приходная накладная",
            "prefix": "PN",
            "available_for_upload": True,
            "available_for_generation": True,
            "requires_items": True,
            "description": "Приходная накладная по складскому приему.",
        },
        {
            "code": "issue_note",
            "name": "Требование-накладная",
            "prefix": "ISS",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": True,
            "description": "Документ отпуска материалов на участок.",
        },
        {
            "code": "write_off_act",
            "name": "Акт списания",
            "prefix": "WO",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": True,
            "description": "Акт списания материалов по нормам.",
        },
        {
            "code": "ppe_statement",
            "name": "Ведомость спецодежды",
            "prefix": "PPE",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": True,
            "description": "Документ выдачи СИЗ и спецодежды.",
        },
        {
            "code": "summary_report",
            "name": "Сводный отчет",
            "prefix": "SUM",
            "available_for_upload": False,
            "available_for_generation": False,
            "requires_items": False,
            "description": "Служебный системный тип для аналитических отчетов.",
        },
    ]
    for item in default_types:
        DocumentType.objects.update_or_create(
            code=item["code"],
            defaults={
                "name": item["name"],
                "prefix": item["prefix"],
                "is_active": True,
                "available_for_upload": item["available_for_upload"],
                "available_for_generation": item["available_for_generation"],
                "requires_items": item["requires_items"],
                "description": item["description"],
            },
        )


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0001_initial"),
    ]

    operations = [
        migrations.CreateModel(
            name="DocumentType",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("code", models.CharField(max_length=64, unique=True)),
                ("name", models.CharField(max_length=128, unique=True)),
                ("prefix", models.CharField(max_length=16, unique=True)),
                ("is_active", models.BooleanField(default=True)),
                ("available_for_upload", models.BooleanField(default=False)),
                ("available_for_generation", models.BooleanField(default=False)),
                ("requires_items", models.BooleanField(default=True)),
                ("description", models.TextField(blank=True)),
            ],
            options={"ordering": ["name"]},
        ),
        migrations.CreateModel(
            name="PrimaryDocument",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
                ("number", models.CharField(max_length=128, unique=True)),
                ("doc_date", models.DateField()),
                ("site_name", models.CharField(blank=True, max_length=255)),
                ("basis_reference", models.CharField(blank=True, max_length=255)),
                ("amount", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("vat_amount", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("status", models.CharField(choices=[("draft", "Черновик"), ("approval", "На утверждении"), ("approved", "Утвержден"), ("sent_accounting", "Отправлен в бухгалтерию"), ("accepted", "Принят"), ("rework", "Возвращен на доработку"), ("uploaded", "Загружен")], default="draft", max_length=32)),
                ("notes", models.TextField(blank=True)),
                ("created_by", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="primary_documents", to=settings.AUTH_USER_MODEL)),
                ("document_type", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="primary_documents", to="core.documenttype")),
                ("procurement_request", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="primary_documents", to="core.procurementrequest")),
                ("stock_receipt", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="primary_documents", to="core.stockreceipt")),
                ("supplier", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="primary_documents", to="core.supplier")),
                ("supply_contract", models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name="primary_documents", to="core.supplycontract")),
            ],
            options={"ordering": ["-doc_date", "-id"]},
        ),
        migrations.CreateModel(
            name="PrimaryDocumentLine",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("quantity", models.DecimalField(decimal_places=3, max_digits=14)),
                ("unit_price", models.DecimalField(decimal_places=2, default=0, max_digits=14)),
                ("notes", models.CharField(blank=True, max_length=255)),
                ("document", models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name="lines", to="core.primarydocument")),
                ("material", models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name="primary_document_lines", to="core.material")),
            ],
            options={"ordering": ["material__code"]},
        ),
        migrations.RunPython(seed_document_types, migrations.RunPython.noop),
    ]
