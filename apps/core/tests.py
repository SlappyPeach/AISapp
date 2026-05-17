from __future__ import annotations

import json
from datetime import timedelta
from decimal import Decimal
from io import StringIO
from zipfile import ZipFile

from django.conf import settings
from django.core.management import call_command
from django.db.models import Q, Sum
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from .exports import Exporter
from .forms import MaterialForm, ProcurementRequestCreateForm, SupplierForm, UserForm, WriteOffCreateForm
from .models import (
    AuditLog,
    DocumentRecord,
    DocumentStatus,
    DocumentType,
    Material,
    MaterialNorm,
    Notification,
    NotificationType,
    PPEIssuance,
    PPEIssuanceLine,
    PrimaryDocument,
    ProcurementRequest,
    SiteMaterialRequest,
    SMRContract,
    StockIssue,
    StockMovement,
    StockReceipt,
    Supplier,
    SupplierDocument,
    SupplyContract,
    User,
    Worker,
    WorkAcceptanceAct,
    WorkLog,
    WriteOffAct,
    WriteOffTemplateVariant,
)
from .reporting import report_ppe_scoped
from .services import (
    create_backup_payload,
    create_ppe_issuance,
    create_work_log,
    create_primary_document,
    create_procurement_request,
    create_site_material_request,
    create_stock_issue,
    create_stock_receipt,
    create_supplier_document,
    create_work_acceptance,
    create_writeoff,
    load_operation_draft,
    restore_backup_payload,
    save_operation_draft,
    transition_document,
    workflow_allowed_statuses,
)


def material_items(
    *,
    code: str = "MAT-001",
    quantity: str = "1",
    unit_price: str = "100",
    notes: str = "",
) -> str:
    return json.dumps(
        [
            {
                "material_code": code,
                "quantity": quantity,
                "unit_price": unit_price,
                "notes": notes,
            }
        ],
        ensure_ascii=False,
    )


class WorkflowTests(TestCase):
    def setUp(self) -> None:
        self.supplier = Supplier.objects.create(name='ООО "Тест-Снаб"')
        self.user = User.objects.create_user(username="site", password="site123", role="site_manager", site_name="Участок 12")
        self.warehouse = User.objects.create_user(username="warehouse", password="warehouse123", role="warehouse")
        self.director = User.objects.create_user(username="director", password="director123", role="director")
        self.accounting = User.objects.create_user(username="accounting", password="accounting123", role="accounting")
        self.invoice_type, _created = DocumentType.objects.get_or_create(
            code="invoice",
            defaults={
                "name": "Счет",
                "prefix": "INV",
                "available_for_generation": True,
                "available_for_upload": True,
                "requires_items": True,
            },
        )
        self.material = Material.objects.create(code="MAT-001", name="Кабель", unit="м", price=100, min_stock=5)
        self.contract = SMRContract.objects.create(
            number="SMR-001",
            contract_date=timezone.localdate(),
            customer_name="Заказчик",
            subject="Монтаж",
            work_type="Прокладка кабеля",
            amount=Decimal("150000"),
            created_by=self.user,
        )
        MaterialNorm.objects.create(work_type="Прокладка кабеля", material=self.material, norm_per_unit=Decimal("2.5"), unit="м")

    def test_procurement_request_creates_document_record(self) -> None:
        request = create_procurement_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "Тестовая заявка",
                "items": material_items(quantity="10", unit_price="100", notes="Для монтажа"),
            },
        )
        self.assertTrue(DocumentRecord.objects.filter(entity_type="procurement_request", entity_id=request.id).exists())
        self.assertEqual(request.lines.count(), 1)

    def test_invalid_initial_status_is_rejected(self) -> None:
        with self.assertRaisesMessage(ValueError, "статусы"):
            create_procurement_request(
                user=self.user,
                cleaned_data={
                    "request_date": timezone.localdate(),
                    "site_name": "Участок 12",
                    "contract": self.contract,
                    "supplier": self.supplier,
                    "status": DocumentStatus.ACCEPTED,
                    "notes": "Некорректный старт",
                    "items": material_items(quantity="10", unit_price="100", notes="Для монтажа"),
                },
            )

    def test_site_material_request_on_approval_notifies_warehouse(self) -> None:
        request = create_site_material_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "status": DocumentStatus.APPROVAL,
                "notes": "Нужно выдать",
                "items": material_items(quantity="2", unit_price="100", notes="Уведомление"),
            },
        )

        self.assertTrue(
            Notification.objects.filter(
                user=self.warehouse,
                kind=NotificationType.ACTION_REQUIRED,
                entity_type="site_material_request",
                entity_id=request.id,
                is_read=False,
            ).exists()
        )

    def test_rework_status_notifies_document_creator(self) -> None:
        request = create_procurement_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.APPROVAL,
                "notes": "Вернуть на доработку",
                "items": material_items(quantity="10", unit_price="100", notes="Доработка"),
            },
        )
        Notification.objects.all().delete()
        record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=request.id)

        transition_document(user=self.director, record=record, new_status=DocumentStatus.REWORK)

        self.assertTrue(
            Notification.objects.filter(
                user=self.user,
                kind=NotificationType.ACTION_REQUIRED,
                entity_type="procurement_request",
                entity_id=request.id,
                is_read=False,
            ).exists()
        )

    def test_low_stock_notification_is_created_after_stock_issue(self) -> None:
        receipt = create_stock_receipt(
            user=self.warehouse,
            cleaned_data={
                "receipt_date": timezone.localdate(),
                "supplier": self.supplier,
                "supplier_document": None,
                "primary_document": None,
                "status": DocumentStatus.DRAFT,
                "notes": "Запас",
                "items": material_items(quantity="6", unit_price="100", notes="Склад"),
            },
        )

        create_stock_issue(
            user=self.warehouse,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": "Участок 12",
                "site_request": None,
                "contract": self.contract,
                "stock_receipt": receipt,
                "received_by_name": "Получатель",
                "status": DocumentStatus.DRAFT,
                "notes": "До минимума",
                "items": material_items(quantity="1", unit_price="100", notes="Выдача"),
            },
        )

        self.assertTrue(
            Notification.objects.filter(
                user=self.warehouse,
                kind=NotificationType.LOW_STOCK,
                entity_type="material",
                entity_id=self.material.id,
                is_read=False,
            ).exists()
        )

    def test_document_record_stores_workflow_route_metadata(self) -> None:
        request = create_procurement_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="3", unit_price="100", notes="route"),
            },
        )
        record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=request.id)
        self.assertIn("workflow_created_by", record.metadata_json)
        self.assertIn("workflow_approved_by", record.metadata_json)
        self.assertIn("workflow_sent_accounting_by", record.metadata_json)
        self.assertIn("workflow_view_only", record.metadata_json)
        self.assertIn("workflow_route", record.metadata_json)

    def test_smr_contract_export_uses_docx_template(self) -> None:
        if not settings.DOCUMENT_TEMPLATES_DIR.exists():
            self.skipTest("Document templates directory is not available.")

        path = Exporter().export_document("smr_contract", self.contract.id)

        with ZipFile(path) as archive:
            document_xml = archive.read("word/document.xml").decode("utf-8")
        self.assertIn(self.contract.number, document_xml)
        self.assertIn("Заказчик", document_xml)
        self.assertNotIn("{{", document_xml)

    def test_site_material_report_export_uses_xlsx_template(self) -> None:
        if not settings.DOCUMENT_TEMPLATES_DIR.exists():
            self.skipTest("Document templates directory is not available.")

        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=self.material,
            quantity_delta=Decimal("7"),
            location_name=self.user.site_name,
            source_type="seed",
            source_id=7,
            unit_price=Decimal("100"),
            created_by=self.user,
        )

        path = Exporter().export_report(
            "site_material_report",
            {
                "date_from": timezone.localdate().replace(day=1),
                "date_to": timezone.localdate(),
                "location_name": self.user.site_name,
            },
            user=self.user,
        )

        with ZipFile(path) as archive:
            workbook_xml = "\n".join(
                archive.read(name).decode("utf-8")
                for name in archive.namelist()
                if name.endswith(".xml")
            )
        self.assertIn(self.user.site_name, workbook_xml)
        self.assertIn(self.material.code, workbook_xml)
        self.assertNotIn("{{", workbook_xml)

    def test_invoice_export_uses_xlsx_template(self) -> None:
        if not (settings.DOCUMENT_TEMPLATES_DIR / "Счет на оплату_шаблон.xlsx").exists():
            self.skipTest("Invoice XLSX template is not available.")

        document = create_primary_document(
            user=self.user,
            cleaned_data={
                "document_type": self.invoice_type,
                "doc_date": timezone.localdate(),
                "supplier": self.supplier,
                "request": None,
                "supply_contract": None,
                "stock_receipt": None,
                "site_name": self.user.site_name,
                "amount": Decimal("200"),
                "vat_amount": Decimal("40"),
                "status": DocumentStatus.DRAFT,
                "notes": "Оплата материалов",
                "items": material_items(quantity="2", unit_price="100", notes="Экспорт"),
            },
        )

        path = Exporter().export_document("primary_document", document.id)

        self.assertEqual(path.suffix, ".xlsx")
        with ZipFile(path) as archive:
            workbook_xml = "\n".join(
                archive.read(name).decode("utf-8")
                for name in archive.namelist()
                if name.endswith(".xml")
            )
        self.assertIn(document.number, workbook_xml)
        self.assertIn(self.material.name, workbook_xml)
        self.assertNotIn("{{", workbook_xml)

    def test_payment_order_export_uses_docx_template(self) -> None:
        if not (settings.DOCUMENT_TEMPLATES_DIR / "Платежное поручение_шаблон.docx").exists():
            self.skipTest("Payment order DOCX template is not available.")

        payment_type, _created = DocumentType.objects.get_or_create(
            code="payment_order",
            defaults={
                "name": "Платежное поручение",
                "prefix": "PAY",
                "available_for_generation": True,
                "available_for_upload": False,
                "requires_items": False,
            },
        )
        document = create_primary_document(
            user=self.user,
            cleaned_data={
                "document_type": payment_type,
                "doc_date": timezone.localdate(),
                "supplier": self.supplier,
                "request": None,
                "supply_contract": None,
                "stock_receipt": None,
                "site_name": self.user.site_name,
                "amount": Decimal("1200"),
                "vat_amount": Decimal("0"),
                "status": DocumentStatus.DRAFT,
                "notes": "Оплата по счету",
                "items": "",
            },
        )

        path = Exporter().export_document("primary_document", document.id)

        self.assertEqual(path.suffix, ".docx")
        with ZipFile(path) as archive:
            document_xml = archive.read("word/document.xml").decode("utf-8")
        self.assertIn(document.number, document_xml)
        self.assertIn(self.supplier.name, document_xml)
        self.assertNotIn("{{", document_xml)

    def test_writeoff_can_use_production_economic_template(self) -> None:
        if not (settings.DOCUMENT_TEMPLATES_DIR / "Акт списания материалов на производственно-хозяйственные нужды_шаблон.docx").exists():
            self.skipTest("Production-economic write-off template is not available.")

        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=self.material,
            quantity_delta=Decimal("20"),
            location_name="Участок 12",
            source_type="seed",
            source_id=9,
            unit_price=Decimal("100"),
            created_by=self.user,
        )
        act = create_writeoff(
            user=self.user,
            cleaned_data={
                "act_date": timezone.localdate(),
                "contract": self.contract,
                "template_variant": WriteOffTemplateVariant.PRODUCTION_ECONOMIC,
                "site_name": "Участок 12",
                "work_type": "Прокладка кабеля",
                "work_volume": Decimal("3"),
                "volume_unit": "этап",
                "status": DocumentStatus.DRAFT,
                "notes": "Производственно-хозяйственные нужды",
            },
        )

        path = Exporter().export_document("write_off", act.id)

        with ZipFile(path) as archive:
            document_xml = archive.read("word/document.xml").decode("utf-8")
        self.assertIn(act.number, document_xml)
        self.assertIn(self.material.code, document_xml)
        self.assertNotIn(self.contract.number, document_xml)
        self.assertNotIn("{{", document_xml)

    def test_supply_contract_workflow_entry_is_limited_to_director_or_admin(self) -> None:
        supply_contract = SupplyContract.objects.create(
            number="SUP-WF-001",
            contract_date=timezone.localdate(),
            supplier=self.supplier,
            related_smr_contract=self.contract,
            amount=Decimal("1000"),
            status=DocumentStatus.DRAFT,
        )
        record = DocumentRecord.objects.get(entity_type="supply_contract", entity_id=supply_contract.id)

        site_manager_statuses = [value for value, _label in workflow_allowed_statuses(self.user, record)]
        director_statuses = [value for value, _label in workflow_allowed_statuses(self.director, record)]

        self.assertNotIn(DocumentStatus.APPROVAL, site_manager_statuses)
        self.assertIn(DocumentStatus.APPROVAL, director_statuses)

    def test_stock_flow_creates_balances(self) -> None:
        receipt = create_stock_receipt(
            user=self.warehouse,
            cleaned_data={
                "receipt_date": timezone.localdate(),
                "supplier": self.supplier,
                "supplier_document": None,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="20", unit_price="100", notes="Приход"),
            },
        )
        create_stock_issue(
            user=self.warehouse,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "received_by_name": "Прораб",
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="8", unit_price="100", notes="Отпуск"),
            },
        )
        warehouse_balance = StockMovement.objects.filter(material=self.material, location_name=settings.WAREHOUSE_NAME).aggregate(total=Sum("quantity_delta"))["total"]
        self.assertEqual(receipt.lines.count(), 1)
        self.assertEqual(warehouse_balance, Decimal("12"))

    def test_site_material_request_can_drive_stock_issue(self) -> None:
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=self.material,
            quantity_delta=Decimal("20"),
            location_name=settings.WAREHOUSE_NAME,
            source_type="seed",
            source_id=3,
            unit_price=Decimal("100"),
            created_by=self.warehouse,
        )
        site_request = create_site_material_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "contract": self.contract,
                "status": DocumentStatus.DRAFT,
                "notes": "Нужно со склада",
                "items": material_items(quantity="6", unit_price="100", notes="по заявке участка"),
            },
        )
        issue = create_stock_issue(
            user=self.warehouse,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "site_request": site_request,
                "contract": None,
                "stock_receipt": None,
                "received_by_name": "Прораб",
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": "",
            },
        )
        self.assertEqual(issue.site_request, site_request)
        self.assertEqual(issue.lines.first().quantity, Decimal("6.000"))

    def test_stock_issue_rejects_negative_warehouse_balance(self) -> None:
        with self.assertRaisesMessage(ValueError, "Недостаточно остатка"):
            create_stock_issue(
                user=self.warehouse,
                cleaned_data={
                    "issue_date": timezone.localdate(),
                    "site_name": "Участок 12",
                    "contract": self.contract,
                    "received_by_name": "Прораб",
                    "status": DocumentStatus.DRAFT,
                    "notes": "",
                    "items": material_items(quantity="2", unit_price="100", notes="Отпуск"),
                },
            )

    def test_writeoff_creates_lines(self) -> None:
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=self.material,
            quantity_delta=Decimal("20"),
            location_name="Участок 12",
            source_type="seed",
            source_id=1,
            unit_price=Decimal("100"),
            created_by=self.user,
        )
        act = create_writeoff(
            user=self.user,
            cleaned_data={
                "act_date": timezone.localdate(),
                "contract": self.contract,
                "site_name": "Участок 12",
                "work_type": "Прокладка кабеля",
                "work_volume": Decimal("3"),
                "volume_unit": "этап",
                "status": DocumentStatus.DRAFT,
                "notes": "",
            },
        )
        self.assertEqual(act.lines.count(), 1)
        self.assertEqual(act.lines.first().actual_quantity, Decimal("7.500"))

    def test_writeoff_can_take_work_type_and_volume_from_contract(self) -> None:
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=self.material,
            quantity_delta=Decimal("20"),
            location_name="Участок 12",
            source_type="seed",
            source_id=4,
            unit_price=Decimal("100"),
            created_by=self.user,
        )
        self.contract.planned_volume = Decimal("2")
        self.contract.volume_unit = "этап"
        self.contract.save(update_fields=["planned_volume", "volume_unit", "updated_at"])
        act = create_writeoff(
            user=self.user,
            cleaned_data={
                "act_date": timezone.localdate(),
                "contract": self.contract,
                "site_name": "Участок 12",
                "work_type": "",
                "work_volume": None,
                "volume_unit": "",
                "status": DocumentStatus.DRAFT,
                "notes": "",
            },
        )
        self.assertEqual(act.work_type, self.contract.work_type)
        self.assertEqual(act.work_volume, Decimal("2"))

    def test_writeoff_form_uses_dropdowns_for_site_and_work_type(self) -> None:
        form = WriteOffCreateForm(user=self.user, initial={"site_name": self.user.site_name})

        self.assertEqual(form.fields["site_name"].widget.input_type, "select")
        self.assertEqual(form.fields["work_type"].widget.input_type, "select")
        self.assertIn((self.user.site_name, self.user.site_name), list(form.fields["site_name"].choices))
        self.assertIn(("Прокладка кабеля", "Прокладка кабеля"), list(form.fields["work_type"].choices))
        self.assertIn(("", "По договору СМР"), list(form.fields["work_type"].choices))

    def test_writeoff_form_rejects_values_outside_dropdowns(self) -> None:
        form = WriteOffCreateForm(
            data={
                "act_date": timezone.localdate().isoformat(),
                "contract": str(self.contract.id),
                "template_variant": WriteOffTemplateVariant.CONTRACT,
                "site_name": "Чужой участок",
                "work_type": "Несуществующий вид работ",
                "work_volume": "1",
                "volume_unit": "этап",
                "status": DocumentStatus.DRAFT,
                "notes": "",
            },
            user=self.user,
            initial={"site_name": self.user.site_name},
        )

        self.assertFalse(form.is_valid())
        self.assertIn("site_name", form.errors)
        self.assertIn("work_type", form.errors)

    def test_writeoff_rejects_negative_site_balance(self) -> None:
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=self.material,
            quantity_delta=Decimal("2"),
            location_name="Участок 12",
            source_type="seed",
            source_id=2,
            unit_price=Decimal("100"),
            created_by=self.user,
        )
        with self.assertRaisesMessage(ValueError, "Недостаточно остатка"):
            create_writeoff(
                user=self.user,
                cleaned_data={
                    "act_date": timezone.localdate(),
                    "contract": self.contract,
                    "site_name": "Участок 12",
                    "work_type": self.contract.work_type,
                    "work_volume": Decimal("3"),
                    "volume_unit": "этап",
                    "status": DocumentStatus.DRAFT,
                    "notes": "",
                },
            )

    def test_transition_follows_role_workflow_and_auto_routes_accounting(self) -> None:
        request = create_procurement_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.APPROVAL,
                "notes": "На согласовании",
                "items": material_items(quantity="10", unit_price="100", notes="Для монтажа"),
            },
        )
        record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=request.id)

        with self.assertRaises(ValueError):
            transition_document(user=self.user, record=record, new_status=DocumentStatus.APPROVED)

        transition_document(user=self.director, record=record, new_status=DocumentStatus.APPROVED)
        record.refresh_from_db()
        self.assertEqual(record.status, DocumentStatus.APPROVED)

        transition_document(user=self.accounting, record=record, new_status=DocumentStatus.ACCEPTED)
        record.refresh_from_db()
        request.refresh_from_db()
        self.assertEqual(record.status, DocumentStatus.ACCEPTED)
        self.assertEqual(request.status, DocumentStatus.ACCEPTED)
        self.assertEqual(AuditLog.objects.filter(entity_type="procurement_request", action="status_change").count(), 3)

    def test_work_acceptance_closes_smr_contract_after_accounting_acceptance(self) -> None:
        act = create_work_acceptance(
            user=self.user,
            cleaned_data={
                "act_date": timezone.localdate(),
                "contract": self.contract,
                "site_name": self.user.site_name,
                "work_description": "Работы выполнены",
                "accepted_volume": Decimal("1"),
                "volume_unit": "этап",
                "amount": Decimal("1000"),
                "status": DocumentStatus.APPROVAL,
                "notes": "",
            },
        )
        record = DocumentRecord.objects.get(entity_type="work_acceptance", entity_id=act.id)
        transition_document(user=self.director, record=record, new_status=DocumentStatus.APPROVED)
        record.refresh_from_db()
        transition_document(user=self.accounting, record=record, new_status=DocumentStatus.ACCEPTED)
        self.contract.refresh_from_db()
        self.assertEqual(self.contract.status, DocumentStatus.ACCEPTED)

    def test_primary_document_is_generated_from_request_and_synced_to_archive(self) -> None:
        request = create_procurement_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "Основание для счета",
                "items": material_items(quantity="10", unit_price="125", notes="Первичный документ"),
            },
        )
        document = create_primary_document(
            user=self.director,
            cleaned_data={
                "document_type": self.invoice_type,
                "doc_date": timezone.localdate(),
                "supplier": self.supplier,
                "request": request,
                "supply_contract": None,
                "stock_receipt": None,
                "status": DocumentStatus.DRAFT,
                "amount": Decimal("0"),
                "vat_amount": Decimal("250"),
                "notes": "Сгенерирован по заявке",
                "items": "",
            },
        )
        record = DocumentRecord.objects.get(entity_type="primary_document", entity_id=document.id)
        self.assertEqual(document.lines.count(), 1)
        self.assertEqual(document.amount, Decimal("1250"))
        self.assertEqual(record.doc_number, document.number)
        self.assertEqual(record.counterparty, self.supplier.name)

    def test_stock_receipt_can_use_primary_document_lines(self) -> None:
        request = create_procurement_request(
            user=self.user,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="4", unit_price="125", notes="накладная"),
            },
        )
        goods_waybill, _created = DocumentType.objects.get_or_create(
            code="goods_waybill",
            defaults={
                "name": "Товарная накладная",
                "prefix": "TN",
                "available_for_generation": True,
                "requires_items": True,
            },
        )
        primary = create_primary_document(
            user=self.director,
            cleaned_data={
                "document_type": goods_waybill,
                "doc_date": timezone.localdate(),
                "supplier": self.supplier,
                "request": request,
                "supply_contract": None,
                "stock_receipt": None,
                "status": DocumentStatus.DRAFT,
                "amount": Decimal("0"),
                "vat_amount": Decimal("0"),
                "notes": "",
                "items": "",
            },
        )
        receipt = create_stock_receipt(
            user=self.warehouse,
            cleaned_data={
                "receipt_date": timezone.localdate(),
                "supplier": None,
                "supplier_document": None,
                "primary_document": primary,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": "",
            },
        )
        self.assertEqual(receipt.primary_document, primary)
        self.assertEqual(receipt.lines.first().quantity, Decimal("4.000"))

    def test_ppe_issuance_accepts_worker_and_material_names_with_sizes(self) -> None:
        worker = Worker.objects.create(full_name="Иван Иванов", employee_number="EMP-777", site_name=self.user.site_name)
        ppe = Material.objects.create(code="PPE-777", name="Костюм летний", unit="шт", price=500, min_stock=0, is_ppe=True)
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=ppe,
            quantity_delta=Decimal("3"),
            location_name=settings.WAREHOUSE_NAME,
            source_type="seed",
            source_id=5,
            unit_price=Decimal("500"),
            created_by=self.warehouse,
        )
        issuance = create_ppe_issuance(
            user=self.user,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "season": "летняя",
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": '[{"worker_name":"Иван Иванов","material_name":"Костюм летний","quantity":"1","service_life_months":"12","clothing_size":"52","shoe_size":"42"}]',
            },
        )
        line = issuance.lines.get()
        self.assertEqual(line.worker, worker)
        self.assertEqual(line.material, ppe)
        self.assertEqual(line.clothing_size, "52")
        self.assertEqual(line.shoe_size, "42")
        self.assertFalse(StockMovement.objects.filter(source_type="ppe_issuance", source_id=issuance.id).exists())

    def test_ppe_issuance_is_written_off_only_after_warehouse_confirmation(self) -> None:
        worker = Worker.objects.create(full_name="Петр Сидоров", employee_number="EMP-778", site_name=self.user.site_name)
        ppe = Material.objects.create(code="PPE-778", name="Каска", unit="шт", price=300, min_stock=0, is_ppe=True)
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=ppe,
            quantity_delta=Decimal("3"),
            location_name=settings.WAREHOUSE_NAME,
            source_type="seed",
            source_id=6,
            unit_price=Decimal("300"),
            created_by=self.warehouse,
        )
        issuance = create_ppe_issuance(
            user=self.user,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "season": "летняя",
                "status": DocumentStatus.APPROVAL,
                "notes": "",
                "items": '[{"employee_number":"EMP-778","material_code":"PPE-778","quantity":"2","service_life_months":"12"}]',
            },
        )
        record = DocumentRecord.objects.get(entity_type="ppe_issuance", entity_id=issuance.id)
        warehouse_statuses = dict(workflow_allowed_statuses(self.warehouse, record))

        self.assertIn(DocumentStatus.SUPPLY_CONFIRMED, warehouse_statuses)
        self.assertEqual(warehouse_statuses[DocumentStatus.SUPPLY_CONFIRMED], "Выдача подтверждена")
        self.assertFalse(StockMovement.objects.filter(source_type="ppe_issuance", source_id=issuance.id).exists())

        transition_document(user=self.warehouse, record=record, new_status=DocumentStatus.SUPPLY_CONFIRMED)

        issuance.refresh_from_db()
        movement = StockMovement.objects.get(source_type="ppe_issuance", source_id=issuance.id)
        self.assertEqual(issuance.status, DocumentStatus.SUPPLY_CONFIRMED)
        self.assertEqual(issuance.confirmed_by, self.warehouse)
        self.assertIsNotNone(issuance.confirmed_at)
        self.assertEqual(movement.material, ppe)
        self.assertEqual(movement.quantity_delta, Decimal("-2.000"))
        self.assertEqual(movement.created_by, self.warehouse)

    def test_ppe_confirmation_rejects_insufficient_warehouse_stock(self) -> None:
        Worker.objects.create(full_name="Сергей Иванов", employee_number="EMP-779", site_name=self.user.site_name)
        Material.objects.create(code="PPE-779", name="Очки защитные", unit="шт", price=120, min_stock=0, is_ppe=True)
        issuance = create_ppe_issuance(
            user=self.user,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "season": "летняя",
                "status": DocumentStatus.APPROVAL,
                "notes": "",
                "items": '[{"employee_number":"EMP-779","material_code":"PPE-779","quantity":"1","service_life_months":"12"}]',
            },
        )
        record = DocumentRecord.objects.get(entity_type="ppe_issuance", entity_id=issuance.id)

        with self.assertRaisesMessage(ValueError, "Недостаточно остатка"):
            transition_document(user=self.warehouse, record=record, new_status=DocumentStatus.SUPPLY_CONFIRMED)

        issuance.refresh_from_db()
        record.refresh_from_db()
        self.assertEqual(issuance.status, DocumentStatus.APPROVAL)
        self.assertEqual(record.status, DocumentStatus.APPROVAL)
        self.assertIsNone(issuance.confirmed_by)
        self.assertFalse(StockMovement.objects.filter(source_type="ppe_issuance", source_id=issuance.id).exists())

    def test_ppe_rework_clears_warehouse_confirmation_and_stock_movement(self) -> None:
        Worker.objects.create(full_name="Анна Петрова", employee_number="EMP-780", site_name=self.user.site_name)
        ppe = Material.objects.create(code="PPE-780", name="Перчатки", unit="пар", price=80, min_stock=0, is_ppe=True)
        StockMovement.objects.create(
            movement_date=timezone.localdate(),
            material=ppe,
            quantity_delta=Decimal("5"),
            location_name=settings.WAREHOUSE_NAME,
            source_type="seed",
            source_id=7,
            unit_price=Decimal("80"),
            created_by=self.warehouse,
        )
        issuance = create_ppe_issuance(
            user=self.user,
            cleaned_data={
                "issue_date": timezone.localdate(),
                "site_name": self.user.site_name,
                "season": "летняя",
                "status": DocumentStatus.APPROVAL,
                "notes": "",
                "items": '[{"employee_number":"EMP-780","material_code":"PPE-780","quantity":"1","service_life_months":"6"}]',
            },
        )
        record = DocumentRecord.objects.get(entity_type="ppe_issuance", entity_id=issuance.id)
        confirmed_record = transition_document(user=self.warehouse, record=record, new_status=DocumentStatus.SUPPLY_CONFIRMED)

        transition_document(user=self.warehouse, record=confirmed_record, new_status=DocumentStatus.REWORK)

        issuance.refresh_from_db()
        self.assertEqual(issuance.status, DocumentStatus.REWORK)
        self.assertIsNone(issuance.confirmed_by)
        self.assertIsNone(issuance.confirmed_at)
        self.assertFalse(StockMovement.objects.filter(source_type="ppe_issuance", source_id=issuance.id).exists())

    def test_supplier_cannot_upload_document_for_another_supplier(self) -> None:
        supplier_user = User.objects.create_user(username="supplier-guard", password="supplier123", role="supplier", supplier=self.supplier)
        foreign_supplier = Supplier.objects.create(name='ООО "Чужой контрагент"')
        with self.assertRaisesMessage(ValueError, "Пользователь-поставщик может работать только со своей организацией"):
            create_supplier_document(
                user=supplier_user,
                cleaned_data={
                    "supplier": foreign_supplier,
                    "request": None,
                    "supply_contract": None,
                    "doc_type": "Счет",
                    "doc_number": "SUP-777",
                    "doc_date": timezone.localdate(),
                    "amount": Decimal("1000"),
                    "vat_amount": Decimal("200"),
                    "attachment": None,
                    "notes": "",
                },
            )

    def test_operation_draft_is_saved_and_loaded(self) -> None:
        save_operation_draft(
            user=self.user,
            operation_slug="procurement",
            payload={"site_name": "Участок 12", "notes": "Черновик заявки", "items": material_items(quantity="3", unit_price="100", notes="Автосохранение")},
        )
        payload = load_operation_draft(user=self.user, operation_slug="procurement")
        self.assertEqual(payload["site_name"], "Участок 12")
        self.assertIn("Автосохранение", payload["items"])

    def test_restore_backup_payload_restores_deleted_records(self) -> None:
        Material.objects.create(code="MAT-777", name="Труба", unit="шт", price=250, min_stock=1)
        payload = create_backup_payload()
        Supplier.objects.all().delete()
        Material.objects.all().delete()

        restored = restore_backup_payload(payload=payload)

        self.assertGreater(restored["suppliers"], 0)
        self.assertTrue(Supplier.objects.filter(name='ООО "Тест-Снаб"').exists())
        self.assertTrue(Material.objects.filter(code="MAT-777").exists())


class ViewSmokeTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create_user(username="admin", password="admin123", role="admin", is_staff=True, is_superuser=True)
        self.director = User.objects.create_user(username="director", password="director123", role="director")
        self.accounting = User.objects.create_user(username="accounting", password="accounting123", role="accounting")
        self.supplier = Supplier.objects.create(name='ООО "Поставщик"')
        self.other_supplier = Supplier.objects.create(name='ООО "Чужой поставщик"')
        self.supplier_user = User.objects.create_user(username="supplier", password="supplier123", role="supplier", supplier=self.supplier)
        self.other_supplier_user = User.objects.create_user(username="supplier2", password="supplier123", role="supplier", supplier=self.other_supplier)
        self.other_site_manager = User.objects.create_user(username="site2", password="site123", role="site_manager", site_name="Участок 99")
        self.site_manager = User.objects.create_user(username="site", password="site123", role="site_manager", site_name="Участок 12")
        self.invoice_type, _created = DocumentType.objects.get_or_create(
            code="invoice",
            defaults={
                "name": "Счет",
                "prefix": "INV",
                "available_for_generation": True,
                "available_for_upload": True,
                "requires_items": True,
            },
        )
        self.material = Material.objects.create(code="MAT-001", name="Кабель", unit="м", price=100, min_stock=5)
        self.contract = SMRContract.objects.create(
            number="SMR-002",
            contract_date=timezone.localdate(),
            customer_name="Заказчик",
            subject="Монтаж",
            work_type="Прокладка",
            amount=Decimal("50000"),
            created_by=self.site_manager,
        )

    def test_login_and_dashboard(self) -> None:
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 302)
        self.client.login(username="admin", password="admin123")
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)

    def test_writeoff_operation_renders_dropdown_fields(self) -> None:
        self.client.login(username="site", password="site123")

        response = self.client.get(reverse("operation-page", kwargs={"slug": "writeoffs"}))

        self.assertEqual(response.status_code, 200)
        html = response.content.decode("utf-8")
        self.assertIn('<select name="site_name"', html)
        self.assertIn('<select name="work_type"', html)

    def test_notification_can_be_marked_as_read(self) -> None:
        notification = Notification.objects.create(
            user=self.user,
            kind=NotificationType.ACTION_REQUIRED,
            title="Проверить документ",
            message="Тестовое уведомление",
        )
        self.client.login(username="admin", password="admin123")

        response = self.client.post(reverse("notification-read", kwargs={"notification_id": notification.id}), {"next": reverse("dashboard")})

        notification.refresh_from_db()
        self.assertEqual(response.status_code, 302)
        self.assertTrue(notification.is_read)
        self.assertIsNotNone(notification.read_at)

    def test_notification_feed_returns_current_unread_notifications(self) -> None:
        visible_notification = Notification.objects.create(
            user=self.user,
            kind=NotificationType.ACTION_REQUIRED,
            title="Проверить документ",
            message="Статус изменен",
        )
        Notification.objects.create(
            user=self.user,
            kind=NotificationType.STATUS_CHANGED,
            title="Старое уведомление",
            is_read=True,
        )
        Notification.objects.create(
            user=self.director,
            kind=NotificationType.ACTION_REQUIRED,
            title="Чужое уведомление",
        )
        self.client.login(username="admin", password="admin123")

        response = self.client.get(reverse("notifications-feed"))

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["count"], 1)
        self.assertEqual(len(payload["items"]), 1)
        item = payload["items"][0]
        self.assertEqual(item["id"], visible_notification.id)
        self.assertEqual(item["title"], "Проверить документ")
        self.assertEqual(item["message"], "Статус изменен")
        self.assertEqual(
            item["read_url"],
            reverse("notification-read", kwargs={"notification_id": visible_notification.id}),
        )
        self.assertEqual(item["documents_url"], reverse("documents"))

    def test_operation_creation_shows_immediate_file_download(self) -> None:
        self.client.login(username="site", password="site123")

        response = self.client.post(
            reverse("operation-page", kwargs={"slug": "site-requests"}),
            {
                "request_date": timezone.localdate().isoformat(),
                "site_name": self.site_manager.site_name,
                "contract": self.contract.id,
                "status": DocumentStatus.DRAFT,
                "notes": "Сразу скачать файл",
                "items": material_items(quantity="2", unit_price="100", notes="export"),
            },
            follow=True,
        )

        request = SiteMaterialRequest.objects.get(notes="Сразу скачать файл")
        export_url = reverse("export-document", kwargs={"entity_type": "site_material_request", "entity_id": request.id})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["created_export_url"], export_url)
        self.assertContains(response, "Файл готов")
        self.assertContains(response, export_url)

    def test_rework_document_can_be_edited_and_resubmitted(self) -> None:
        site_request = create_site_material_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.site_manager.site_name,
                "contract": self.contract,
                "status": DocumentStatus.APPROVAL,
                "notes": "Нужна правка",
                "items": material_items(quantity="2", unit_price="100", notes="old"),
            },
        )
        record = DocumentRecord.objects.get(entity_type="site_material_request", entity_id=site_request.id)
        transition_document(user=self.user, record=record, new_status=DocumentStatus.REWORK)

        self.client.login(username="site", password="site123")
        documents_response = self.client.get(reverse("documents"))
        returned_record = next(item for item in documents_response.context["records"] if item.entity_id == site_request.id)
        self.assertIn("rework=", returned_record.rework_edit_url)
        self.assertContains(documents_response, "Доработать")

        post_response = self.client.post(
            returned_record.rework_edit_url,
            {
                "request_date": timezone.localdate().isoformat(),
                "site_name": self.site_manager.site_name,
                "contract": self.contract.id,
                "status": DocumentStatus.APPROVAL,
                "notes": "Исправлено",
                "items": material_items(quantity="4", unit_price="100", notes="new"),
            },
        )

        site_request.refresh_from_db()
        record.refresh_from_db()
        self.assertEqual(post_response.status_code, 302)
        self.assertEqual(site_request.status, DocumentStatus.APPROVAL)
        self.assertEqual(record.status, DocumentStatus.APPROVAL)
        self.assertEqual(site_request.notes, "Исправлено")
        self.assertEqual(site_request.lines.get().quantity, Decimal("4.000"))

    def test_documents_and_archive_are_separate_sections(self) -> None:
        active_record = DocumentRecord.objects.create(
            entity_type="manual",
            entity_id=1,
            doc_type="Тестовый документ",
            doc_number="ACTIVE-001",
            doc_date=timezone.localdate(),
            status=DocumentStatus.DRAFT,
            title="Активный документ",
        )
        archived_record = DocumentRecord.objects.create(
            entity_type="manual",
            entity_id=2,
            doc_type="Тестовый документ",
            doc_number="ARCH-001",
            doc_date=timezone.localdate(),
            status=DocumentStatus.ACCEPTED,
            title="Закрытый документ",
        )
        future_closed_record = DocumentRecord.objects.create(
            entity_type="manual",
            entity_id=3,
            doc_type="Тестовый документ",
            doc_number="FUTURE-001",
            doc_date=timezone.localdate() + timedelta(days=1),
            status=DocumentStatus.ACCEPTED,
            title="Будущий закрытый документ",
        )

        self.client.login(username="admin", password="admin123")
        documents_response = self.client.get(reverse("documents"))
        archive_response = self.client.get(reverse("archive"))

        self.assertIn(active_record, documents_response.context["records"])
        self.assertNotIn(archived_record, documents_response.context["records"])
        self.assertIn(archived_record, archive_response.context["records"])
        self.assertNotIn(active_record, archive_response.context["records"])
        self.assertNotIn(future_closed_record, archive_response.context["records"])

    def test_login_page_uses_customer_branding(self) -> None:
        response = self.client.get(reverse("login"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "АО «СТ-1»")
        self.assertNotContains(response, "AIS 2026")
        self.assertNotContains(response, "Python + Django + DRF + Celery + PostgreSQL")

    def test_backups_page_is_fully_russian(self) -> None:
        self.client.login(username="admin", password="admin123")
        response = self.client.get(reverse("backups"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Создать резервную копию")
        self.assertContains(response, "JSON-файл резервной копии")
        self.assertNotContains(response, "Backup JSON")

    def test_worklog_form_uses_russian_default_status(self) -> None:
        self.client.login(username="site", password="site123")
        response = self.client.get(reverse("operation-page", kwargs={"slug": "worklogs"}))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Запланировано")
        self.assertNotContains(response, 'value="planned"')

    def test_reports_for_supplier_forbidden(self) -> None:
        self.client.login(username="supplier", password="supplier123")
        response = self.client.get(reverse("reports"))
        self.assertEqual(response.status_code, 403)

    def test_supplier_can_view_only_own_supply_contracts_in_catalog(self) -> None:
        own_contract = SupplyContract.objects.create(
            number="SUP-CON-001",
            contract_date=timezone.localdate(),
            supplier=self.supplier,
            related_smr_contract=self.contract,
            amount=Decimal("10000"),
            status=DocumentStatus.DRAFT,
        )
        foreign_contract = SupplyContract.objects.create(
            number="SUP-CON-999",
            contract_date=timezone.localdate(),
            supplier=self.other_supplier,
            related_smr_contract=self.contract,
            amount=Decimal("20000"),
            status=DocumentStatus.DRAFT,
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get(reverse("catalog-page", kwargs={"slug": "supply-contracts"}))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, own_contract.number)
        self.assertNotContains(response, foreign_contract.number)
        self.assertFalse(response.context["can_create"])

        post_response = self.client.post(
            reverse("catalog-page", kwargs={"slug": "supply-contracts"}),
            {
                "number": "SUP-CON-NEW",
                "contract_date": timezone.localdate().isoformat(),
                "supplier": self.supplier.id,
                "related_smr_contract": self.contract.id,
                "amount": "1",
                "status": DocumentStatus.DRAFT,
                "terms": "",
            },
        )
        self.assertEqual(post_response.status_code, 403)

    def test_site_manager_contract_catalog_is_scoped_and_read_only(self) -> None:
        foreign_contract = SMRContract.objects.create(
            number="SMR-FOREIGN-001",
            contract_date=timezone.localdate(),
            customer_name="Другой заказчик",
            subject="Чужой контракт",
            work_type="Монтаж",
            amount=Decimal("25000"),
            created_by=self.other_site_manager,
        )

        self.client.login(username="site", password="site123")
        response = self.client.get(reverse("catalog-page", kwargs={"slug": "contracts"}))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.contract.number)
        self.assertNotContains(response, foreign_contract.number)
        self.assertFalse(response.context["can_create"])
        self.assertFalse(response.context["catalog_has_manage_actions"])

    def test_site_manager_cannot_access_supply_contract_catalog(self) -> None:
        self.client.login(username="site", password="site123")
        response = self.client.get(reverse("catalog-page", kwargs={"slug": "supply-contracts"}))
        self.assertEqual(response.status_code, 403)

    def test_supplier_procurement_page_is_scoped_and_read_only(self) -> None:
        own_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.site_manager.site_name,
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="3", unit_price="100", notes="own"),
            },
        )
        foreign_request = create_procurement_request(
            user=self.other_site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.other_site_manager.site_name,
                "contract": self.contract,
                "supplier": self.other_supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="4", unit_price="100", notes="foreign"),
            },
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get(reverse("operation-page", kwargs={"slug": "procurement"}))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, own_request.number)
        self.assertNotContains(response, foreign_request.number)
        self.assertFalse(response.context["can_create"])

        post_response = self.client.post(
            reverse("operation-page", kwargs={"slug": "procurement"}),
            {
                "request_date": timezone.localdate().isoformat(),
                "site_name": self.site_manager.site_name,
                "supplier": self.supplier.id,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="1", unit_price="100", notes="new"),
            },
        )
        self.assertEqual(post_response.status_code, 403)

    def test_supplier_documents_section_shows_only_own_docs_and_allows_supply_confirmation(self) -> None:
        own_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.site_manager.site_name,
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="5", unit_price="100", notes="own"),
            },
        )
        own_doc = create_supplier_document(
            user=self.supplier_user,
            cleaned_data={
                "supplier": self.supplier,
                "request": own_request,
                "supply_contract": None,
                "doc_type": "Счет",
                "doc_number": "SUP-OWN-001",
                "doc_date": timezone.localdate(),
                "amount": Decimal("500"),
                "vat_amount": Decimal("100"),
                "attachment": None,
                "notes": "",
            },
        )
        create_supplier_document(
            user=self.other_supplier_user,
            cleaned_data={
                "supplier": self.other_supplier,
                "request": None,
                "supply_contract": None,
                "doc_type": "Счет",
                "doc_number": "SUP-FOREIGN-001",
                "doc_date": timezone.localdate(),
                "amount": Decimal("700"),
                "vat_amount": Decimal("140"),
                "attachment": None,
                "notes": "",
            },
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get(reverse("documents"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, own_doc.doc_number)
        self.assertNotContains(response, "SUP-FOREIGN-001")

        record = next(item for item in response.context["records"] if item.entity_type == "supplier_document" and item.entity_id == own_doc.id)
        available_statuses = [value for value, _label in record.available_status_choices]
        self.assertIn(DocumentStatus.SUPPLY_CONFIRMED, available_statuses)
        self.assertNotIn(DocumentStatus.APPROVAL, available_statuses)

        post_response = self.client.post(
            reverse("documents"),
            {"record_id": record.id, "new_status": DocumentStatus.SUPPLY_CONFIRMED},
        )
        self.assertEqual(post_response.status_code, 302)
        own_doc.refresh_from_db()
        self.assertEqual(own_doc.status, DocumentStatus.SUPPLY_CONFIRMED)

    def test_supplier_api_supply_contracts_are_scoped_to_own_supplier(self) -> None:
        own_contract = SupplyContract.objects.create(
            number="SUP-API-001",
            contract_date=timezone.localdate(),
            supplier=self.supplier,
            related_smr_contract=self.contract,
            amount=Decimal("11000"),
            status=DocumentStatus.DRAFT,
        )
        SupplyContract.objects.create(
            number="SUP-API-999",
            contract_date=timezone.localdate(),
            supplier=self.other_supplier,
            related_smr_contract=self.contract,
            amount=Decimal("22000"),
            status=DocumentStatus.DRAFT,
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get("/api/supply-contracts/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["number"], own_contract.number)

    def test_supplier_api_is_scoped_to_own_documents(self) -> None:
        request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="5", unit_price="100", notes="Тест"),
            },
        )
        create_supplier_document(
            user=self.supplier_user,
            cleaned_data={
                "supplier": self.supplier,
                "request": request,
                "supply_contract": None,
                "doc_type": "Счет",
                "doc_number": "SUP-001",
                "doc_date": timezone.localdate(),
                "amount": Decimal("500"),
                "vat_amount": Decimal("100"),
                "attachment": None,
                "notes": "",
            },
        )
        create_supplier_document(
            user=self.other_supplier_user,
            cleaned_data={
                "supplier": self.other_supplier,
                "request": None,
                "supply_contract": None,
                "doc_type": "Счет",
                "doc_number": "SUP-002",
                "doc_date": timezone.localdate(),
                "amount": Decimal("700"),
                "vat_amount": Decimal("140"),
                "attachment": None,
                "notes": "",
            },
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get("/api/supplier-documents/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["doc_number"], "SUP-001")

    def test_supplier_primary_document_api_is_scoped_to_own_supplier(self) -> None:
        request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="5", unit_price="100", notes="Тест"),
            },
        )
        create_primary_document(
            user=self.user,
            cleaned_data={
                "document_type": self.invoice_type,
                "doc_date": timezone.localdate(),
                "supplier": self.supplier,
                "request": request,
                "supply_contract": None,
                "stock_receipt": None,
                "status": DocumentStatus.DRAFT,
                "amount": Decimal("500"),
                "vat_amount": Decimal("100"),
                "notes": "",
                "items": "",
            },
        )
        create_primary_document(
            user=self.user,
            cleaned_data={
                "document_type": self.invoice_type,
                "doc_date": timezone.localdate(),
                "supplier": self.other_supplier,
                "request": None,
                "supply_contract": None,
                "stock_receipt": None,
                "status": DocumentStatus.DRAFT,
                "amount": Decimal("700"),
                "vat_amount": Decimal("140"),
                "notes": "",
                "items": material_items(quantity="7", unit_price="100", notes="Чужой"),
            },
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get("/api/primary-documents/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["supplier_name"], self.supplier.name)

    def test_operation_draft_endpoint_saves_site_request_draft(self) -> None:
        self.client.login(username="site", password="site123")
        response = self.client.post(
            reverse("operation-draft", kwargs={"slug": "site-requests"}),
            {
                "request_date": timezone.localdate().isoformat(),
                "site_name": "Участок 12",
                "status": DocumentStatus.DRAFT,
                "notes": "Автосохранение из UI",
                "items": material_items(quantity="2", unit_price="100", notes="Черновик"),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = load_operation_draft(user=self.site_manager, operation_slug="site-requests")
        self.assertEqual(payload["site_name"], "Участок 12")
        self.assertIn("Черновик", payload["items"])


    def test_accounting_dashboard_navigation_is_read_only(self) -> None:
        self.client.login(username="accounting", password="accounting123")
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["catalog_links"], [])
        self.assertEqual(response.context["operation_links"], [])
        self.assertTrue(response.context["can_access_documents"])
        self.assertTrue(response.context["can_access_archive"])
        self.assertTrue(response.context["can_access_reports"])
        self.assertFalse(response.context["can_access_backups"])
        self.assertEqual(response.context["warehouse_rows"], [])

    def test_site_manager_sees_ppe_in_ui_and_only_own_rows_in_reports(self) -> None:
        create_work_log(
            user=self.site_manager,
            cleaned_data={
                "site_name": "Участок 12",
                "contract": self.contract,
                "work_type": "Монтаж",
                "planned_volume": Decimal("5"),
                "actual_volume": Decimal("3"),
                "volume_unit": "м",
                "plan_date": timezone.localdate(),
                "actual_date": timezone.localdate(),
                "status": "planned",
                "notes": "",
            },
        )
        create_work_log(
            user=self.other_site_manager,
            cleaned_data={
                "site_name": "Участок 99",
                "contract": self.contract,
                "work_type": "Чужой контур",
                "planned_volume": Decimal("7"),
                "actual_volume": Decimal("4"),
                "volume_unit": "м",
                "plan_date": timezone.localdate(),
                "actual_date": timezone.localdate(),
                "status": "planned",
                "notes": "",
            },
        )

        self.client.login(username="site", password="site123")
        dashboard_response = self.client.get(reverse("dashboard"))
        operation_slugs = {item["slug"] for item in dashboard_response.context["operation_links"]}
        self.assertIn("ppe", operation_slugs)

        report_response = self.client.get(reverse("reports"), {"report": "work"})
        self.assertEqual(report_response.status_code, 200)
        self.assertContains(report_response, "Участок 12")
        self.assertNotContains(report_response, "Участок 99")

    def test_site_manager_sees_ppe_in_ui_and_only_own_rows_in_reports(self) -> None:
        create_work_log(
            user=self.site_manager,
            cleaned_data={
                "site_name": self.site_manager.site_name,
                "contract": self.contract,
                "work_type": "Монтаж",
                "planned_volume": Decimal("5"),
                "actual_volume": Decimal("3"),
                "volume_unit": "Рј",
                "plan_date": timezone.localdate(),
                "actual_date": timezone.localdate(),
                "status": "planned",
                "notes": "",
            },
        )
        create_work_log(
            user=self.other_site_manager,
            cleaned_data={
                "site_name": self.other_site_manager.site_name,
                "contract": self.contract,
                "work_type": "Чужой контур",
                "planned_volume": Decimal("7"),
                "actual_volume": Decimal("4"),
                "volume_unit": "Рј",
                "plan_date": timezone.localdate(),
                "actual_date": timezone.localdate(),
                "status": "planned",
                "notes": "",
            },
        )

        self.client.login(username="site", password="site123")
        dashboard_response = self.client.get(reverse("dashboard"))
        operation_slugs = {item["slug"] for item in dashboard_response.context["operation_links"]}
        self.assertIn("ppe", operation_slugs)

        report_response = self.client.get(reverse("reports"), {"report": "work"})
        self.assertEqual(report_response.status_code, 200)
        self.assertContains(report_response, self.site_manager.site_name)
        self.assertNotContains(report_response, self.other_site_manager.site_name)

    def test_accounting_documents_section_accepts_records_and_archive_shows_closed_only(self) -> None:
        draft_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": material_items(quantity="1", unit_price="100", notes="draft"),
            },
        )
        approved_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.APPROVAL,
                "notes": "",
                "items": material_items(quantity="2", unit_price="100", notes="approved"),
            },
        )
        approved_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=approved_request.id)
        transition_document(user=self.director, record=approved_record, new_status=DocumentStatus.APPROVED)

        self.client.login(username="accounting", password="accounting123")
        documents_response = self.client.get(reverse("documents"))
        self.assertEqual(documents_response.status_code, 200)
        records = documents_response.context["records"]
        self.assertEqual([record.entity_id for record in records], [approved_request.id])
        self.assertTrue(records[0].can_update_status)
        available_statuses = [value for value, _label in records[0].available_status_choices]
        self.assertIn(DocumentStatus.ACCEPTED, available_statuses)
        self.assertIn(DocumentStatus.REWORK, available_statuses)

        post_response = self.client.post(
            reverse("documents"),
            {"record_id": approved_record.id, "new_status": DocumentStatus.ACCEPTED},
        )
        self.assertEqual(post_response.status_code, 302)
        approved_request.refresh_from_db()
        self.assertEqual(approved_request.status, DocumentStatus.ACCEPTED)

        archive_response = self.client.get(reverse("archive"))
        self.assertEqual(archive_response.status_code, 200)
        archive_records = archive_response.context["records"]
        self.assertEqual([record.entity_id for record in archive_records], [approved_request.id])
        self.assertFalse(archive_records[0].can_update_status)

        api_response = self.client.get("/api/documents/")
        self.assertEqual(api_response.status_code, 200)
        payload = api_response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["entity_id"], approved_request.id)
        self.assertEqual(payload[0]["status"], DocumentStatus.ACCEPTED)

        draft_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=draft_request.id)
        self.assertNotEqual(draft_record.id, approved_record.id)

    def test_rework_status_requires_and_stores_reason(self) -> None:
        request = create_site_material_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "status": DocumentStatus.APPROVAL,
                "notes": "Вернуть с причиной",
                "items": material_items(quantity="1", unit_price="100", notes="reason"),
            },
        )
        record = DocumentRecord.objects.get(entity_type="site_material_request", entity_id=request.id)
        self.client.login(username="admin", password="admin123")

        missing_reason_response = self.client.post(
            reverse("documents"),
            {"record_id": record.id, "new_status": DocumentStatus.REWORK},
        )
        request.refresh_from_db()
        self.assertEqual(missing_reason_response.status_code, 302)
        self.assertEqual(request.status, DocumentStatus.APPROVAL)

        reason = "Не приложено коммерческое предложение"
        response = self.client.post(
            reverse("documents"),
            {
                "record_id": record.id,
                "new_status": DocumentStatus.REWORK,
                "rework_reason": reason,
            },
        )

        self.assertEqual(response.status_code, 302)
        request.refresh_from_db()
        self.assertEqual(request.status, DocumentStatus.REWORK)
        record.refresh_from_db()
        self.assertEqual(record.metadata_json["last_rework_reason"], reason)
        self.assertTrue(AuditLog.objects.filter(entity_type="site_material_request", details__icontains=reason).exists())
        self.assertTrue(
            Notification.objects.filter(
                user=self.site_manager,
                entity_type="site_material_request",
                entity_id=request.id,
                message__icontains=reason,
            ).exists()
        )

        self.client.login(username="site", password="site123")
        documents_response = self.client.get(reverse("documents"))
        self.assertContains(documents_response, reason)
        returned_record = next(item for item in documents_response.context["records"] if item.entity_id == request.id)
        rework_response = self.client.get(returned_record.rework_edit_url)
        self.assertContains(rework_response, reason)

    def test_director_can_send_documents_to_accounting_in_bulk(self) -> None:
        first_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.APPROVAL,
                "notes": "bulk-1",
                "items": material_items(quantity="1", unit_price="100", notes="bulk-1"),
            },
        )
        second_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.APPROVAL,
                "notes": "bulk-2",
                "items": material_items(quantity="1", unit_price="100", notes="bulk-2"),
            },
        )
        draft_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "bulk-draft",
                "items": material_items(quantity="1", unit_price="100", notes="bulk-draft"),
            },
        )
        first_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=first_request.id)
        second_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=second_request.id)
        draft_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=draft_request.id)
        first_record = transition_document(user=self.director, record=first_record, new_status=DocumentStatus.APPROVED)
        second_record = transition_document(user=self.director, record=second_record, new_status=DocumentStatus.APPROVED)

        self.client.login(username="director", password="director123")
        documents_response = self.client.get(reverse("documents"))
        self.assertContains(documents_response, "Пакетная отправка в бухгалтерию")
        self.assertContains(documents_response, "data-bulk-accounting-item")

        post_response = self.client.post(
            reverse("documents"),
            {
                "action": "bulk_send_accounting",
                "record_ids": [str(first_record.id), str(second_record.id), str(draft_record.id)],
            },
        )

        self.assertEqual(post_response.status_code, 302)
        first_request.refresh_from_db()
        second_request.refresh_from_db()
        draft_request.refresh_from_db()
        self.assertEqual(first_request.status, DocumentStatus.SENT_ACCOUNTING)
        self.assertEqual(second_request.status, DocumentStatus.SENT_ACCOUNTING)
        self.assertEqual(draft_request.status, DocumentStatus.DRAFT)

    def test_admin_can_manage_users_from_catalog(self) -> None:
        self.client.login(username="admin", password="admin123")

        create_response = self.client.post(
            reverse("catalog-page", kwargs={"slug": "users"}),
            {
                "action": "save",
                "username": "managed-user",
                "first_name": "Иван",
                "last_name": "Петров",
                "email": "managed@example.com",
                "role": "warehouse",
                "site_name": "",
                "supplier": "",
                "is_active": "on",
                "password1": "ManagedPass123!",
                "password2": "ManagedPass123!",
            },
        )
        self.assertEqual(create_response.status_code, 302)

        managed_user = User.objects.get(username="managed-user")
        self.assertTrue(managed_user.check_password("ManagedPass123!"))
        self.assertTrue(managed_user.is_active)

        update_response = self.client.post(
            reverse("catalog-page", kwargs={"slug": "users"}),
            {
                "action": "save",
                "object_id": managed_user.pk,
                "username": "managed-user",
                "first_name": "Иван",
                "last_name": "Сидоров",
                "email": "managed@example.com",
                "role": "warehouse",
                "site_name": "",
                "supplier": "",
                "password1": "",
                "password2": "",
            },
        )
        self.assertEqual(update_response.status_code, 302)

        managed_user.refresh_from_db()
        self.assertEqual(managed_user.last_name, "Сидоров")
        self.assertFalse(managed_user.is_active)

        delete_response = self.client.post(
            reverse("catalog-page", kwargs={"slug": "users"}),
            {"action": "delete", "object_id": managed_user.pk},
        )
        self.assertEqual(delete_response.status_code, 302)
        self.assertFalse(User.objects.filter(pk=managed_user.pk).exists())

    def test_admin_can_view_audit_log_page(self) -> None:
        AuditLog.objects.create(
            user=self.user,
            action="status_change",
            entity_type="procurement_request",
            entity_id=101,
            details="draft -> approved",
            ip_address="127.0.0.1",
        )

        self.client.login(username="admin", password="admin123")
        response = self.client.get(reverse("audit-log"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "status_change")
        self.assertContains(response, "procurement_request")
        self.assertContains(response, "127.0.0.1")

    def test_accounting_and_director_are_blocked_from_extra_operation_api(self) -> None:
        self.client.login(username="accounting", password="accounting123")
        accounting_response = self.client.get("/api/worklogs/")
        self.assertEqual(accounting_response.status_code, 403)

        self.client.login(username="director", password="director123")
        dashboard_response = self.client.get(reverse("dashboard"))
        self.assertEqual(dashboard_response.status_code, 200)
        self.assertEqual(dashboard_response.context["operation_links"], [])

        director_response = self.client.get("/api/worklogs/")
        self.assertEqual(director_response.status_code, 403)


class PPELifecycleReportingTests(TestCase):
    def setUp(self) -> None:
        self.user = User.objects.create_user(username="ppe_admin", password="ppe_admin123", role="admin")
        self.worker = Worker.objects.create(full_name="Иван Петров", employee_number="EMP-001", site_name="Участок 1")
        self.material = Material.objects.create(
            code="PPE-001",
            name="Куртка сигнальная",
            unit="шт",
            price=Decimal("100"),
            min_stock=Decimal("0"),
            is_ppe=True,
        )

    def _create_line(self, *, number: str, issue_start_date, service_life_months: int) -> PPEIssuanceLine:
        issuance = PPEIssuance.objects.create(
            number=number,
            issue_date=issue_start_date,
            site_name="Участок 1",
            season="",
            issued_by=self.user,
            confirmed_by=self.user,
            confirmed_at=timezone.now(),
            status=DocumentStatus.SUPPLY_CONFIRMED,
            notes="",
        )
        return PPEIssuanceLine.objects.create(
            issuance=issuance,
            worker=self.worker,
            material=self.material,
            quantity=Decimal("1"),
            service_life_months=service_life_months,
            issue_start_date=issue_start_date,
            notes="",
        )

    def test_report_ppe_shows_all_issued_items_for_period(self) -> None:
        current_day = timezone.localdate()
        expired_line = self._create_line(number="PPE-EXP", issue_start_date=current_day - timedelta(days=70), service_life_months=1)
        expiring_line = self._create_line(number="PPE-SOON", issue_start_date=current_day - timedelta(days=15), service_life_months=1)
        ok_line = self._create_line(number="PPE-OK", issue_start_date=current_day - timedelta(days=2), service_life_months=6)

        rows = report_ppe_scoped(
            {
                "date_from": current_day - timedelta(days=90),
                "date_to": current_day,
            },
            user=self.user,
        )
        data_rows = [row for row in rows if row.get("Период") != "ИТОГО"]
        issuance_numbers = {row["Ведомость №"] for row in data_rows}

        self.assertIn(expired_line.issuance.number, issuance_numbers)
        self.assertIn(expiring_line.issuance.number, issuance_numbers)
        self.assertIn(ok_line.issuance.number, issuance_numbers)

        expired_row = next(row for row in data_rows if row["Ведомость №"] == expired_line.issuance.number)
        self.assertEqual(expired_row["Требуется замена"], "Да")
        self.assertIn("Просрочено", expired_row["Предупреждение"])

        ok_row = next(row for row in data_rows if row["Ведомость №"] == ok_line.issuance.number)
        self.assertEqual(ok_row["Требуется замена"], "Нет")
        self.assertEqual(ok_row["Статус срока"], "В норме")


class BootstrapProductCommandTests(TestCase):
    def test_bootstrap_product_creates_admin(self) -> None:
        buffer = StringIO()
        call_command(
            "bootstrap_product",
            username="owner",
            password="StrongPassword123!",
            email="owner@example.com",
            first_name="РРІР°РЅ",
            last_name="РРІР°РЅРѕРІ",
            site_name="Главный офис",
            stdout=buffer,
        )
        user = User.objects.get(username="owner")
        self.assertTrue(user.is_superuser)
        self.assertTrue(user.is_staff)
        self.assertEqual(user.role, "admin")
        self.assertTrue(user.check_password("StrongPassword123!"))


class BootstrapRoleAccountsCommandTests(TestCase):
    def test_bootstrap_role_accounts_creates_all_roles(self) -> None:
        buffer = StringIO()
        call_command("bootstrap_role_accounts", stdout=buffer)
        users = {user.username: user for user in User.objects.all()}
        self.assertEqual(len(users), 7)
        self.assertIn("admin", users)


class SeedDemoDataCommandTests(TestCase):
    def test_seed_demo_data_creates_connected_demo_set(self) -> None:
        buffer = StringIO()

        call_command(
            "seed_demo_data",
            top_records=12,
            prefix="TST",
            password="SeedDemo123!",
            stdout=buffer,
        )

        self.assertIn("Демонстрационный набор TST успешно создан.", buffer.getvalue())

        demo_admin = User.objects.get(username="tst_admin")
        self.assertEqual(demo_admin.role, "admin")
        self.assertTrue(demo_admin.check_password("SeedDemo123!"))

        total_top_records = (
            SMRContract.objects.filter(number__startswith="TST-SMR-").count()
            + SupplyContract.objects.filter(number__startswith="TST-SUP-").count()
            + ProcurementRequest.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + SupplierDocument.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + PrimaryDocument.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + StockReceipt.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + StockIssue.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + WorkLog.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + WriteOffAct.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
            + PPEIssuance.objects.filter(notes__icontains="[DEMO-SEED TST]").count()
        )
        self.assertEqual(total_top_records, 12)

        self.assertTrue(
            DocumentRecord.objects.filter(
                Q(search_text__icontains="[DEMO-SEED TST]")
                | Q(doc_number__startswith="TST-SMR-")
                | Q(doc_number__startswith="TST-SUP-")
            ).exists()
        )
        self.assertTrue(AuditLog.objects.filter(user__username__startswith="tst_").exists())


class LocalizationSmokeTests(TestCase):
    def test_user_facing_labels_are_localized(self) -> None:
        self.assertEqual(settings.WAREHOUSE_NAME, "Центральный склад")

        material_form = MaterialForm()
        self.assertEqual(material_form.fields["code"].label, "Код")
        self.assertEqual(material_form.fields["is_ppe"].label, "СИЗ / спецодежда")

        user_form = UserForm()
        self.assertEqual(user_form.fields["role"].label, "Роль")
        self.assertEqual(user_form.fields["site_name"].label, "Участок / подразделение")
        self.assertEqual(user_form.fields["supplier"].empty_label, "Не выбрано")

        procurement_form = ProcurementRequestCreateForm()
        self.assertEqual(procurement_form.fields["request_date"].label, "Дата заявки")
        self.assertEqual(procurement_form.fields["contract"].empty_label, "Не выбрано")

        supplier_form = SupplierForm()
        self.assertEqual(supplier_form.fields["email"].label, "Эл. почта")
        self.assertEqual(supplier_form.fields["address"].label, "Адрес")

    def test_supplier_requisites_use_russian_labels(self) -> None:
        supplier = Supplier(
            name='ООО "Тест-Снаб"',
            tax_id="7700000000",
            phone="+7 000 000 00 00",
            email="supplier@example.com",
            address="г. Москва",
        )

        requisites = supplier.requisites_text()

        self.assertIn("ИНН 7700000000", requisites)
        self.assertIn("Тел.: +7 000 000 00 00", requisites)
        self.assertIn("Эл. почта: supplier@example.com", requisites)

