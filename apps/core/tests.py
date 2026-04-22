from __future__ import annotations

from datetime import timedelta
from decimal import Decimal
from io import StringIO

from django.conf import settings
from django.core.management import call_command
from django.db.models import Q, Sum
from django.test import TestCase
from django.urls import reverse
from django.utils import timezone

from .forms import MaterialForm, ProcurementRequestCreateForm, SupplierForm, UserForm
from .models import (
    AuditLog,
    DocumentRecord,
    DocumentStatus,
    DocumentType,
    Material,
    MaterialNorm,
    PPEIssuance,
    PPEIssuanceLine,
    PrimaryDocument,
    ProcurementRequest,
    SMRContract,
    StockIssue,
    StockMovement,
    StockReceipt,
    Supplier,
    SupplierDocument,
    SupplyContract,
    User,
    Worker,
    WorkLog,
    WriteOffAct,
)
from .reporting import report_ppe_scoped
from .services import (
    create_backup_payload,
    create_work_log,
    create_primary_document,
    create_procurement_request,
    create_stock_issue,
    create_stock_receipt,
    create_supplier_document,
    create_writeoff,
    load_operation_draft,
    restore_backup_payload,
    save_operation_draft,
    transition_document,
    workflow_allowed_statuses,
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
                "items": "MAT-001|10|100|Для монтажа",
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
                    "items": "MAT-001|10|100|Для монтажа",
                },
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
                "items": "MAT-001|3|100|route",
            },
        )
        record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=request.id)
        self.assertIn("workflow_created_by", record.metadata_json)
        self.assertIn("workflow_approved_by", record.metadata_json)
        self.assertIn("workflow_sent_accounting_by", record.metadata_json)
        self.assertIn("workflow_view_only", record.metadata_json)
        self.assertIn("workflow_route", record.metadata_json)

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
                "items": "MAT-001|20|100|Приход",
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
                "items": "MAT-001|8|100|Отпуск",
            },
        )
        warehouse_balance = StockMovement.objects.filter(material=self.material, location_name=settings.WAREHOUSE_NAME).aggregate(total=Sum("quantity_delta"))["total"]
        self.assertEqual(receipt.lines.count(), 1)
        self.assertEqual(warehouse_balance, Decimal("12"))

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
                    "items": "MAT-001|2|100|Отпуск",
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
                "items": "MAT-001|10|100|Для монтажа",
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
                "items": "MAT-001|10|125|Первичный документ",
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
            payload={"site_name": "Участок 12", "notes": "Черновик заявки", "items": "MAT-001|3|100|Автосохранение"},
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
                "items": "MAT-001|3|100|own",
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
                "items": "MAT-001|4|100|foreign",
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
                "items": "MAT-001|1|100|new",
            },
        )
        self.assertEqual(post_response.status_code, 403)

    def test_supplier_archive_shows_only_own_docs_and_allows_supply_confirmation(self) -> None:
        own_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": self.site_manager.site_name,
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": "MAT-001|5|100|own",
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
        response = self.client.get(reverse("archive"))
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, own_doc.doc_number)
        self.assertNotContains(response, "SUP-FOREIGN-001")

        record = next(item for item in response.context["records"] if item.entity_type == "supplier_document" and item.entity_id == own_doc.id)
        available_statuses = [value for value, _label in record.available_status_choices]
        self.assertIn(DocumentStatus.SUPPLY_CONFIRMED, available_statuses)
        self.assertNotIn(DocumentStatus.APPROVAL, available_statuses)

        post_response = self.client.post(
            reverse("archive"),
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
                "items": "MAT-001|5|100|Тест",
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
                "items": "MAT-001|5|100|Тест",
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
                "items": "MAT-001|7|100|Чужой",
            },
        )

        self.client.login(username="supplier", password="supplier123")
        response = self.client.get("/api/primary-documents/")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["supplier_name"], self.supplier.name)

    def test_operation_draft_endpoint_saves_procurement_draft(self) -> None:
        self.client.login(username="site", password="site123")
        response = self.client.post(
            reverse("operation-draft", kwargs={"slug": "procurement"}),
            {
                "request_date": timezone.localdate().isoformat(),
                "site_name": "Участок 12",
                "status": DocumentStatus.DRAFT,
                "notes": "Автосохранение из UI",
                "items": "MAT-001|2|100|Черновик",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = load_operation_draft(user=self.site_manager, operation_slug="procurement")
        self.assertEqual(payload["site_name"], "Участок 12")
        self.assertIn("Черновик", payload["items"])


    def test_accounting_dashboard_navigation_is_read_only(self) -> None:
        self.client.login(username="accounting", password="accounting123")
        response = self.client.get(reverse("dashboard"))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["catalog_links"], [])
        self.assertEqual(response.context["operation_links"], [])
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

    def test_accounting_archive_and_documents_api_show_only_approved_records(self) -> None:
        draft_request = create_procurement_request(
            user=self.site_manager,
            cleaned_data={
                "request_date": timezone.localdate(),
                "site_name": "Участок 12",
                "contract": self.contract,
                "supplier": self.supplier,
                "status": DocumentStatus.DRAFT,
                "notes": "",
                "items": "MAT-001|1|100|draft",
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
                "items": "MAT-001|2|100|approved",
            },
        )
        approved_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=approved_request.id)
        transition_document(user=self.director, record=approved_record, new_status=DocumentStatus.APPROVED)

        self.client.login(username="accounting", password="accounting123")
        archive_response = self.client.get(reverse("archive"))
        self.assertEqual(archive_response.status_code, 200)
        records = archive_response.context["records"]
        self.assertEqual([record.entity_id for record in records], [approved_request.id])
        self.assertTrue(records[0].can_update_status)
        available_statuses = [value for value, _label in records[0].available_status_choices]
        self.assertIn(DocumentStatus.ACCEPTED, available_statuses)
        self.assertIn(DocumentStatus.REWORK, available_statuses)

        post_response = self.client.post(
            reverse("archive"),
            {"record_id": approved_record.id, "new_status": DocumentStatus.ACCEPTED},
        )
        self.assertEqual(post_response.status_code, 302)
        approved_request.refresh_from_db()
        self.assertEqual(approved_request.status, DocumentStatus.ACCEPTED)

        api_response = self.client.get("/api/documents/")
        self.assertEqual(api_response.status_code, 200)
        payload = api_response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["entity_id"], approved_request.id)
        self.assertEqual(payload[0]["status"], DocumentStatus.ACCEPTED)

        draft_record = DocumentRecord.objects.get(entity_type="procurement_request", entity_id=draft_request.id)
        self.assertNotEqual(draft_record.id, approved_record.id)

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
            status=DocumentStatus.DRAFT,
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

