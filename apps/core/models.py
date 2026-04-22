from __future__ import annotations

import calendar
from datetime import date

from django.conf import settings
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone


def _add_months(base_date: date, months: int) -> date:
    if months <= 0:
        return base_date
    month_offset = base_date.month - 1 + months
    year = base_date.year + month_offset // 12
    month = month_offset % 12 + 1
    day = min(base_date.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


class TimeStampedModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class RoleChoices(models.TextChoices):
    DIRECTOR = "director", "Начальник монтажного объекта"
    PROCUREMENT = "procurement", "Снабженец"
    WAREHOUSE = "warehouse", "Кладовщик"
    SITE_MANAGER = "site_manager", "Начальник участка"
    ACCOUNTING = "accounting", "Бухгалтерия"
    SUPPLIER = "supplier", "Поставщик"
    ADMIN = "admin", "Администратор"


class DocumentStatus(models.TextChoices):
    DRAFT = "draft", "Черновик"
    APPROVAL = "approval", "На утверждении"
    APPROVED = "approved", "Утвержден"
    SENT_ACCOUNTING = "sent_accounting", "Отправлен в бухгалтерию"
    ACCEPTED = "accepted", "Принят"
    REWORK = "rework", "Возвращен на доработку"
    UPLOADED = "uploaded", "Загружен поставщиком"
    SUPPLY_CONFIRMED = "supply_confirmed", "Подтверждение поставки"


class DocumentType(TimeStampedModel):
    code = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=128, unique=True)
    prefix = models.CharField(max_length=16, unique=True)
    is_active = models.BooleanField(default=True)
    available_for_upload = models.BooleanField(default=False)
    available_for_generation = models.BooleanField(default=False)
    requires_items = models.BooleanField(default=True)
    description = models.TextField(blank=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class Supplier(TimeStampedModel):
    name = models.CharField(max_length=255, unique=True)
    tax_id = models.CharField(max_length=32, blank=True)
    contact_person = models.CharField(max_length=255, blank=True)
    phone = models.CharField(max_length=64, blank=True)
    email = models.EmailField(blank=True)
    address = models.TextField(blank=True)
    requisites = models.TextField(blank=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name

    def requisites_text(self) -> str:
        explicit = (self.requisites or "").strip()
        if explicit:
            return explicit

        parts: list[str] = []
        if self.tax_id:
            parts.append(f"ИНН {self.tax_id}")
        if self.address:
            parts.append(self.address.strip())
        if self.phone:
            parts.append(f"Тел.: {self.phone}")
        if self.email:
            parts.append(f"Эл. почта: {self.email}")
        return "; ".join(part for part in parts if part)


class User(AbstractUser):
    role = models.CharField(max_length=32, choices=RoleChoices.choices, default=RoleChoices.SITE_MANAGER)
    site_name = models.CharField(max_length=255, blank=True)
    supplier = models.ForeignKey(Supplier, null=True, blank=True, on_delete=models.SET_NULL, related_name="users")

    class Meta:
        ordering = ["last_name", "first_name", "username"]

    @property
    def full_name_or_username(self) -> str:
        full_name = self.get_full_name().strip()
        return full_name or self.username

    @property
    def role_label(self) -> str:
        return dict(RoleChoices.choices).get(self.role, self.role)


class Material(TimeStampedModel):
    code = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    unit = models.CharField(max_length=32)
    price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    min_stock = models.DecimalField(max_digits=14, decimal_places=3, default=0)
    category = models.CharField(max_length=128, blank=True)
    is_ppe = models.BooleanField(default=False)

    class Meta:
        ordering = ["code"]

    def __str__(self) -> str:
        return f"{self.code} - {self.name}"


class ConstructionObject(TimeStampedModel):
    name = models.CharField(max_length=255)
    address = models.CharField(max_length=255, blank=True)
    customer_name = models.CharField(max_length=255, blank=True)
    customer_requisites = models.TextField(blank=True)
    description = models.TextField(blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class Worker(TimeStampedModel):
    full_name = models.CharField(max_length=255)
    employee_number = models.CharField(max_length=64, unique=True)
    site_name = models.CharField(max_length=255, blank=True)
    position = models.CharField(max_length=255, blank=True)
    hire_date = models.DateField(null=True, blank=True)

    class Meta:
        ordering = ["full_name"]

    def __str__(self) -> str:
        return f"{self.full_name} ({self.employee_number})"


class MaterialNorm(TimeStampedModel):
    work_type = models.CharField(max_length=255)
    material = models.ForeignKey(Material, on_delete=models.CASCADE, related_name="norms")
    norm_per_unit = models.DecimalField(max_digits=14, decimal_places=4)
    unit = models.CharField(max_length=32, blank=True)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["work_type", "material__code"]
        constraints = [models.UniqueConstraint(fields=["work_type", "material"], name="uq_material_norm_work_type")]

    def __str__(self) -> str:
        return f"{self.work_type}: {self.material.code}"


class SMRContract(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    contract_date = models.DateField()
    object = models.ForeignKey(ConstructionObject, null=True, blank=True, on_delete=models.SET_NULL, related_name="contracts")
    customer_name = models.CharField(max_length=255)
    customer_requisites = models.TextField(blank=True)
    contractor_name = models.CharField(max_length=255, blank=True)
    contractor_requisites = models.TextField(blank=True)
    subject = models.CharField(max_length=255)
    work_type = models.CharField(max_length=255, blank=True)
    planned_volume = models.DecimalField(max_digits=14, decimal_places=3, null=True, blank=True)
    volume_unit = models.CharField(max_length=64, blank=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2)
    vat_rate = models.DecimalField(max_digits=5, decimal_places=2, default=20)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="created_contracts")

    class Meta:
        ordering = ["-contract_date", "-id"]

    def __str__(self) -> str:
        return self.number

    def resolved_customer_name(self) -> str:
        value = (self.customer_name or "").strip()
        if value:
            return value
        if self.object:
            return (self.object.customer_name or "").strip()
        return ""

    def resolved_customer_requisites(self) -> str:
        value = (self.customer_requisites or "").strip()
        if value:
            return value
        if self.object:
            return (self.object.customer_requisites or "").strip()
        return ""

    def resolved_contractor_name(self) -> str:
        value = (self.contractor_name or "").strip()
        if value:
            return value
        profile = getattr(settings, "ORGANIZATION_PROFILE", {}) or {}
        return str(profile.get("name", "")).strip()

    def resolved_contractor_requisites(self) -> str:
        value = (self.contractor_requisites or "").strip()
        if value:
            return value

        profile = getattr(settings, "ORGANIZATION_PROFILE", {}) or {}
        explicit = str(profile.get("requisites", "")).strip()
        if explicit:
            return explicit

        parts: list[str] = []
        if profile.get("tax_id"):
            parts.append(f"ИНН {profile['tax_id']}")
        if profile.get("kpp"):
            parts.append(f"КПП {profile['kpp']}")
        if profile.get("ogrn"):
            parts.append(f"ОГРН {profile['ogrn']}")
        if profile.get("address"):
            parts.append(str(profile["address"]).strip())
        if profile.get("bank_details"):
            parts.append(str(profile["bank_details"]).strip())
        return "; ".join(part for part in parts if part)


class SupplyContract(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    contract_date = models.DateField()
    supplier = models.ForeignKey(Supplier, on_delete=models.PROTECT, related_name="supply_contracts")
    related_smr_contract = models.ForeignKey(SMRContract, null=True, blank=True, on_delete=models.SET_NULL, related_name="supply_contracts")
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    terms = models.TextField(blank=True)

    class Meta:
        ordering = ["-contract_date", "-id"]

    def __str__(self) -> str:
        return self.number


class ProcurementRequest(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    request_date = models.DateField()
    site_name = models.CharField(max_length=255)
    contract = models.ForeignKey(SMRContract, null=True, blank=True, on_delete=models.SET_NULL, related_name="procurement_requests")
    supplier = models.ForeignKey(Supplier, null=True, blank=True, on_delete=models.SET_NULL, related_name="procurement_requests")
    requested_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="procurement_requests")
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-request_date", "-id"]

    def __str__(self) -> str:
        return self.number


class ProcurementRequestLine(models.Model):
    request = models.ForeignKey(ProcurementRequest, on_delete=models.CASCADE, related_name="lines")
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="procurement_lines")
    quantity = models.DecimalField(max_digits=14, decimal_places=3)
    unit_price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    notes = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ["material__code"]


class SupplierDocument(TimeStampedModel):
    supplier = models.ForeignKey(Supplier, on_delete=models.PROTECT, related_name="documents")
    request = models.ForeignKey(ProcurementRequest, null=True, blank=True, on_delete=models.SET_NULL, related_name="supplier_documents")
    supply_contract = models.ForeignKey(SupplyContract, null=True, blank=True, on_delete=models.SET_NULL, related_name="supplier_documents")
    doc_type = models.CharField(max_length=64)
    doc_number = models.CharField(max_length=128)
    doc_date = models.DateField()
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    vat_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    uploaded_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="uploaded_supplier_documents")
    attachment = models.FileField(upload_to="supplier_docs/", blank=True, null=True)
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.UPLOADED)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-doc_date", "-id"]

    def __str__(self) -> str:
        return f"{self.doc_type} {self.doc_number}"


class PrimaryDocument(TimeStampedModel):
    document_type = models.ForeignKey(DocumentType, on_delete=models.PROTECT, related_name="primary_documents")
    number = models.CharField(max_length=128, unique=True)
    doc_date = models.DateField()
    supplier = models.ForeignKey(Supplier, on_delete=models.PROTECT, related_name="primary_documents")
    procurement_request = models.ForeignKey(ProcurementRequest, null=True, blank=True, on_delete=models.SET_NULL, related_name="primary_documents")
    supply_contract = models.ForeignKey(SupplyContract, null=True, blank=True, on_delete=models.SET_NULL, related_name="primary_documents")
    stock_receipt = models.ForeignKey("StockReceipt", null=True, blank=True, on_delete=models.SET_NULL, related_name="primary_documents")
    site_name = models.CharField(max_length=255, blank=True)
    basis_reference = models.CharField(max_length=255, blank=True)
    amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    vat_amount = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="primary_documents")
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-doc_date", "-id"]

    def __str__(self) -> str:
        return f"{self.document_type.name} {self.number}"


class PrimaryDocumentLine(models.Model):
    document = models.ForeignKey(PrimaryDocument, on_delete=models.CASCADE, related_name="lines")
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="primary_document_lines")
    quantity = models.DecimalField(max_digits=14, decimal_places=3)
    unit_price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    notes = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ["material__code"]


class StockReceipt(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    receipt_date = models.DateField()
    supplier = models.ForeignKey(Supplier, on_delete=models.PROTECT, related_name="stock_receipts")
    supplier_document = models.ForeignKey(SupplierDocument, null=True, blank=True, on_delete=models.SET_NULL, related_name="stock_receipts")
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="stock_receipts")
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-receipt_date", "-id"]

    def __str__(self) -> str:
        return self.number


class StockReceiptLine(models.Model):
    receipt = models.ForeignKey(StockReceipt, on_delete=models.CASCADE, related_name="lines")
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="receipt_lines")
    quantity = models.DecimalField(max_digits=14, decimal_places=3)
    unit_price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    notes = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ["material__code"]


class StockIssue(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    issue_date = models.DateField()
    site_name = models.CharField(max_length=255)
    contract = models.ForeignKey(SMRContract, null=True, blank=True, on_delete=models.SET_NULL, related_name="stock_issues")
    issued_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="stock_issues")
    received_by_name = models.CharField(max_length=255)
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-issue_date", "-id"]

    def __str__(self) -> str:
        return self.number


class StockIssueLine(models.Model):
    issue = models.ForeignKey(StockIssue, on_delete=models.CASCADE, related_name="lines")
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="issue_lines")
    quantity = models.DecimalField(max_digits=14, decimal_places=3)
    unit_price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    notes = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ["material__code"]


class WorkLog(TimeStampedModel):
    site_name = models.CharField(max_length=255)
    contract = models.ForeignKey(SMRContract, null=True, blank=True, on_delete=models.SET_NULL, related_name="work_logs")
    work_type = models.CharField(max_length=255)
    planned_volume = models.DecimalField(max_digits=14, decimal_places=3, default=0)
    actual_volume = models.DecimalField(max_digits=14, decimal_places=3, default=0)
    volume_unit = models.CharField(max_length=64, blank=True)
    plan_date = models.DateField(null=True, blank=True)
    actual_date = models.DateField(null=True, blank=True)
    status = models.CharField(max_length=64, default="Запланировано")
    notes = models.TextField(blank=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="work_logs")

    class Meta:
        ordering = ["-actual_date", "-plan_date", "-id"]

    @property
    def status_label(self) -> str:
        status_map = {
            "planned": "Запланировано",
            "delayed": "С задержкой",
        }
        return status_map.get(self.status, self.status)


class WriteOffAct(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    act_date = models.DateField()
    contract = models.ForeignKey(SMRContract, on_delete=models.PROTECT, related_name="write_off_acts")
    site_name = models.CharField(max_length=255)
    work_type = models.CharField(max_length=255)
    work_volume = models.DecimalField(max_digits=14, decimal_places=3)
    volume_unit = models.CharField(max_length=64, blank=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="write_off_acts")
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-act_date", "-id"]

    def __str__(self) -> str:
        return self.number


class WriteOffLine(models.Model):
    act = models.ForeignKey(WriteOffAct, on_delete=models.CASCADE, related_name="lines")
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="writeoff_lines")
    norm_per_unit = models.DecimalField(max_digits=14, decimal_places=4)
    calculated_quantity = models.DecimalField(max_digits=14, decimal_places=3)
    actual_quantity = models.DecimalField(max_digits=14, decimal_places=3)
    unit_price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["material__code"]


class PPEIssuance(TimeStampedModel):
    number = models.CharField(max_length=128, unique=True)
    issue_date = models.DateField()
    site_name = models.CharField(max_length=255)
    season = models.CharField(max_length=64, blank=True)
    issued_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="ppe_issuances")
    status = models.CharField(max_length=32, choices=DocumentStatus.choices, default=DocumentStatus.DRAFT)
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-issue_date", "-id"]

    def __str__(self) -> str:
        return self.number


class PPEIssuanceLine(models.Model):
    REPLACEMENT_WARNING_DAYS = 30
    REPLACEMENT_STATUS_OK = "ok"
    REPLACEMENT_STATUS_EXPIRING = "expiring_soon"
    REPLACEMENT_STATUS_EXPIRED = "expired"

    issuance = models.ForeignKey(PPEIssuance, on_delete=models.CASCADE, related_name="lines")
    worker = models.ForeignKey(Worker, on_delete=models.PROTECT, related_name="ppe_lines")
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="ppe_lines")
    quantity = models.DecimalField(max_digits=14, decimal_places=3)
    service_life_months = models.PositiveIntegerField(default=0)
    issue_start_date = models.DateField(null=True, blank=True)
    notes = models.CharField(max_length=255, blank=True)

    class Meta:
        ordering = ["worker__full_name", "material__code"]

    @property
    def replacement_start_date(self) -> date | None:
        return self.issue_start_date or getattr(self.issuance, "issue_date", None)

    @property
    def replacement_due_date(self) -> date | None:
        start_date = self.replacement_start_date
        if not start_date or self.service_life_months <= 0:
            return None
        return _add_months(start_date, self.service_life_months)

    @property
    def days_until_replacement(self) -> int | None:
        due_date = self.replacement_due_date
        if not due_date:
            return None
        return (due_date - timezone.localdate()).days

    @property
    def replacement_status(self) -> str:
        days_left = self.days_until_replacement
        if days_left is None:
            return self.REPLACEMENT_STATUS_OK
        if days_left < 0:
            return self.REPLACEMENT_STATUS_EXPIRED
        if days_left <= self.REPLACEMENT_WARNING_DAYS:
            return self.REPLACEMENT_STATUS_EXPIRING
        return self.REPLACEMENT_STATUS_OK

    @property
    def replacement_status_label(self) -> str:
        label_map = {
            self.REPLACEMENT_STATUS_OK: "В норме",
            self.REPLACEMENT_STATUS_EXPIRING: "Истекает срок",
            self.REPLACEMENT_STATUS_EXPIRED: "Срок истек",
        }
        return label_map.get(self.replacement_status, self.replacement_status)

    @property
    def needs_replacement(self) -> bool:
        return self.replacement_status in {self.REPLACEMENT_STATUS_EXPIRING, self.REPLACEMENT_STATUS_EXPIRED}

    @property
    def replacement_warning(self) -> str:
        days_left = self.days_until_replacement
        if days_left is None:
            return ""
        if days_left < 0:
            return f"Просрочено на {abs(days_left)} дн."
        if days_left <= self.REPLACEMENT_WARNING_DAYS:
            return f"Истекает через {days_left} дн."
        return ""


class StockMovement(TimeStampedModel):
    movement_date = models.DateField()
    material = models.ForeignKey(Material, on_delete=models.PROTECT, related_name="movements")
    quantity_delta = models.DecimalField(max_digits=14, decimal_places=3)
    location_name = models.CharField(max_length=255)
    source_type = models.CharField(max_length=64)
    source_id = models.PositiveBigIntegerField()
    unit_price = models.DecimalField(max_digits=14, decimal_places=2, default=0)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="stock_movements")
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["-movement_date", "-id"]
        indexes = [
            models.Index(fields=["location_name", "movement_date"]),
            models.Index(fields=["source_type", "source_id"]),
        ]


class DocumentRecord(TimeStampedModel):
    entity_type = models.CharField(max_length=64)
    entity_id = models.PositiveBigIntegerField()
    doc_type = models.CharField(max_length=128)
    doc_number = models.CharField(max_length=128)
    doc_date = models.DateField()
    status = models.CharField(max_length=32, choices=DocumentStatus.choices)
    title = models.CharField(max_length=255)
    counterparty = models.CharField(max_length=255, blank=True)
    object_name = models.CharField(max_length=255, blank=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="document_records")
    file_path = models.CharField(max_length=512, blank=True)
    metadata_json = models.JSONField(default=dict, blank=True)
    search_text = models.TextField(blank=True)

    class Meta:
        ordering = ["-doc_date", "-id"]
        constraints = [models.UniqueConstraint(fields=["entity_type", "entity_id"], name="uq_document_record_entity")]
        indexes = [
            models.Index(fields=["doc_type", "doc_date"]),
            models.Index(fields=["status", "doc_date"]),
        ]


class FormDraft(TimeStampedModel):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE, related_name="form_drafts")
    operation_slug = models.CharField(max_length=64)
    payload_json = models.JSONField(default=dict, blank=True)

    class Meta:
        ordering = ["-updated_at", "-id"]
        constraints = [models.UniqueConstraint(fields=["user", "operation_slug"], name="uq_form_draft_user_slug")]


class AuditLog(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True, on_delete=models.SET_NULL, related_name="audit_entries")
    action = models.CharField(max_length=64)
    entity_type = models.CharField(max_length=64)
    entity_id = models.PositiveBigIntegerField(null=True, blank=True)
    details = models.TextField(blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-created_at", "-id"]
