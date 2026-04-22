from __future__ import annotations

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin

from .models import (
    AuditLog,
    ConstructionObject,
    DocumentRecord,
    Material,
    MaterialNorm,
    PPEIssuance,
    PPEIssuanceLine,
    ProcurementRequest,
    ProcurementRequestLine,
    SMRContract,
    StockIssue,
    StockIssueLine,
    StockMovement,
    StockReceipt,
    StockReceiptLine,
    Supplier,
    SupplierDocument,
    SupplyContract,
    User,
    Worker,
    WorkLog,
    WriteOffAct,
    WriteOffLine,
)


class ProcurementLineInline(admin.TabularInline):
    model = ProcurementRequestLine
    extra = 0


class ReceiptLineInline(admin.TabularInline):
    model = StockReceiptLine
    extra = 0


class IssueLineInline(admin.TabularInline):
    model = StockIssueLine
    extra = 0


class WriteOffLineInline(admin.TabularInline):
    model = WriteOffLine
    extra = 0


class PPELineInline(admin.TabularInline):
    model = PPEIssuanceLine
    extra = 0


@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    list_display = ("username", "first_name", "last_name", "role", "site_name", "is_staff", "is_active")
    fieldsets = DjangoUserAdmin.fieldsets + (("Параметры АИС", {"fields": ("role", "site_name", "supplier")}),)
    add_fieldsets = DjangoUserAdmin.add_fieldsets + (("Параметры АИС", {"fields": ("role", "site_name", "supplier")}),)


@admin.register(Material)
class MaterialAdmin(admin.ModelAdmin):
    list_display = ("code", "name", "unit", "price", "min_stock", "category", "is_ppe")
    search_fields = ("code", "name")
    list_filter = ("category", "is_ppe")


@admin.register(Supplier)
class SupplierAdmin(admin.ModelAdmin):
    list_display = ("name", "tax_id", "contact_person", "phone", "email")
    search_fields = ("name", "tax_id")


@admin.register(ConstructionObject)
class ConstructionObjectAdmin(admin.ModelAdmin):
    list_display = ("name", "customer_name", "address", "start_date", "end_date")
    search_fields = ("name", "customer_name", "address")


@admin.register(Worker)
class WorkerAdmin(admin.ModelAdmin):
    list_display = ("full_name", "employee_number", "site_name", "position", "hire_date")
    search_fields = ("full_name", "employee_number")


@admin.register(MaterialNorm)
class MaterialNormAdmin(admin.ModelAdmin):
    list_display = ("work_type", "material", "norm_per_unit", "unit")
    search_fields = ("work_type", "material__code", "material__name")


@admin.register(SMRContract)
class SMRContractAdmin(admin.ModelAdmin):
    list_display = ("number", "contract_date", "customer_name", "amount", "status")
    search_fields = ("number", "customer_name", "subject")
    list_filter = ("status",)


@admin.register(SupplyContract)
class SupplyContractAdmin(admin.ModelAdmin):
    list_display = ("number", "contract_date", "supplier", "amount", "status")
    search_fields = ("number", "supplier__name")
    list_filter = ("status",)


@admin.register(ProcurementRequest)
class ProcurementRequestAdmin(admin.ModelAdmin):
    list_display = ("number", "request_date", "site_name", "supplier", "status")
    search_fields = ("number", "site_name", "supplier__name")
    list_filter = ("status",)
    inlines = [ProcurementLineInline]


@admin.register(SupplierDocument)
class SupplierDocumentAdmin(admin.ModelAdmin):
    list_display = ("doc_type", "doc_number", "doc_date", "supplier", "amount", "status")
    search_fields = ("doc_number", "supplier__name")
    list_filter = ("doc_type", "status")


@admin.register(StockReceipt)
class StockReceiptAdmin(admin.ModelAdmin):
    list_display = ("number", "receipt_date", "supplier", "status")
    search_fields = ("number", "supplier__name")
    list_filter = ("status",)
    inlines = [ReceiptLineInline]


@admin.register(StockIssue)
class StockIssueAdmin(admin.ModelAdmin):
    list_display = ("number", "issue_date", "site_name", "received_by_name", "status")
    search_fields = ("number", "site_name", "received_by_name")
    list_filter = ("status",)
    inlines = [IssueLineInline]


@admin.register(WorkLog)
class WorkLogAdmin(admin.ModelAdmin):
    list_display = ("site_name", "work_type", "plan_date", "actual_date", "status")
    search_fields = ("site_name", "work_type")
    list_filter = ("status",)


@admin.register(WriteOffAct)
class WriteOffActAdmin(admin.ModelAdmin):
    list_display = ("number", "act_date", "site_name", "work_type", "status")
    search_fields = ("number", "site_name", "work_type")
    list_filter = ("status",)
    inlines = [WriteOffLineInline]


@admin.register(PPEIssuance)
class PPEIssuanceAdmin(admin.ModelAdmin):
    list_display = ("number", "issue_date", "site_name", "season", "status")
    search_fields = ("number", "site_name")
    list_filter = ("status", "season")
    inlines = [PPELineInline]


@admin.register(StockMovement)
class StockMovementAdmin(admin.ModelAdmin):
    list_display = ("movement_date", "material", "quantity_delta", "location_name", "source_type")
    search_fields = ("material__code", "material__name", "location_name")
    list_filter = ("source_type", "location_name")


@admin.register(DocumentRecord)
class DocumentRecordAdmin(admin.ModelAdmin):
    list_display = ("doc_type", "doc_number", "doc_date", "status", "counterparty", "object_name")
    search_fields = ("doc_type", "doc_number", "counterparty", "object_name", "search_text")
    list_filter = ("status", "doc_type")


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    list_display = ("created_at", "user", "action", "entity_type", "entity_id")
    search_fields = ("action", "entity_type", "details")
