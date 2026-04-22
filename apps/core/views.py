from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
import json
from pathlib import Path
from typing import Any, Callable

from django import forms as django_forms
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.db.models import Q
from django.db.models.deletion import ProtectedError
from django.http import FileResponse, Http404, HttpRequest, HttpResponse, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from .access import (
    ROLE_SET_ALL,
    ROLE_SET_ARCHIVE,
    ROLE_SET_AUDIT_LOG,
    ROLE_SET_BACKUP,
    ROLE_SET_OFFICE,
    ROLE_SET_REPORTS,
    can_access_archive,
    can_access_audit_log,
    can_access_backups,
    can_access_reports,
    can_update_archive_status,
)
from .exports import Exporter
from .forms import (
    ArchiveFilterForm,
    AuditLogFilterForm,
    BackupRestoreUploadForm,
    ConstructionObjectForm,
    DocumentTypeForm,
    MaterialForm,
    MaterialNormForm,
    PPEIssuanceCreateForm,
    PrimaryDocumentCreateForm,
    ProcurementRequestCreateForm,
    ReportFilterForm,
    SMRContractForm,
    StockIssueCreateForm,
    StockReceiptCreateForm,
    SupplierDocumentUploadForm,
    SupplierForm,
    SupplyContractForm,
    UserForm,
    WorkerForm,
    WorkLogCreateForm,
    WriteOffCreateForm,
)
from .models import (
    AuditLog,
    ConstructionObject,
    DocumentRecord,
    DocumentStatus,
    DocumentType,
    Material,
    MaterialNorm,
    PPEIssuance,
    PrimaryDocument,
    ProcurementRequest,
    RoleChoices,
    SMRContract,
    StockIssue,
    StockReceipt,
    Supplier,
    SupplierDocument,
    SupplyContract,
    User,
    Worker,
    WorkLog,
    WriteOffAct,
)
from .reporting import REPORT_PROVIDERS, REPORT_TITLES
from .services import (
    backup_files,
    clear_operation_draft,
    create_ppe_issuance,
    create_primary_document,
    create_procurement_request,
    create_supplier_document,
    create_stock_issue,
    create_stock_receipt,
    create_work_log,
    create_writeoff,
    dashboard_metrics,
    document_records,
    filter_queryset_for_user,
    load_operation_draft,
    low_stock_alerts,
    restore_backup_payload,
    restore_backup_file,
    save_operation_draft,
    site_balances,
    transition_document,
    warehouse_balances,
    workflow_allowed_statuses,
    write_backup_file,
)



CATALOG_CONFIG: dict[str, dict[str, Any]] = {
    "materials": {
        "title": "Справочник материалов",
        "description": "Материалы, цены, минимальные остатки и признак СИЗ.",
        "form_class": MaterialForm,
        "queryset": lambda: Material.objects.order_by("code"),
        "columns": [
            ("Код", lambda obj: obj.code),
            ("Наименование", lambda obj: obj.name),
            ("Ед.", lambda obj: obj.unit),
            ("Цена", lambda obj: obj.price),
            ("Мин. остаток", lambda obj: obj.min_stock),
            ("Категория", lambda obj: obj.category),
            ("СИЗ", lambda obj: obj.is_ppe),
        ],
        "allowed_roles": ROLE_SET_OFFICE | {RoleChoices.SITE_MANAGER},
    },
    "suppliers": {
        "title": "Справочник поставщиков",
        "description": "Контрагенты, контактные лица и реквизиты для закупок.",
        "form_class": SupplierForm,
        "queryset": lambda: Supplier.objects.order_by("name"),
        "columns": [
            ("Поставщик", lambda obj: obj.name),
            ("ИНН", lambda obj: obj.tax_id),
            ("Контакт", lambda obj: obj.contact_person),
            ("Телефон", lambda obj: obj.phone),
            ("Эл. почта", lambda obj: obj.email),
        ],
        "allowed_roles": ROLE_SET_OFFICE | {RoleChoices.SITE_MANAGER},
    },
    "objects": {
        "title": "Строительные объекты",
        "description": "Объекты строительства и связанная информация по заказчику.",
        "form_class": ConstructionObjectForm,
        "queryset": lambda: ConstructionObject.objects.order_by("name"),
        "columns": [
            ("Объект", lambda obj: obj.name),
            ("Заказчик", lambda obj: obj.customer_name),
            ("Адрес", lambda obj: obj.address),
            ("Начало", lambda obj: obj.start_date),
            ("Окончание", lambda obj: obj.end_date),
        ],
        "allowed_roles": ROLE_SET_OFFICE | {RoleChoices.SITE_MANAGER},
    },
    "workers": {
        "title": "Работники",
        "description": "Табельные номера и принадлежность к участкам.",
        "form_class": WorkerForm,
        "queryset": lambda: Worker.objects.order_by("full_name"),
        "columns": [
            ("ФИО", lambda obj: obj.full_name),
            ("Табельный номер", lambda obj: obj.employee_number),
            ("Участок", lambda obj: obj.site_name),
            ("Должность", lambda obj: obj.position),
            ("Дата приема", lambda obj: obj.hire_date),
        ],
        "allowed_roles": ROLE_SET_OFFICE | {RoleChoices.SITE_MANAGER},
    },
    "norms": {
        "title": "Нормы расхода",
        "description": "Нормативы списания материалов по видам работ.",
        "form_class": MaterialNormForm,
        "queryset": lambda: MaterialNorm.objects.select_related("material").order_by("work_type", "material__code"),
        "columns": [
            ("Вид работ", lambda obj: obj.work_type),
            ("Материал", lambda obj: obj.material.code),
            ("Наименование", lambda obj: obj.material.name),
            ("Норма", lambda obj: obj.norm_per_unit),
            ("Ед.", lambda obj: obj.unit or obj.material.unit),
        ],
        "allowed_roles": ROLE_SET_OFFICE | {RoleChoices.SITE_MANAGER},
    },
    "contracts": {
        "title": "Договоры СМР",
        "description": "Основные договоры с заказчиками по строительно-монтажным работам.",
        "form_class": SMRContractForm,
        "queryset": lambda: SMRContract.objects.select_related("object", "created_by").order_by("-contract_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.contract_date),
            ("Заказчик", lambda obj: obj.customer_name),
            ("Объект", lambda obj: obj.object.name if obj.object else ""),
            ("Сумма", lambda obj: obj.amount),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.DIRECTOR, RoleChoices.SITE_MANAGER},
        "scoped_roles": {RoleChoices.SITE_MANAGER},
        "read_only_roles": {RoleChoices.SITE_MANAGER},
        "save_callback": "save_contract",
    },
    "supply-contracts": {
        "title": "Договоры поставки",
        "description": "Договоры на закупку и поставку материалов от контрагентов.",
        "form_class": SupplyContractForm,
        "queryset": lambda: SupplyContract.objects.select_related("supplier", "related_smr_contract").order_by("-contract_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.contract_date),
            ("Поставщик", lambda obj: obj.supplier.name),
            ("Договор СМР", lambda obj: obj.related_smr_contract.number if obj.related_smr_contract else ""),
            ("Сумма", lambda obj: obj.amount),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.DIRECTOR, RoleChoices.PROCUREMENT, RoleChoices.SUPPLIER},
        "scoped_roles": {RoleChoices.SUPPLIER},
        "read_only_roles": {RoleChoices.SUPPLIER},
    },
    "document-types": {
        "title": "Типы документов",
        "description": "Справочник системных типов документов для загрузки и генерации.",
        "form_class": DocumentTypeForm,
        "queryset": lambda: DocumentType.objects.order_by("name"),
        "columns": [
            ("Код", lambda obj: obj.code),
            ("Наименование", lambda obj: obj.name),
            ("Префикс", lambda obj: obj.prefix),
            ("Загрузка", lambda obj: obj.available_for_upload),
            ("Генерация", lambda obj: obj.available_for_generation),
            ("Активен", lambda obj: obj.is_active),
        ],
        "allowed_roles": ROLE_SET_OFFICE,
    },
    "users": {
        "title": "Пользователи",
        "description": "Учетные записи ролей АИС и привязка к участкам/поставщикам.",
        "form_class": UserForm,
        "queryset": lambda: User.objects.select_related("supplier").order_by("username"),
        "columns": [
            ("Логин", lambda obj: obj.username),
            ("ФИО", lambda obj: obj.full_name_or_username),
            ("Роль", lambda obj: obj.role_label),
            ("Участок", lambda obj: obj.site_name),
            ("Поставщик", lambda obj: obj.supplier.name if obj.supplier else ""),
            ("Активен", lambda obj: obj.is_active),
        ],
        "allowed_roles": {RoleChoices.ADMIN},
        "save_callback": "save_user",
    },
}


OPERATION_CONFIG: dict[str, dict[str, Any]] = {
    "procurement": {
        "title": "Заявки на закупку",
        "description": "Создание заявок поставщику с перечнем материалов и суммой позиций.",
        "form_class": ProcurementRequestCreateForm,
        "handler": create_procurement_request,
        "queryset": lambda: ProcurementRequest.objects.select_related("supplier", "contract", "requested_by").order_by("-request_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.request_date),
            ("Участок", lambda obj: obj.site_name),
            ("Поставщик", lambda obj: obj.supplier.name if obj.supplier else ""),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SITE_MANAGER, RoleChoices.SUPPLIER},
        "read_only_roles": {RoleChoices.SUPPLIER},
        "initial": lambda request: {"request_date": date.today(), "site_name": request.user.site_name or ""},
        "sample_lines": "MAT-001 | 100 | 280.50 | Для объекта\nMAT-002 | 20 | 1500 | Срочно",
    },
    "supplier-documents": {
        "title": "Документы поставщиков",
        "description": "Загрузка счетов, счетов-фактур и накладных с привязкой к заявке или договору.",
        "form_class": SupplierDocumentUploadForm,
        "handler": create_supplier_document,
        "queryset": lambda: SupplierDocument.objects.select_related("supplier", "request", "supply_contract").order_by("-doc_date", "-id"),
        "columns": [
            ("Тип", lambda obj: obj.doc_type),
            ("Номер", lambda obj: obj.doc_number),
            ("Дата", lambda obj: obj.doc_date),
            ("Поставщик", lambda obj: obj.supplier.name),
            ("Сумма", lambda obj: obj.amount),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SUPPLIER},
        "scope_form_for_supplier": True,
        "initial": lambda request: {"doc_date": date.today(), "supplier": request.user.supplier if request.user.supplier_id else None},
    },
    "primary-documents": {
        "title": "Первичные документы",
        "description": "Генерация счетов, счетов-фактур, товарных и приходных накладных на основе заявки, договора или прихода.",
        "form_class": PrimaryDocumentCreateForm,
        "handler": create_primary_document,
        "queryset": lambda: PrimaryDocument.objects.select_related(
            "document_type",
            "supplier",
            "procurement_request",
            "supply_contract",
            "stock_receipt",
            "created_by",
        ).order_by("-doc_date", "-id"),
        "columns": [
            ("Тип", lambda obj: obj.document_type.name),
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.doc_date),
            ("Поставщик", lambda obj: obj.supplier.name),
            ("Основание", lambda obj: obj.basis_reference),
            ("Сумма", lambda obj: obj.amount),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE},
        "initial": lambda request: {"doc_date": date.today(), "status": DocumentStatus.DRAFT},
        "sample_lines": "MAT-001 | 100 | 280.50 | Счет по заявке\nMAT-002 | 20 | 1500 | Поставка на склад",
    },
    "receipts": {
        "title": "Приход на склад",
        "description": "Оформление приходных ордеров и пополнение остатков центрального склада.",
        "form_class": StockReceiptCreateForm,
        "handler": create_stock_receipt,
        "queryset": lambda: StockReceipt.objects.select_related("supplier", "supplier_document", "created_by").order_by("-receipt_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.receipt_date),
            ("Поставщик", lambda obj: obj.supplier.name),
            ("Документ", lambda obj: obj.supplier_document.doc_number if obj.supplier_document else ""),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE},
        "initial": lambda request: {"receipt_date": date.today()},
        "sample_lines": "MAT-001 | 50 | 300 | Первая партия",
    },
    "issues": {
        "title": "Отпуск материалов",
        "description": "Требования-накладные на передачу материалов с центрального склада на участок.",
        "form_class": StockIssueCreateForm,
        "handler": create_stock_issue,
        "queryset": lambda: StockIssue.objects.select_related("contract", "issued_by").order_by("-issue_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.issue_date),
            ("Участок", lambda obj: obj.site_name),
            ("Получатель", lambda obj: obj.received_by_name),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE},
        "initial": lambda request: {"issue_date": date.today(), "site_name": request.user.site_name or ""},
        "sample_lines": "MAT-001 | 10 | 300 | Передача на объект",
    },
    "writeoffs": {
        "title": "Акты списания",
        "description": "Списание материалов по нормативам на основании объема выполненных работ.",
        "form_class": WriteOffCreateForm,
        "handler": create_writeoff,
        "queryset": lambda: WriteOffAct.objects.select_related("contract", "created_by").order_by("-act_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.act_date),
            ("Участок", lambda obj: obj.site_name),
            ("Вид работ", lambda obj: obj.work_type),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER},
        "initial": lambda request: {"act_date": date.today(), "site_name": request.user.site_name or ""},
    },
    "ppe": {
        "title": "Выдача спецодежды",
        "description": "Учет СИЗ и спецодежды по работникам и срокам службы.",
        "form_class": PPEIssuanceCreateForm,
        "handler": create_ppe_issuance,
        "queryset": lambda: PPEIssuance.objects.select_related("issued_by").order_by("-issue_date", "-id"),
        "columns": [
            ("Номер", lambda obj: obj.number),
            ("Дата", lambda obj: obj.issue_date),
            ("Участок", lambda obj: obj.site_name),
            ("Сезон", lambda obj: obj.season),
            ("Статус", lambda obj: obj.get_status_display()),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER},
        "initial": lambda request: {"issue_date": date.today(), "site_name": request.user.site_name or ""},
        "sample_lines": "EMP-001 | PPE-001 | 1 | 12",
    },
    "worklogs": {
        "title": "Журнал работ",
        "description": "Фиксация плановых и фактических объемов работ по участкам.",
        "form_class": WorkLogCreateForm,
        "handler": create_work_log,
        "queryset": lambda: WorkLog.objects.select_related("contract", "created_by").order_by("-actual_date", "-plan_date", "-id"),
        "columns": [
            ("Участок", lambda obj: obj.site_name),
            ("Договор", lambda obj: obj.contract.number if obj.contract else ""),
            ("Вид работ", lambda obj: obj.work_type),
            ("План", lambda obj: obj.planned_volume),
            ("Факт", lambda obj: obj.actual_volume),
            ("Статус", lambda obj: obj.status_label),
        ],
        "allowed_roles": {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER},
        "initial": lambda request: {"site_name": request.user.site_name or "", "plan_date": date.today()},
    },
}


def _require_roles(request: HttpRequest, allowed_roles: set[str]) -> None:
    if request.user.is_superuser:
        return
    if getattr(request.user, "role", None) not in allowed_roles:
        raise PermissionDenied("Недостаточно прав для выполнения операции.")


def _client_ip(request: HttpRequest) -> str | None:
    forwarded = request.META.get("HTTP_X_FORWARDED_FOR")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.META.get("REMOTE_ADDR")


def _material_catalog(*, ppe_only: bool = False) -> list[dict[str, str]]:
    queryset = Material.objects.order_by("code")
    if ppe_only:
        queryset = queryset.filter(is_ppe=True)
    return [
        {
            "code": row["code"],
            "name": row["name"],
            "unit": row["unit"],
            "price": str(row["price"]),
        }
        for row in queryset.values("code", "name", "unit", "price")
    ]


def _worker_catalog() -> list[dict[str, str]]:
    return [
        {
            "employee_number": row["employee_number"],
            "full_name": row["full_name"],
            "site_name": row["site_name"],
        }
        for row in Worker.objects.order_by("employee_number").values("employee_number", "full_name", "site_name")
    ]


def _format_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, bool):
        return "Да" if value else "Нет"
    if isinstance(value, Decimal):
        return f"{value:.3f}".rstrip("0").rstrip(".")
    if isinstance(value, (date, datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _table_rows(queryset, columns: list[tuple[str, Callable[[Any], Any]]], *, limit: int = 20) -> list[list[str]]:
    rows: list[list[str]] = []
    for item in queryset[:limit]:
        rows.append([_format_value(getter(item)) for _header, getter in columns])
    return rows


def _catalog_rows(
    queryset,
    columns: list[tuple[str, Callable[[Any], Any]]],
    *,
    slug: str,
    can_manage: bool,
    editing_id: int | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in queryset[:limit]:
        rows.append(
            {
                "id": item.pk,
                "cells": [_format_value(getter(item)) for _header, getter in columns],
                "can_manage": can_manage,
                "edit_url": f"{reverse('catalog-page', kwargs={'slug': slug})}?edit={item.pk}",
                "is_editing": editing_id == item.pk,
            }
        )
    return rows


def _dict_rows(items: list[dict[str, Any]]) -> tuple[list[str], list[list[str]]]:
    if not items:
        return [], []
    headers = list(items[0].keys())
    rows = [[_format_value(item.get(header)) for header in headers] for item in items]
    return headers, rows


def _catalog_queryset_for_request(request: HttpRequest, config: dict[str, Any]):
    queryset = config["queryset"]()
    scoped_roles = set(config.get("scoped_roles", set()))
    if getattr(request.user, "role", None) in scoped_roles:
        queryset = filter_queryset_for_user(request.user, queryset)
    return queryset


def _navigation(request: HttpRequest) -> dict[str, Any]:
    role = getattr(getattr(request, "user", None), "role", None)
    catalog_links = []
    operation_links = []
    if role:
        catalog_links = [
            {"slug": slug, "title": config["title"], "url": reverse("catalog-page", kwargs={"slug": slug})}
            for slug, config in CATALOG_CONFIG.items()
            if role in config["allowed_roles"]
        ]
        operation_links = [
            {"slug": slug, "title": config["title"], "url": reverse("operation-page", kwargs={"slug": slug})}
            for slug, config in OPERATION_CONFIG.items()
            if role in config["allowed_roles"]
        ]
    return {
        "catalog_links": catalog_links,
        "operation_links": operation_links,
        "archive_url": reverse("archive"),
        "reports_url": reverse("reports"),
        "backups_url": reverse("backups"),
        "audit_log_url": reverse("audit-log"),
        "dashboard_url": reverse("dashboard"),
        "can_access_archive": can_access_archive(role),
        "can_access_reports": can_access_reports(role),
        "can_access_backups": can_access_backups(role),
        "can_access_audit_log": can_access_audit_log(role),
    }


def _render(request: HttpRequest, template_name: str, context: dict[str, Any]) -> HttpResponse:
    base_context = _navigation(request)
    base_context.update(context)
    return render(request, template_name, base_context)


def _save_contract(form, request: HttpRequest):
    contract = form.save(commit=False)
    contract.created_by = request.user
    contract.save()
    return contract


def _save_user(form, request: HttpRequest):
    user = form.save(commit=False)
    user.is_staff = user.role in {RoleChoices.ADMIN, RoleChoices.ACCOUNTING, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE, RoleChoices.DIRECTOR}
    user.is_superuser = user.role == RoleChoices.ADMIN
    user.save()
    return user


SAVE_CALLBACKS = {
    "save_contract": _save_contract,
    "save_user": _save_user,
}


def _safe_file_response(path: Path) -> FileResponse:
    if not path.exists():
        raise Http404("Файл не найден.")
    return FileResponse(path.open("rb"), as_attachment=True, filename=path.name)


def _draft_payload_from_form(form) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for name, field in form.fields.items():
        if isinstance(field.widget, django_forms.FileInput):
            continue
        if isinstance(field, (django_forms.MultipleChoiceField, django_forms.ModelMultipleChoiceField)):
            payload[name] = form.data.getlist(name)
        else:
            payload[name] = form.data.get(name, "")
    return payload


def _can_create_in_config(*, request: HttpRequest, config: dict[str, Any]) -> bool:
    if request.user.is_superuser:
        return True
    read_only_roles = set(config.get("read_only_roles", set()))
    return getattr(request.user, "role", None) not in read_only_roles


def _scope_operation_form_for_supplier(*, request: HttpRequest, config: dict[str, Any], form) -> None:
    if getattr(request.user, "role", None) != RoleChoices.SUPPLIER:
        return
    if not config.get("scope_form_for_supplier"):
        return

    supplier_id = getattr(request.user, "supplier_id", None)
    if "supplier" in form.fields:
        form.fields["supplier"].queryset = Supplier.objects.filter(pk=supplier_id) if supplier_id else Supplier.objects.none()
    if "request" in form.fields:
        requests_qs = ProcurementRequest.objects.select_related("supplier", "contract").order_by("-request_date", "-id")
        form.fields["request"].queryset = filter_queryset_for_user(request.user, requests_qs)
    if "supply_contract" in form.fields:
        contracts_qs = SupplyContract.objects.select_related("supplier", "related_smr_contract").order_by("-contract_date", "-id")
        form.fields["supply_contract"].queryset = filter_queryset_for_user(request.user, contracts_qs)


@login_required
def dashboard(request: HttpRequest) -> HttpResponse:
    recent_documents = filter_queryset_for_user(request.user, DocumentRecord.objects.select_related("created_by").order_by("-doc_date", "-id"))
    if request.user.role == RoleChoices.SUPPLIER:
        metrics = {
            "contracts": 0,
            "pending": recent_documents.count(),
            "supplier_docs": recent_documents.filter(entity_type__in=["supplier_document", "primary_document"]).count(),
            "site_tasks": 0,
            "alerts": 0,
        }
        alerts = []
        warehouse_rows = []
        site_rows = []
    elif request.user.role == RoleChoices.ACCOUNTING:
        metrics = dashboard_metrics(user=request.user)
        alerts = []
        warehouse_rows = []
        site_rows = []
    elif request.user.role == RoleChoices.SITE_MANAGER:
        site_name = (request.user.site_name or "").strip()
        metrics = dashboard_metrics(user=request.user)
        alerts = []
        warehouse_rows = []
        site_rows = site_balances(site_name=site_name)[:10] if site_name else []
    else:
        metrics = dashboard_metrics(user=request.user)
        alerts = low_stock_alerts()[:6]
        warehouse_rows = warehouse_balances()[:10]
        site_rows = site_balances()[:10]
    context = {
        "title": "Панель управления",
        "metrics": metrics,
        "alerts": alerts,
        "warehouse_rows": warehouse_rows,
        "site_rows": site_rows,
        "recent_documents": recent_documents[:10],
    }
    return _render(request, "core/dashboard.html", context)


@login_required
def catalog_page(request: HttpRequest, slug: str) -> HttpResponse:
    config = CATALOG_CONFIG.get(slug)
    if not config:
        raise Http404("Справочник не найден.")
    _require_roles(request, set(config["allowed_roles"]))

    can_create = _can_create_in_config(request=request, config=config)
    queryset = _catalog_queryset_for_request(request, config)
    object_id = request.POST.get("object_id") if request.method == "POST" else request.GET.get("edit")
    instance = get_object_or_404(queryset, pk=object_id) if object_id else None
    form = config["form_class"](request.POST or None, instance=instance)
    if request.method == "POST":
        if not can_create:
            raise PermissionDenied("Для вашей роли доступен только просмотр справочника.")
    if request.method == "POST":
        action = request.POST.get("action", "save")
        if action == "delete":
            if instance is None:
                raise Http404("Запись не найдена.")
            if slug == "users" and instance.pk == request.user.pk:
                messages.error(request, "Нельзя удалить текущую учетную запись.")
                return redirect("catalog-page", slug=slug)
            try:
                instance.delete()
                messages.success(request, "Запись удалена.")
            except ProtectedError:
                messages.error(
                    request,
                    "Запись нельзя удалить, потому что она связана с другими данными. Используйте редактирование и деактивацию.",
                )
            return redirect("catalog-page", slug=slug)
    if request.method == "POST" and form.is_valid():
        try:
            callback_name = config.get("save_callback")
            if callback_name:
                SAVE_CALLBACKS[callback_name](form, request)
            else:
                form.save()
            messages.success(request, "Запись обновлена." if instance else "Запись успешно сохранена.")
            return redirect("catalog-page", slug=slug)
        except Exception as exc:
            form.add_error(None, str(exc))

    context = {
        "title": config["title"],
        "description": config["description"],
        "form": form,
        "can_create": can_create,
        "headers": [header for header, _getter in config["columns"]],
        "rows": _catalog_rows(
            queryset,
            config["columns"],
            slug=slug,
            can_manage=can_create,
            editing_id=instance.pk if instance else None,
        ),
        "catalog_has_manage_actions": can_create,
        "is_editing": instance is not None,
        "current_catalog": slug,
    }
    return _render(request, "core/catalogs.html", context)


@login_required
def operation_page(request: HttpRequest, slug: str) -> HttpResponse:
    config = OPERATION_CONFIG.get(slug)
    if not config:
        raise Http404("Операция не найдена.")
    _require_roles(request, set(config["allowed_roles"]))

    can_create = _can_create_in_config(request=request, config=config)
    initial = config.get("initial", lambda _request: {})(request)
    draft_payload = {}
    if request.method == "GET":
        draft_payload = load_operation_draft(user=request.user, operation_slug=slug)
        if draft_payload:
            initial = {**initial, **draft_payload}
    form = config["form_class"](request.POST or None, request.FILES or None, initial=initial)
    _scope_operation_form_for_supplier(request=request, config=config, form=form)
    if request.method == "POST" and not can_create:
        raise PermissionDenied("Для вашей роли доступен только просмотр операции.")
    if request.method == "POST" and form.is_valid():
        try:
            config["handler"](user=request.user, cleaned_data=form.cleaned_data, ip_address=_client_ip(request))
            clear_operation_draft(user=request.user, operation_slug=slug)
            messages.success(request, "Документ успешно создан.")
            return redirect("operation-page", slug=slug)
        except Exception as exc:
            form.add_error(None, str(exc))

    items_field = form.fields.get("items")
    items_mode = ""
    if items_field is not None:
        items_mode = items_field.widget.attrs.get("data-items-mode", "")
    material_catalog = _material_catalog(ppe_only=items_mode == "ppe-lines") if items_mode else []
    worker_catalog = _worker_catalog() if items_mode == "ppe-lines" else []

    queryset = filter_queryset_for_user(request.user, config["queryset"]())
    context = {
        "title": config["title"],
        "description": config["description"],
        "form": form,
        "headers": [header for header, _getter in config["columns"]],
        "rows": _table_rows(queryset, config["columns"]),
        "sample_lines": config.get("sample_lines", ""),
        "current_operation": slug,
        "autosave_url": reverse("operation-draft", kwargs={"slug": slug}),
        "draft_loaded": bool(draft_payload),
        "can_create": can_create,
        "has_items_field": items_field is not None,
        "items_mode": items_mode,
        "material_catalog": material_catalog,
        "worker_catalog": worker_catalog,
    }
    return _render(request, "core/operation.html", context)


@login_required
def operation_draft(request: HttpRequest, slug: str) -> JsonResponse:
    if request.method != "POST":
        return JsonResponse({"ok": False, "detail": "Метод не поддерживается."}, status=405)

    config = OPERATION_CONFIG.get(slug)
    if not config:
        raise Http404("Операция не найдена.")
    _require_roles(request, set(config["allowed_roles"]))
    if not _can_create_in_config(request=request, config=config):
        return JsonResponse({"ok": False, "detail": "Для вашей роли автосохранение недоступно."}, status=403)

    form = config["form_class"](request.POST or None, request.FILES or None)
    _scope_operation_form_for_supplier(request=request, config=config, form=form)
    payload = _draft_payload_from_form(form)
    draft = save_operation_draft(user=request.user, operation_slug=slug, payload=payload)
    return JsonResponse(
        {
            "ok": True,
            "saved": draft is not None,
            "saved_at": timezone.localtime().strftime("%H:%M:%S"),
        }
    )


@login_required
def archive(request: HttpRequest) -> HttpResponse:
    _require_roles(request, ROLE_SET_ARCHIVE)
    can_manage_status = can_update_archive_status(getattr(request.user, "role", None))

    if request.method == "POST":
        if not can_manage_status:
            raise PermissionDenied("Недостаточно прав для изменения статуса.")
        record = get_object_or_404(filter_queryset_for_user(request.user, DocumentRecord.objects.all()), pk=request.POST.get("record_id"))
        new_status = request.POST.get("new_status")
        if new_status not in dict(DocumentStatus.choices):
            raise Http404("Недопустимый статус.")
        try:
            transition_document(user=request.user, record=record, new_status=new_status, ip_address=_client_ip(request))
            messages.success(request, f"Статус документа {record.doc_number} обновлен.")
        except Exception as exc:
            messages.error(request, str(exc))
        return redirect("archive")

    form = ArchiveFilterForm(request.GET or None)
    filters = form.cleaned_data if form.is_valid() else {}
    records = document_records(filters, user=request.user)
    for record in records:
        record.available_status_choices = [(record.status, record.get_status_display())]
        if can_manage_status:
            allowed_statuses = workflow_allowed_statuses(request.user, record)
            for value, label in allowed_statuses:
                if value != record.status:
                    record.available_status_choices.append((value, label))
        record.can_update_status = can_manage_status and len(record.available_status_choices) > 1
    context = {
        "title": "Архив документов",
        "form": form,
        "records": records,
    }
    return _render(request, "core/archive.html", context)


@login_required
def reports(request: HttpRequest) -> HttpResponse:
    _require_roles(request, ROLE_SET_REPORTS)
    form = ReportFilterForm(request.GET or None, initial={"report": "summary"})
    rows: list[dict[str, Any]] = []
    report_name = "summary"
    if form.is_valid():
        report_name = form.cleaned_data["report"]
        rows = REPORT_PROVIDERS[report_name](form.cleaned_data, user=request.user)
    elif not request.GET:
        rows = REPORT_PROVIDERS["summary"]({}, user=request.user)

    headers, table_rows = _dict_rows(rows)
    context = {
        "title": "Отчеты",
        "form": form,
        "report_name": report_name,
        "report_title": REPORT_TITLES[report_name],
        "headers": headers,
        "rows": table_rows,
    }
    return _render(request, "core/reports.html", context)


@login_required
def backups(request: HttpRequest) -> HttpResponse:
    _require_roles(request, ROLE_SET_BACKUP)
    restore_form = BackupRestoreUploadForm()
    if request.method == "POST":
        action = request.POST.get("action", "create")
        try:
            if action == "create":
                path = write_backup_file(user=request.user)
                messages.success(request, f"Резервная копия создана: {path.name}")
                return redirect("backups")
            if action == "restore-existing":
                backup_name = request.POST.get("backup_name", "")
                backup_path = (settings.BACKUPS_DIR / backup_name).resolve()
                if backup_path.parent != settings.BACKUPS_DIR.resolve() or not backup_path.exists():
                    raise Http404("Файл не найден.")
                restored_counts = restore_backup_file(backup_path, user=request.user, ip_address=_client_ip(request))
                messages.success(request, f"Данные восстановлены из {backup_name}. Записей: {sum(restored_counts.values())}.")
                return redirect("backups")
            if action == "restore-upload":
                restore_form = BackupRestoreUploadForm(request.POST, request.FILES)
                if restore_form.is_valid():
                    uploaded = restore_form.cleaned_data["backup_file"]
                    payload = json.loads(uploaded.read().decode("utf-8"))
                    restored_counts = restore_backup_payload(
                        payload=payload,
                        user=request.user,
                        ip_address=_client_ip(request),
                        source_name=uploaded.name,
                    )
                    messages.success(request, f"Данные восстановлены из загруженного файла {uploaded.name}. Записей: {sum(restored_counts.values())}.")
                    return redirect("backups")
            else:
                raise Http404("Неизвестное действие.")
        except Exception as exc:
            if action == "restore-upload":
                restore_form.add_error(None, str(exc))
            else:
                messages.error(request, str(exc))
    context = {
        "title": "Резервные копии",
        "items": backup_files(),
        "restore_form": restore_form,
    }
    return _render(request, "core/backups.html", context)


@login_required
def audit_log(request: HttpRequest) -> HttpResponse:
    _require_roles(request, ROLE_SET_AUDIT_LOG)
    form = AuditLogFilterForm(request.GET or None)
    entries = AuditLog.objects.select_related("user").order_by("-created_at", "-id")

    if form.is_valid():
        filters = form.cleaned_data
        if filters.get("username"):
            entries = entries.filter(user__username__icontains=filters["username"])
        if filters.get("action"):
            entries = entries.filter(action__icontains=filters["action"])
        if filters.get("entity_type"):
            entries = entries.filter(entity_type__icontains=filters["entity_type"])
        if filters.get("date_from"):
            entries = entries.filter(created_at__date__gte=filters["date_from"])
        if filters.get("date_to"):
            entries = entries.filter(created_at__date__lte=filters["date_to"])
        if filters.get("query"):
            query = filters["query"]
            entries = entries.filter(
                Q(details__icontains=query)
                | Q(action__icontains=query)
                | Q(entity_type__icontains=query)
                | Q(user__username__icontains=query)
            )

    context = {
        "title": "Журнал действий",
        "form": form,
        "entries": entries[:100],
    }
    return _render(request, "core/audit_log.html", context)


@login_required
def export_document(request: HttpRequest, entity_type: str, entity_id: int) -> FileResponse:
    get_object_or_404(filter_queryset_for_user(request.user, DocumentRecord.objects.all()), entity_type=entity_type, entity_id=entity_id)
    exporter = Exporter()
    path = exporter.export_document(entity_type, entity_id)
    return _safe_file_response(path)


@login_required
def export_report(request: HttpRequest) -> FileResponse:
    _require_roles(request, ROLE_SET_REPORTS)
    form = ReportFilterForm(request.GET or None)
    if not form.is_valid():
        raise Http404("Некорректные параметры отчета.")
    exporter = Exporter()
    path = exporter.export_report(form.cleaned_data["report"], form.cleaned_data, user=request.user)
    return _safe_file_response(path)


@login_required
def download_backup(request: HttpRequest, backup_name: str) -> FileResponse:
    _require_roles(request, ROLE_SET_BACKUP)
    backup_path = (settings.BACKUPS_DIR / backup_name).resolve()
    if backup_path.parent != settings.BACKUPS_DIR.resolve():
        raise Http404("Файл не найден.")
    return _safe_file_response(backup_path)
