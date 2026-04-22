from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.color import no_style
from django.db import connection, transaction
from django.db.models import DecimalField, Q, Sum, Value
from django.db.models.functions import Coalesce
from django.utils import timezone

from .access import ACCOUNTING_VISIBLE_STATUSES, ROLE_SET_OFFICE
from .models import (
    AuditLog,
    ConstructionObject,
    DocumentRecord,
    DocumentStatus,
    DocumentType,
    FormDraft,
    Material,
    MaterialNorm,
    PPEIssuance,
    PPEIssuanceLine,
    PrimaryDocument,
    PrimaryDocumentLine,
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
    Worker,
    WorkLog,
    WriteOffAct,
    WriteOffLine,
)
from .models import RoleChoices


def today() -> date:
    return timezone.localdate()


def decimalize(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value in (None, ""):
        return default
    return Decimal(str(value).replace(",", "."))


def _user_site_name(user) -> str:
    return (getattr(user, "site_name", "") or "").strip()


def _site_name_variants(value: str | None) -> set[str]:
    raw_value = (value or "").strip()
    if not raw_value:
        return set()

    variants: set[str] = set()
    pending = [raw_value]
    while pending and len(variants) < 8:
        current = pending.pop()
        normalized = current.casefold()
        if normalized in variants:
            continue
        variants.add(normalized)
        for source_encoding, target_encoding in (("cp1251", "utf-8"), ("utf-8", "cp1251")):
            try:
                candidate = current.encode(source_encoding).decode(target_encoding).strip()
            except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
                continue
            if candidate and candidate.casefold() not in variants:
                pending.append(candidate)
    return variants


def _scoped_site_name(*, user, site_name: str | None = None, fallback: str = "") -> str:
    resolved_site = (site_name or "").strip()
    if getattr(user, "role", None) != RoleChoices.SITE_MANAGER:
        return resolved_site or fallback

    user_site = _user_site_name(user)
    if not user_site:
        raise ValueError("За начальником участка не закреплен контур участка.")
    if resolved_site and _site_name_variants(resolved_site).isdisjoint(_site_name_variants(user_site)):
        raise ValueError("Начальник участка может работать только в рамках своего участка.")
    return user_site


def generate_number(prefix: str) -> str:
    date_part = today().strftime("%Y%m%d")
    pattern = f"{prefix}-{date_part}-"
    count = DocumentRecord.objects.filter(doc_number__startswith=pattern).count()
    return f"{prefix}-{date_part}-{count + 1:03d}"


STATUS_LABELS = dict(DocumentStatus.choices)
ROLE_LABELS = dict(RoleChoices.choices)
WORKFLOW_ENTRY_STATUSES = (DocumentStatus.DRAFT, DocumentStatus.APPROVAL)
WORKFLOW_ACCOUNTING_ROLES = {RoleChoices.ADMIN, RoleChoices.ACCOUNTING}
SUPPLIER_CONFIRM_ROLES = {RoleChoices.SUPPLIER}
WORKFLOW_ROLE_ORDER = [
    RoleChoices.ADMIN,
    RoleChoices.DIRECTOR,
    RoleChoices.PROCUREMENT,
    RoleChoices.WAREHOUSE,
    RoleChoices.SITE_MANAGER,
    RoleChoices.SUPPLIER,
    RoleChoices.ACCOUNTING,
]
DEFAULT_WORKFLOW_ROUTE = {
    "creators": {
        RoleChoices.ADMIN,
        RoleChoices.DIRECTOR,
        RoleChoices.PROCUREMENT,
        RoleChoices.WAREHOUSE,
        RoleChoices.SITE_MANAGER,
    },
    "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
    "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
    "viewers": {RoleChoices.ACCOUNTING},
}
WORKFLOW_ROUTE_MAP: dict[str, dict[str, set[str]]] = {
    "smr_contract": {
        "creators": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.ACCOUNTING},
    },
    "supply_contract": {
        "creators": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.SUPPLIER, RoleChoices.ACCOUNTING},
    },
    "procurement_request": {
        "creators": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SITE_MANAGER},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.SUPPLIER, RoleChoices.ACCOUNTING},
    },
    "primary_document": {
        "creators": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.ACCOUNTING},
    },
    "supplier_document": {
        "creators": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SUPPLIER},
        "reviewers": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.ACCOUNTING},
    },
    "stock_receipt": {
        "creators": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.SITE_MANAGER, RoleChoices.ACCOUNTING},
    },
    "stock_issue": {
        "creators": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.SITE_MANAGER, RoleChoices.ACCOUNTING},
    },
    "write_off": {
        "creators": {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.ACCOUNTING},
    },
    "ppe_issuance": {
        "creators": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.ACCOUNTING},
    },
}


def _ordered_roles(roles: set[str]) -> list[str]:
    prioritized = [role for role in WORKFLOW_ROLE_ORDER if role in roles]
    extra = sorted(role for role in roles if role not in WORKFLOW_ROLE_ORDER)
    return prioritized + extra


def _join_role_labels(roles: set[str]) -> str:
    if not roles:
        return "-"
    return ", ".join(ROLE_LABELS.get(role, role) for role in _ordered_roles(roles))


def workflow_route_metadata(entity_type: str) -> dict[str, str]:
    route = WORKFLOW_ROUTE_MAP.get(entity_type, DEFAULT_WORKFLOW_ROUTE)
    creators = _join_role_labels(route.get("creators", set()))
    approvers = _join_role_labels(route.get("approvers", set()))
    senders = _join_role_labels(route.get("senders", set()))
    viewers = _join_role_labels(route.get("viewers", set()))
    return {
        "workflow_created_by": creators,
        "workflow_approved_by": approvers,
        "workflow_sent_accounting_by": senders,
        "workflow_view_only": viewers,
        "workflow_route": f"{creators} -> {approvers} -> {senders} -> {viewers}",
    }


def _build_default_workflow_transitions(route: dict[str, set[str]]) -> dict[str, dict[str, set[str]]]:
    creators = route.get("creators", set())
    approvers = route.get("approvers", set())
    senders = route.get("senders", set())
    return {
        DocumentStatus.DRAFT: {
            DocumentStatus.APPROVAL: creators,
        },
        DocumentStatus.APPROVAL: {
            DocumentStatus.APPROVED: approvers,
            DocumentStatus.REWORK: approvers,
        },
        DocumentStatus.APPROVED: {
            DocumentStatus.SENT_ACCOUNTING: senders,
        },
        DocumentStatus.SENT_ACCOUNTING: {
            DocumentStatus.ACCEPTED: WORKFLOW_ACCOUNTING_ROLES,
            DocumentStatus.REWORK: WORKFLOW_ACCOUNTING_ROLES,
        },
        DocumentStatus.REWORK: {
            DocumentStatus.APPROVAL: creators,
        },
    }


def _build_supplier_document_transitions(route: dict[str, set[str]]) -> dict[str, dict[str, set[str]]]:
    reviewers = route.get("reviewers", route.get("approvers", set()))
    approvers = route.get("approvers", set())
    senders = route.get("senders", set())
    return {
        DocumentStatus.UPLOADED: {
            DocumentStatus.SUPPLY_CONFIRMED: SUPPLIER_CONFIRM_ROLES,
            DocumentStatus.APPROVAL: reviewers,
            DocumentStatus.REWORK: reviewers,
        },
        DocumentStatus.SUPPLY_CONFIRMED: {
            DocumentStatus.APPROVAL: reviewers,
            DocumentStatus.REWORK: reviewers,
        },
        DocumentStatus.APPROVAL: {
            DocumentStatus.APPROVED: approvers,
            DocumentStatus.REWORK: approvers,
        },
        DocumentStatus.APPROVED: {
            DocumentStatus.SENT_ACCOUNTING: senders,
        },
        DocumentStatus.SENT_ACCOUNTING: {
            DocumentStatus.ACCEPTED: WORKFLOW_ACCOUNTING_ROLES,
            DocumentStatus.REWORK: WORKFLOW_ACCOUNTING_ROLES,
        },
        DocumentStatus.REWORK: {
            DocumentStatus.SUPPLY_CONFIRMED: SUPPLIER_CONFIRM_ROLES,
            DocumentStatus.APPROVAL: reviewers,
        },
    }


DEFAULT_WORKFLOW_TRANSITIONS = _build_default_workflow_transitions(DEFAULT_WORKFLOW_ROUTE)
ENTITY_WORKFLOW_TRANSITIONS: dict[str, dict[str, dict[str, set[str]]]] = {}
for _entity_type, _route in WORKFLOW_ROUTE_MAP.items():
    if _entity_type == "supplier_document":
        ENTITY_WORKFLOW_TRANSITIONS[_entity_type] = _build_supplier_document_transitions(_route)
    else:
        ENTITY_WORKFLOW_TRANSITIONS[_entity_type] = _build_default_workflow_transitions(_route)
SUPPLIER_DOCUMENT_TRANSITIONS = ENTITY_WORKFLOW_TRANSITIONS["supplier_document"]


def validate_initial_document_status(status: str) -> str:
    if status not in WORKFLOW_ENTRY_STATUSES:
        allowed_labels = ", ".join(STATUS_LABELS[item] for item in WORKFLOW_ENTRY_STATUSES)
        raise ValueError(f"На этапе создания доступны только статусы: {allowed_labels}.")
    return status


def _workflow_transitions(entity_type: str) -> dict[str, dict[str, set[str]]]:
    return ENTITY_WORKFLOW_TRANSITIONS.get(entity_type, DEFAULT_WORKFLOW_TRANSITIONS)


def _supports_accounting_handoff(entity_type: str) -> bool:
    sent_accounting_rules = _workflow_transitions(entity_type).get(DocumentStatus.SENT_ACCOUNTING, {})
    return bool(sent_accounting_rules.get(DocumentStatus.ACCEPTED) and sent_accounting_rules.get(DocumentStatus.REWORK))


def _automatic_transition_path(user_role: str | None, entity_type: str, current_status: str, new_status: str) -> list[str]:
    if not _supports_accounting_handoff(entity_type):
        return []
    if user_role in WORKFLOW_ACCOUNTING_ROLES and current_status == DocumentStatus.APPROVED and new_status in {DocumentStatus.ACCEPTED, DocumentStatus.REWORK}:
        return [DocumentStatus.SENT_ACCOUNTING, new_status]
    return []


def workflow_allowed_statuses(user, record: DocumentRecord) -> list[tuple[str, str]]:
    user_role = getattr(user, "role", None)
    if not user_role:
        return []

    allowed: list[tuple[str, str]] = []
    seen: set[str] = set()
    for status, roles in _workflow_transitions(record.entity_type).get(record.status, {}).items():
        if user_role in roles and status not in seen:
            allowed.append((status, STATUS_LABELS.get(status, status)))
            seen.add(status)

    for target_status in (DocumentStatus.ACCEPTED, DocumentStatus.REWORK):
        path = _automatic_transition_path(user_role, record.entity_type, record.status, target_status)
        if path and target_status not in seen:
            allowed.append((target_status, STATUS_LABELS[target_status]))
            seen.add(target_status)

    return allowed


def _resolve_transition_path(*, user_role: str | None, entity_type: str, current_status: str, new_status: str) -> list[str]:
    if new_status == current_status:
        return [current_status]

    direct_rules = _workflow_transitions(entity_type).get(current_status, {})
    allowed_roles = direct_rules.get(new_status)
    if allowed_roles and user_role in allowed_roles:
        return [new_status]

    automatic_path = _automatic_transition_path(user_role, entity_type, current_status, new_status)
    if automatic_path:
        return automatic_path

    current_label = STATUS_LABELS.get(current_status, current_status)
    target_label = STATUS_LABELS.get(new_status, new_status)
    raise ValueError(f"Переход из статуса '{current_label}' в '{target_label}' для вашей роли недоступен.")


def filter_queryset_for_user(user, queryset):
    role = getattr(user, "role", None)
    if not role:
        return queryset.none()

    if role in ROLE_SET_OFFICE:
        return queryset

    model = queryset.model

    if role == RoleChoices.ACCOUNTING:
        if model is DocumentRecord:
            return queryset.filter(status__in=ACCOUNTING_VISIBLE_STATUSES)
        return queryset.none()

    if role == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        if model is SMRContract:
            filters = Q(created_by=user)
            if site_name:
                filters |= (
                    Q(procurement_requests__site_name__iexact=site_name)
                    | Q(stock_issues__site_name__iexact=site_name)
                    | Q(write_off_acts__site_name__iexact=site_name)
                    | Q(work_logs__site_name__iexact=site_name)
                )
            return queryset.filter(filters).distinct()
        if model is ProcurementRequest:
            filters = Q(requested_by=user)
            if site_name:
                filters |= Q(site_name__iexact=site_name)
            return queryset.filter(filters).distinct()
        if model is SupplierDocument:
            filters = Q(uploaded_by=user)
            if site_name:
                filters |= Q(request__site_name__iexact=site_name)
            return queryset.filter(filters).distinct()
        if model is PrimaryDocument:
            filters = Q(created_by=user)
            if site_name:
                filters |= Q(site_name__iexact=site_name) | Q(procurement_request__site_name__iexact=site_name)
            return queryset.filter(filters).distinct()
        if model is DocumentRecord:
            filters = Q(created_by=user)
            if site_name:
                filters |= Q(metadata_json__site_name__iexact=site_name) | Q(object_name__iexact=site_name)
            return queryset.filter(filters).distinct()
        if model in {StockIssue, WriteOffAct, PPEIssuance, WorkLog}:
            if not site_name:
                return queryset.none()
            return queryset.filter(site_name__iexact=site_name)
        return queryset.none()

    if role == RoleChoices.SUPPLIER:
        if not getattr(user, "supplier_id", None):
            return queryset.none()
        if model is SupplyContract:
            return queryset.filter(supplier=user.supplier)
        if model is ProcurementRequest:
            return queryset.filter(supplier=user.supplier)
        if model is SupplierDocument:
            return queryset.filter(supplier=user.supplier)
        if model is PrimaryDocument:
            return queryset.filter(supplier=user.supplier)
        if model is DocumentRecord:
            return queryset.filter(
                (Q(metadata_json__supplier_id=user.supplier_id) | Q(counterparty__iexact=user.supplier.name))
                & Q(entity_type__in=["procurement_request", "supplier_document", "supply_contract", "primary_document"])
            )
        return queryset.none()

    return queryset.none()


def _structured_rows(raw_text: str) -> list[dict[str, Any]] | None:
    payload = (raw_text or "").strip()
    if not payload or not payload.startswith("["):
        return None

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("Некорректный структурированный список позиций.") from exc

    if not isinstance(data, list):
        raise ValueError("Структурированный список позиций должен быть списком.")

    rows: list[dict[str, Any]] = []
    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Structured row #{index} has invalid format.")
        rows.append(item)
    return rows


def parse_line_items(raw_text: str, *, require_price: bool = False) -> list[dict[str, Any]]:
    structured_rows = _structured_rows(raw_text)
    lines: list[dict[str, Any]] = []

    if structured_rows is not None:
        for index, item in enumerate(structured_rows, start=1):
            material_code = str(item.get("material_code", "")).strip()
            quantity = decimalize(item.get("quantity"))
            unit_price_raw = item.get("unit_price")
            unit_price = decimalize(unit_price_raw) if unit_price_raw not in (None, "") else Decimal("0")
            notes = str(item.get("notes", "") or "").strip()
            line_ref = f"строка #{index}"
            if not material_code:
                raise ValueError(f"Код материала обязателен для {line_ref}.")
            if quantity <= 0:
                raise ValueError(f"Количество должно быть больше нуля для {line_ref}.")
            if require_price and unit_price <= 0:
                raise ValueError(f"Цена обязательна для {line_ref}.")
            lines.append(
                {
                    "material_code": material_code,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "notes": notes,
                }
            )
    else:
        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 2:
                raise ValueError(f"Некорректный формат строки: {line}")
            item = {
                "material_code": parts[0],
                "quantity": decimalize(parts[1]),
                "unit_price": decimalize(parts[2]) if len(parts) >= 3 and parts[2] else Decimal("0"),
                "notes": parts[3] if len(parts) >= 4 else "",
            }
            if not item["material_code"]:
                raise ValueError(f"Код материала обязателен в строке: {line}")
            if item["quantity"] <= 0:
                raise ValueError(f"Количество должно быть больше нуля в строке: {line}")
            if require_price and item["unit_price"] <= 0:
                raise ValueError(f"Цена обязательна в строке: {line}")
            lines.append(item)

    if not lines:
        raise ValueError("Не указаны позиции документа.")
    return lines


def parse_ppe_lines(raw_text: str) -> list[dict[str, Any]]:
    structured_rows = _structured_rows(raw_text)
    lines: list[dict[str, Any]] = []

    if structured_rows is not None:
        for index, item in enumerate(structured_rows, start=1):
            employee_number = str(item.get("employee_number", "")).strip()
            material_code = str(item.get("material_code", "")).strip()
            quantity = decimalize(item.get("quantity"))
            service_life_months = int(decimalize(item.get("service_life_months")))
            line_ref = f"строка #{index}"
            if not employee_number:
                raise ValueError(f"Табельный номер обязателен для {line_ref}.")
            if not material_code:
                raise ValueError(f"Код материала обязателен для {line_ref}.")
            if quantity <= 0:
                raise ValueError(f"Количество СИЗ должно быть больше нуля для {line_ref}.")
            if service_life_months <= 0:
                raise ValueError(f"Срок службы СИЗ должен быть больше нуля для {line_ref}.")
            lines.append(
                {
                    "employee_number": employee_number,
                    "material_code": material_code,
                    "quantity": quantity,
                    "service_life_months": service_life_months,
                }
            )
    else:
        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 4:
                raise ValueError("Формат строки СИЗ: табельный номер | код материала | количество | срок службы.")
            item = {
                "employee_number": parts[0],
                "material_code": parts[1],
                "quantity": decimalize(parts[2]),
                "service_life_months": int(decimalize(parts[3])),
            }
            if not item["employee_number"]:
                raise ValueError(f"Табельный номер обязателен в строке: {line}")
            if not item["material_code"]:
                raise ValueError(f"Код материала обязателен в строке: {line}")
            if item["quantity"] <= 0:
                raise ValueError(f"Количество СИЗ должно быть больше нуля в строке: {line}")
            if item["service_life_months"] <= 0:
                raise ValueError(f"Срок службы СИЗ должен быть больше нуля в строке: {line}")
            lines.append(item)

    if not lines:
        raise ValueError("Не указаны позиции выдачи спецодежды.")
    return lines


def audit(user, action: str, entity_type: str, entity_id: int | None = None, details: str = "", ip_address: str | None = None) -> None:
    AuditLog.objects.create(user=user, action=action, entity_type=entity_type, entity_id=entity_id, details=details, ip_address=ip_address)


def sync_document_record(
    *,
    entity_type: str,
    entity_id: int,
    doc_type: str,
    doc_number: str,
    doc_date: date,
    status: str,
    title: str,
    counterparty: str = "",
    object_name: str = "",
    created_by=None,
    file_path: str = "",
    metadata: dict[str, Any] | None = None,
    search_text: str = "",
) -> DocumentRecord:
    merged_metadata = {**workflow_route_metadata(entity_type), **(metadata or {})}
    if created_by is not None and getattr(created_by, "role", None):
        created_role = created_by.role
        merged_metadata.setdefault("workflow_created_role", created_role)
        merged_metadata.setdefault("workflow_created_role_label", ROLE_LABELS.get(created_role, created_role))
    return DocumentRecord.objects.update_or_create(
        entity_type=entity_type,
        entity_id=entity_id,
        defaults={
            "doc_type": doc_type,
            "doc_number": doc_number,
            "doc_date": doc_date,
            "status": status,
            "title": title,
            "counterparty": counterparty,
            "object_name": object_name,
            "created_by": created_by,
            "file_path": file_path,
            "metadata_json": merged_metadata,
            "search_text": " ".join(part for part in [doc_type, doc_number, title, counterparty, object_name, search_text] if part),
        },
    )[0]


def _get_material_or_raise(code: str) -> Material:
    material = Material.objects.filter(code=code).first()
    if not material:
        raise ValueError(f"Материал с кодом {code} не найден.")
    return material


def stock_balance(material: Material, location_name: str) -> Decimal:
    aggregate = StockMovement.objects.filter(material=material, location_name__iexact=location_name).aggregate(
        total=Coalesce(
            Sum("quantity_delta"),
            Value(Decimal("0"), output_field=DecimalField(max_digits=14, decimal_places=3)),
        )
    )
    return aggregate["total"] or Decimal("0")


def _ensure_available_stock(*, material: Material, location_name: str, required_quantity: Decimal, reason: str) -> None:
    available_quantity = stock_balance(material, location_name)
    if available_quantity < required_quantity:
        raise ValueError(
            f"Недостаточно остатка по материалу {material.code} на локации «{location_name}». "
            f"Доступно: {available_quantity}, требуется: {required_quantity}. Операция: {reason}."
        )


def _ensure_supplier_access(*, user, supplier: Supplier) -> None:
    if getattr(user, "role", None) == RoleChoices.SUPPLIER and getattr(user, "supplier_id", None) != supplier.pk:
        raise ValueError("Пользователь-поставщик может работать только со своей организацией.")


def _validate_supplier_consistency(*, supplier: Supplier, related_suppliers: list[Supplier]) -> None:
    if any(related_supplier.pk != supplier.pk for related_supplier in related_suppliers):
        raise ValueError("Поставщик не соответствует связанным документам.")


def _supplier_document_type(cleaned_data: dict[str, Any]) -> DocumentType | None:


    doc_type_name = (cleaned_data.get("doc_type") or "").strip()
    if not doc_type_name:
        return None
    document_type = DocumentType.objects.filter(name=doc_type_name, is_active=True).first()
    if document_type and not document_type.available_for_upload:
        raise ValueError("Выбранный тип документа недоступен для загрузки поставщиком.")
    return document_type


def _primary_document_line_items(cleaned_data: dict[str, Any], *, document_type: DocumentType) -> list[dict[str, Any]]:
    raw_items = (cleaned_data.get("items") or "").strip()
    if raw_items:
        return parse_line_items(raw_items)

    request = cleaned_data.get("request")
    if request:
        return [
            {
                "material_code": line.material.code,
                "quantity": line.quantity,
                "unit_price": line.unit_price,
                "notes": line.notes,
            }
            for line in request.lines.select_related("material")
        ]

    stock_receipt = cleaned_data.get("stock_receipt")
    if stock_receipt:
        return [
            {
                "material_code": line.material.code,
                "quantity": line.quantity,
                "unit_price": line.unit_price,
                "notes": line.notes,
            }
            for line in stock_receipt.lines.select_related("material")
        ]

    if document_type.requires_items:
        raise ValueError("Для выбранного документа нужно заполнить позиции или привязать заявку/приход.")
    return []


def _primary_document_supplier(*, cleaned_data: dict[str, Any], user) -> Supplier:
    request = cleaned_data.get("request")
    supply_contract = cleaned_data.get("supply_contract")
    stock_receipt = cleaned_data.get("stock_receipt")
    supplier = (
        cleaned_data.get("supplier")
        or (request.supplier if request else None)
        or (supply_contract.supplier if supply_contract else None)
        or (stock_receipt.supplier if stock_receipt else None)
        or getattr(user, "supplier", None)
    )
    if not supplier:
        raise ValueError("Не удалось определить поставщика для первичного документа.")

    related_suppliers = [
        related_supplier
        for related_supplier in [
            request.supplier if request and request.supplier_id else None,
            supply_contract.supplier if supply_contract and supply_contract.supplier_id else None,
            stock_receipt.supplier if stock_receipt and stock_receipt.supplier_id else None,
        ]
        if related_supplier is not None
    ]
    if any(related_supplier.pk != supplier.pk for related_supplier in related_suppliers):
        raise ValueError("Поставщик документа не совпадает с поставщиком в связанных документах.")
    return supplier


def _primary_document_site_name(*, cleaned_data: dict[str, Any], user) -> str:
    request = cleaned_data.get("request")
    stock_receipt = cleaned_data.get("stock_receipt")
    explicit_site = (cleaned_data.get("site_name") or "").strip()
    return explicit_site or (request.site_name if request else "") or (settings.WAREHOUSE_NAME if stock_receipt else "") or getattr(user, "site_name", "")


def _primary_document_basis_reference(cleaned_data: dict[str, Any]) -> str:
    request = cleaned_data.get("request")
    supply_contract = cleaned_data.get("supply_contract")
    stock_receipt = cleaned_data.get("stock_receipt")
    if request:
        return f"Заявка {request.number}"
    if stock_receipt:
        return f"Приход {stock_receipt.number}"
    if supply_contract:
        return f"Договор поставки {supply_contract.number}"
    return ""


@transaction.atomic
def create_procurement_request(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> ProcurementRequest:
    cleaned_data = {
        **cleaned_data,
        "site_name": _scoped_site_name(
            user=user,
            site_name=cleaned_data.get("site_name"),
            fallback=getattr(user, "site_name", "") or "Участок",
        ),
    }
    request = ProcurementRequest.objects.create(
        number=generate_number("REQ"),
        request_date=cleaned_data["request_date"],
        site_name=cleaned_data["site_name"] or getattr(user, "site_name", "") or "Участок",
        contract=cleaned_data.get("contract"),
        supplier=cleaned_data.get("supplier"),
        requested_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for item in parse_line_items(cleaned_data["items"]):
        material = _get_material_or_raise(item["material_code"])
        ProcurementRequestLine.objects.create(
            request=request,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"],
            notes=item["notes"],
        )
    audit(user, "create", "procurement_request", request.id, f"Создана заявка {request.number}", ip_address)
    return request


@transaction.atomic
def create_supplier_document(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> SupplierDocument:
    request = cleaned_data.get("request")
    supply_contract = cleaned_data.get("supply_contract")
    document_type = _supplier_document_type(cleaned_data)
    supplier = (
        cleaned_data.get("supplier")
        or getattr(user, "supplier", None)
        or (request.supplier if request else None)
        or (supply_contract.supplier if supply_contract else None)
    )
    if not supplier:
        raise ValueError("Unable to resolve supplier. Select a supplier or link the user to one.")

    _ensure_supplier_access(user=user, supplier=supplier)
    related_suppliers = [
        related_supplier
        for related_supplier in [
            request.supplier if request and request.supplier_id else None,
            supply_contract.supplier if supply_contract and supply_contract.supplier_id else None,
        ]
        if related_supplier is not None
    ]
    _validate_supplier_consistency(supplier=supplier, related_suppliers=related_suppliers)

    document = SupplierDocument.objects.create(
        supplier=supplier,
        request=request,
        supply_contract=supply_contract,
        doc_type=cleaned_data["doc_type"],
        doc_number=cleaned_data.get("doc_number") or generate_number(document_type.prefix if document_type else "SUPDOC"),
        doc_date=cleaned_data["doc_date"],
        amount=cleaned_data.get("amount") or Decimal("0"),
        vat_amount=cleaned_data.get("vat_amount") or Decimal("0"),
        uploaded_by=user,
        attachment=cleaned_data.get("attachment"),
        status=DocumentStatus.UPLOADED,
        notes=cleaned_data.get("notes", ""),
    )
    audit(user, "upload", "supplier_document", document.id, f"Загружен документ поставщика {document.doc_number}", ip_address)
    return document


@transaction.atomic
def create_primary_document(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> PrimaryDocument:

    document_type = cleaned_data["document_type"]
    if not document_type.is_active or not document_type.available_for_generation:
        raise ValueError("Выбранный тип документа недоступен для генерации.")

    line_items = _primary_document_line_items(cleaned_data, document_type=document_type)
    calculated_amount = sum((item["quantity"] * item["unit_price"] for item in line_items), Decimal("0"))
    document = PrimaryDocument.objects.create(
        document_type=document_type,
        number=generate_number(document_type.prefix),
        doc_date=cleaned_data["doc_date"],
        supplier=_primary_document_supplier(cleaned_data=cleaned_data, user=user),
        procurement_request=cleaned_data.get("request"),
        supply_contract=cleaned_data.get("supply_contract"),
        stock_receipt=cleaned_data.get("stock_receipt"),
        site_name=_primary_document_site_name(cleaned_data=cleaned_data, user=user),
        basis_reference=_primary_document_basis_reference(cleaned_data),
        amount=cleaned_data.get("amount") or calculated_amount,
        vat_amount=cleaned_data.get("vat_amount") or Decimal("0"),
        created_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        PrimaryDocumentLine.objects.create(
            document=document,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"] or material.price,
            notes=item["notes"],
        )
    audit(user, "create", "primary_document", document.id, f"Создан документ {document.document_type.name} {document.number}", ip_address)
    return document


@transaction.atomic
def create_stock_receipt(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> StockReceipt:
    line_items = parse_line_items(cleaned_data["items"])
    supplier = cleaned_data["supplier"]
    supplier_document = cleaned_data.get("supplier_document")
    if supplier_document and supplier_document.supplier_id != supplier.id:
        raise ValueError("Документ поставщика должен принадлежать выбранному поставщику.")

    receipt = StockReceipt.objects.create(
        number=generate_number("REC"),
        receipt_date=cleaned_data["receipt_date"],
        supplier=supplier,
        supplier_document=supplier_document,
        created_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        unit_price = item["unit_price"] or material.price
        StockReceiptLine.objects.create(receipt=receipt, material=material, quantity=item["quantity"], unit_price=unit_price, notes=item["notes"])
        StockMovement.objects.create(
            movement_date=receipt.receipt_date,
            material=material,
            quantity_delta=item["quantity"],
            location_name=settings.WAREHOUSE_NAME,
            source_type="stock_receipt",
            source_id=receipt.id,
            unit_price=unit_price,
            created_by=user,
            notes=receipt.notes,
        )
    audit(user, "create", "stock_receipt", receipt.id, f"Создан приходный ордер {receipt.number}", ip_address)
    return receipt


@transaction.atomic
def create_stock_issue(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> StockIssue:
    line_items = parse_line_items(cleaned_data["items"])
    resolved_items: list[tuple[Material, dict[str, Any], Decimal]] = []
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        _ensure_available_stock(
            material=material,
            location_name=settings.WAREHOUSE_NAME,
            required_quantity=item["quantity"],
            reason="отпуск материалов",
        )
        resolved_items.append((material, item, item["unit_price"] or material.price))

    issue = StockIssue.objects.create(
        number=generate_number("ISS"),
        issue_date=cleaned_data["issue_date"],
        site_name=cleaned_data["site_name"],
        contract=cleaned_data.get("contract"),
        issued_by=user,
        received_by_name=cleaned_data["received_by_name"],
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for material, item, unit_price in resolved_items:
        StockIssueLine.objects.create(issue=issue, material=material, quantity=item["quantity"], unit_price=unit_price, notes=item["notes"])
        StockMovement.objects.create(
            movement_date=issue.issue_date,
            material=material,
            quantity_delta=-item["quantity"],
            location_name=settings.WAREHOUSE_NAME,
            source_type="stock_issue",
            source_id=issue.id,
            unit_price=unit_price,
            created_by=user,
            notes=issue.notes,
        )
        StockMovement.objects.create(
            movement_date=issue.issue_date,
            material=material,
            quantity_delta=item["quantity"],
            location_name=issue.site_name,
            source_type="stock_issue",
            source_id=issue.id,
            unit_price=unit_price,
            created_by=user,
            notes=issue.notes,
        )
    audit(user, "create", "stock_issue", issue.id, f"Создан отпуск материалов {issue.number}", ip_address)
    return issue


@transaction.atomic
def create_writeoff(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> WriteOffAct:
    cleaned_data = {**cleaned_data, "site_name": _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))}
    work_volume = cleaned_data["work_volume"]
    if work_volume <= 0:
        raise ValueError("Объем работ должен быть больше нуля.")

    norms = list(MaterialNorm.objects.select_related("material").filter(work_type=cleaned_data["work_type"]).order_by("material__code"))
    if not norms:
        raise ValueError("Для выбранного вида работ не настроены нормы расхода материалов.")

    prepared_lines: list[tuple[MaterialNorm, Decimal]] = []
    for norm in norms:
        quantity = (work_volume * norm.norm_per_unit).quantize(Decimal("0.001"))
        _ensure_available_stock(
            material=norm.material,
            location_name=cleaned_data["site_name"],
            required_quantity=quantity,
            reason="списание материалов",
        )
        prepared_lines.append((norm, quantity))

    act = WriteOffAct.objects.create(
        number=generate_number("WO"),
        act_date=cleaned_data["act_date"],
        contract=cleaned_data["contract"],
        site_name=cleaned_data["site_name"],
        work_type=cleaned_data["work_type"],
        work_volume=work_volume,
        volume_unit=cleaned_data.get("volume_unit", ""),
        created_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for norm, quantity in prepared_lines:
        WriteOffLine.objects.create(
            act=act,
            material=norm.material,
            norm_per_unit=norm.norm_per_unit,
            calculated_quantity=quantity,
            actual_quantity=quantity,
            unit_price=norm.material.price,
            notes=norm.notes,
        )
        StockMovement.objects.create(
            movement_date=act.act_date,
            material=norm.material,
            quantity_delta=-quantity,
            location_name=act.site_name,
            source_type="write_off",
            source_id=act.id,
            unit_price=norm.material.price,
            created_by=user,
            notes=f"Списание по акту: {act.work_type}",
        )
    audit(user, "create", "write_off", act.id, f"Создан акт списания {act.number}", ip_address)
    return act


@transaction.atomic
def create_ppe_issuance(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> PPEIssuance:
    cleaned_data = {**cleaned_data, "site_name": _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))}
    prepared_lines: list[tuple[Worker, Material, dict[str, Any]]] = []
    for item in parse_ppe_lines(cleaned_data["items"]):
        worker = Worker.objects.filter(employee_number=item["employee_number"]).first()
        material = Material.objects.filter(code=item["material_code"], is_ppe=True).first()
        if not worker:
            raise ValueError(f"Работник с табельным номером {item['employee_number']} не найден.")
        if not material:
            raise ValueError(f"Материал {item['material_code']} не найден в перечне СИЗ.")
        if getattr(user, "role", None) == RoleChoices.SITE_MANAGER:
            worker_site_name = (worker.site_name or "").strip()
            if _site_name_variants(worker_site_name).isdisjoint(_site_name_variants(cleaned_data["site_name"])):
                raise ValueError("Начальник участка может оформлять спецодежду только сотрудникам своего участка.")
        _ensure_available_stock(
            material=material,
            location_name=settings.WAREHOUSE_NAME,
            required_quantity=item["quantity"],
            reason="выдача спецодежды",
        )
        prepared_lines.append((worker, material, item))

    issuance = PPEIssuance.objects.create(
        number=generate_number("PPE"),
        issue_date=cleaned_data["issue_date"],
        site_name=cleaned_data["site_name"],
        season=cleaned_data.get("season", ""),
        issued_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for worker, material, item in prepared_lines:
        PPEIssuanceLine.objects.create(
            issuance=issuance,
            worker=worker,
            material=material,
            quantity=item["quantity"],
            service_life_months=item["service_life_months"],
            issue_start_date=issuance.issue_date,
        )
        StockMovement.objects.create(
            movement_date=issuance.issue_date,
            material=material,
            quantity_delta=-item["quantity"],
            location_name=settings.WAREHOUSE_NAME,
            source_type="ppe_issuance",
            source_id=issuance.id,
            unit_price=material.price,
            created_by=user,
            notes=f"Выдача {worker.full_name}",
        )
    audit(user, "create", "ppe_issuance", issuance.id, f"Создана выдача спецодежды {issuance.number}", ip_address)
    return issuance


def create_work_log(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> WorkLog:
    cleaned_data = {**cleaned_data, "site_name": _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))}

    log = WorkLog.objects.create(
        site_name=cleaned_data["site_name"],
        contract=cleaned_data.get("contract"),
        work_type=cleaned_data["work_type"],
        planned_volume=cleaned_data.get("planned_volume") or Decimal("0"),
        actual_volume=cleaned_data.get("actual_volume") or Decimal("0"),
        volume_unit=cleaned_data.get("volume_unit", ""),
        plan_date=cleaned_data.get("plan_date"),
        actual_date=cleaned_data.get("actual_date"),
        status=cleaned_data["status"],
        notes=cleaned_data.get("notes", ""),
        created_by=user,
    )
    audit(user, "create", "work_log", log.id, "Создана запись о работах участка", ip_address)
    return log


def load_operation_draft(*, user, operation_slug: str) -> dict[str, Any]:
    draft = FormDraft.objects.filter(user=user, operation_slug=operation_slug).first()
    return dict(draft.payload_json) if draft else {}


def save_operation_draft(*, user, operation_slug: str, payload: dict[str, Any]) -> FormDraft | None:
    has_values = any(value not in ("", None, [], {}) for value in payload.values())
    if not has_values:
        FormDraft.objects.filter(user=user, operation_slug=operation_slug).delete()
        return None
    draft, _created = FormDraft.objects.update_or_create(
        user=user,
        operation_slug=operation_slug,
        defaults={"payload_json": payload},
    )
    return draft


def clear_operation_draft(*, user, operation_slug: str) -> None:
    FormDraft.objects.filter(user=user, operation_slug=operation_slug).delete()


@transaction.atomic
def transition_document(*, user, record: DocumentRecord, new_status: str, ip_address: str | None = None) -> DocumentRecord:
    model_map = {
        "smr_contract": SMRContract,
        "supply_contract": SupplyContract,
        "procurement_request": ProcurementRequest,
        "primary_document": PrimaryDocument,
        "supplier_document": SupplierDocument,
        "stock_receipt": StockReceipt,
        "stock_issue": StockIssue,
        "write_off": WriteOffAct,
        "ppe_issuance": PPEIssuance,
    }
    model_class = model_map.get(record.entity_type)
    if not model_class:
        raise ValueError("Для этого документа смена статуса не поддерживается.")
    instance = model_class.objects.get(pk=record.entity_id)
    transition_path = _resolve_transition_path(
        user_role=getattr(user, "role", None),
        entity_type=record.entity_type,
        current_status=instance.status,
        new_status=new_status,
    )
    for status in transition_path:
        previous_status = instance.status
        instance.status = status
        if hasattr(instance, "updated_at"):
            instance.save(update_fields=["status", "updated_at"])
        else:
            instance.save(update_fields=["status"])
        if previous_status != status:
            audit(user, "status_change", record.entity_type, instance.pk, f"{previous_status} -> {status}", ip_address)
    return DocumentRecord.objects.get(pk=record.pk)


def warehouse_balances() -> list[dict[str, Any]]:
    rows = (
        Material.objects.annotate(
            warehouse_balance=Coalesce(
                Sum(
                    "movements__quantity_delta",
                    filter=Q(movements__location_name=settings.WAREHOUSE_NAME),
                    output_field=DecimalField(max_digits=14, decimal_places=3),
                ),
                Value(Decimal("0"), output_field=DecimalField(max_digits=14, decimal_places=3)),
            )
        )
        .order_by("code")
    )
    return [
        {
            "id": material.id,
            "location_name": settings.WAREHOUSE_NAME,
            "code": material.code,
            "name": material.name,
            "unit": material.unit,
            "min_stock": material.min_stock,
            "warehouse_balance": material.warehouse_balance,
        }
        for material in rows
    ]


def low_stock_alerts() -> list[dict[str, Any]]:
    return [row for row in warehouse_balances() if row["warehouse_balance"] <= row["min_stock"]]


def _ppe_control_window(filters: dict[str, Any]) -> tuple[date, date]:
    current_day = today()
    date_from = filters.get("date_from") or current_day
    date_to = filters.get("date_to") or (current_day + timedelta(days=PPEIssuanceLine.REPLACEMENT_WARNING_DAYS))
    if date_to < date_from:
        return date_to, date_from
    return date_from, date_to


def ppe_replacement_alerts(*, filters: dict[str, Any] | None = None, site_name: str | None = None) -> list[dict[str, Any]]:
    effective_filters = filters or {}
    due_from, due_to = _ppe_control_window(effective_filters)

    qs = PPEIssuanceLine.objects.select_related("issuance", "worker", "material")
    if site_name:
        qs = qs.filter(issuance__site_name__iexact=site_name)

    location_name = (effective_filters.get("location_name") or "").strip()
    if location_name:
        qs = qs.filter(issuance__site_name__icontains=location_name)
    material_code = (effective_filters.get("material_code") or "").strip()
    if material_code:
        qs = qs.filter(material__code__icontains=material_code)

    alerts: list[dict[str, Any]] = []
    for line in qs.order_by("worker__full_name", "material__code", "-issuance__issue_date"):
        due_date = line.replacement_due_date
        if not due_date or not line.needs_replacement:
            continue
        if line.replacement_status == line.REPLACEMENT_STATUS_EXPIRING and not (due_from <= due_date <= due_to):
            continue

        alerts.append(
            {
                "issue_date": line.issuance.issue_date.isoformat(),
                "issue_start_date": line.replacement_start_date.isoformat() if line.replacement_start_date else "",
                "replacement_due_date": due_date.isoformat(),
                "days_until_replacement": line.days_until_replacement,
                "replacement_status": line.replacement_status,
                "replacement_status_label": line.replacement_status_label,
                "needs_replacement": line.needs_replacement,
                "warning": line.replacement_warning,
                "number": line.issuance.number,
                "site_name": line.issuance.site_name,
                "full_name": line.worker.full_name,
                "employee_number": line.worker.employee_number,
                "material_code": line.material.code,
                "material_name": line.material.name,
                "quantity": float(line.quantity),
                "unit": line.material.unit,
                "service_life_months": line.service_life_months,
            }
        )
    return alerts


def site_balances(*, site_name: str | None = None) -> list[dict[str, Any]]:
    rows = StockMovement.objects.exclude(location_name=settings.WAREHOUSE_NAME)
    if site_name:
        rows = rows.filter(location_name__iexact=site_name)
    rows = (
        rows.values("location_name", "material__code", "material__name", "material__unit")
        .annotate(quantity=Coalesce(Sum("quantity_delta"), Decimal("0")))
        .order_by("location_name", "material__code")
    )
    return [
        {
            "location_name": row["location_name"],
            "code": row["material__code"],
            "name": row["material__name"],
            "unit": row["material__unit"],
            "quantity": row["quantity"],
        }
        for row in rows
        if row["quantity"] != 0
    ]


def dashboard_metrics(*, user=None) -> dict[str, int]:
    role = getattr(user, "role", None)
    if role == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        scoped_records = filter_queryset_for_user(user, DocumentRecord.objects.all())
        related_contracts = SMRContract.objects.filter(
            Q(created_by=user)
            | Q(procurement_requests__site_name__iexact=site_name)
            | Q(stock_issues__site_name__iexact=site_name)
            | Q(write_off_acts__site_name__iexact=site_name)
            | Q(work_logs__site_name__iexact=site_name)
        ).distinct()
        return {
            "contracts": related_contracts.count(),
            "pending": scoped_records.filter(
                status__in=[
                    DocumentStatus.DRAFT,
                    DocumentStatus.APPROVAL,
                    DocumentStatus.REWORK,
                    DocumentStatus.UPLOADED,
                    DocumentStatus.SUPPLY_CONFIRMED,
                ]
            ).count(),
            "supplier_docs": scoped_records.filter(entity_type__in=["supplier_document", "primary_document"]).count(),
            "site_tasks": WorkLog.objects.filter(
                site_name__iexact=site_name,
                status__in=["planned", "delayed", "Запланировано", "С задержкой"],
            ).count(),
            "alerts": 0,
        }
    if role == RoleChoices.ACCOUNTING:
        scoped_records = filter_queryset_for_user(user, DocumentRecord.objects.all())
        return {
            "contracts": 0,
            "pending": scoped_records.exclude(status=DocumentStatus.ACCEPTED).count(),
            "supplier_docs": scoped_records.filter(entity_type__in=["supplier_document", "primary_document"]).count(),
            "site_tasks": 0,
            "alerts": 0,
        }
    return {
        "contracts": SMRContract.objects.count(),
        "pending": DocumentRecord.objects.filter(
            status__in=[
                DocumentStatus.DRAFT,
                DocumentStatus.APPROVAL,
                DocumentStatus.REWORK,
                DocumentStatus.UPLOADED,
                DocumentStatus.SUPPLY_CONFIRMED,
            ]
        ).count(),
        "supplier_docs": SupplierDocument.objects.count() + PrimaryDocument.objects.count(),
        "site_tasks": WorkLog.objects.filter(status__in=["planned", "delayed", "Запланировано", "С задержкой"]).count(),
        "alerts": len(low_stock_alerts()),
    }


def report_period(filters: dict[str, Any]) -> tuple[date, date]:
    return filters.get("date_from") or today().replace(day=1), filters.get("date_to") or today()


def report_stock(filters: dict[str, Any]) -> list[dict[str, Any]]:
    return warehouse_balances()


def report_purchases(filters: dict[str, Any]) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    qs = SupplierDocument.objects.select_related("supplier", "request").filter(doc_date__range=(date_from, date_to)).order_by("-doc_date")
    return [
        {
            "doc_date": item.doc_date.isoformat(),
            "doc_type": item.doc_type,
            "doc_number": item.doc_number,
            "supplier_name": item.supplier.name,
            "amount": float(item.amount),
            "vat_amount": float(item.vat_amount),
            "request_number": item.request.number if item.request else "-",
        }
        for item in qs
    ]


def report_writeoffs(filters: dict[str, Any]) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    qs = WriteOffLine.objects.select_related("act__contract__object", "material").filter(act__act_date__range=(date_from, date_to)).order_by(
        "-act__act_date", "material__code"
    )
    return [
        {
            "act_date": line.act.act_date.isoformat(),
            "number": line.act.number,
            "contract_number": line.act.contract.number,
            "object_name": line.act.contract.object.name if line.act.contract.object else "",
            "work_type": line.act.work_type,
            "material_code": line.material.code,
            "material_name": line.material.name,
            "actual_quantity": float(line.actual_quantity),
            "unit": line.material.unit,
        }
        for line in qs
    ]


def report_work(filters: dict[str, Any]) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    qs = WorkLog.objects.select_related("contract").filter(
        Q(actual_date__range=(date_from, date_to)) | Q(plan_date__range=(date_from, date_to))
    ).order_by("-actual_date", "-plan_date")
    return [
        {
            "site_name": log.site_name,
            "contract_number": log.contract.number if log.contract else "",
            "work_type": log.work_type,
            "planned_volume": float(log.planned_volume),
            "actual_volume": float(log.actual_volume),
            "volume_unit": log.volume_unit,
            "plan_date": log.plan_date.isoformat() if log.plan_date else "",
            "actual_date": log.actual_date.isoformat() if log.actual_date else "",
            "status": log.status,
        }
        for log in qs
    ]


def report_summary(filters: dict[str, Any]) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    rows: list[dict[str, Any]] = []
    for contract in SMRContract.objects.filter(contract_date__range=(date_from, date_to)):
        rows.append(
            {
                "section": "Договор СМР",
                "item": contract.number,
                "amount": float(contract.amount),
                "status": contract.status,
                "event_date": contract.contract_date.isoformat(),
            }
        )
    for doc in SupplierDocument.objects.filter(doc_date__range=(date_from, date_to)):
        rows.append(
            {
                "section": "Документы поставщиков",
                "item": doc.doc_number,
                "amount": float(doc.amount),
                "status": doc.status,
                "event_date": doc.doc_date.isoformat(),
            }
        )
    for doc in PrimaryDocument.objects.select_related("document_type").filter(doc_date__range=(date_from, date_to)):
        rows.append(
            {
                "section": doc.document_type.name,
                "item": doc.number,
                "amount": float(doc.amount),
                "status": doc.status,
                "event_date": doc.doc_date.isoformat(),
            }
        )
    for act in WriteOffAct.objects.filter(act_date__range=(date_from, date_to)):
        rows.append(
            {
                "section": "Акты списания",
                "item": act.number,
                "amount": float(act.work_volume),
                "status": act.status,
                "event_date": act.act_date.isoformat(),
            }
        )
    return sorted(rows, key=lambda item: item["event_date"], reverse=True)


def report_ppe(filters: dict[str, Any]) -> list[dict[str, Any]]:
    return ppe_replacement_alerts(filters=filters)


REPORT_PROVIDERS = {
    "stock": report_stock,
    "purchases": report_purchases,
    "writeoffs": report_writeoffs,
    "work": report_work,
    "summary": report_summary,
    "ppe": report_ppe,
}


def document_records(filters: dict[str, Any], *, user=None) -> list[DocumentRecord]:
    qs = DocumentRecord.objects.select_related("created_by").all()
    if user is not None:
        qs = filter_queryset_for_user(user, qs)
    if filters.get("doc_type"):
        qs = qs.filter(doc_type__icontains=filters["doc_type"])
    if filters.get("doc_number"):
        qs = qs.filter(doc_number__icontains=filters["doc_number"])
    if filters.get("status"):
        qs = qs.filter(status=filters["status"])
    if filters.get("date_from"):
        qs = qs.filter(doc_date__gte=filters["date_from"])
    if filters.get("date_to"):
        qs = qs.filter(doc_date__lte=filters["date_to"])
    if filters.get("counterparty"):
        qs = qs.filter(counterparty__icontains=filters["counterparty"])
    if filters.get("object_name"):
        qs = qs.filter(object_name__icontains=filters["object_name"])
    if filters.get("query"):
        query = filters["query"]
        qs = qs.filter(Q(search_text__icontains=query) | Q(doc_number__icontains=query) | Q(counterparty__icontains=query) | Q(object_name__icontains=query))
    records = list(qs.order_by("-doc_date", "-id"))
    for record in records:
        route_metadata = workflow_route_metadata(record.entity_type)
        record.metadata_json = {**route_metadata, **(record.metadata_json or {})}
    return records


def _backup_model_tables() -> list[tuple[str, Any]]:
    User = get_user_model()
    return [
        ("suppliers", Supplier),
        ("document_types", DocumentType),
        ("users", User),
        ("form_drafts", FormDraft),
        ("materials", Material),
        ("objects", ConstructionObject),
        ("workers", Worker),
        ("norms", MaterialNorm),
        ("contracts", SMRContract),
        ("supply_contracts", SupplyContract),
        ("procurement_requests", ProcurementRequest),
        ("procurement_request_lines", ProcurementRequestLine),
        ("supplier_documents", SupplierDocument),
        ("stock_receipts", StockReceipt),
        ("stock_receipt_lines", StockReceiptLine),
        ("primary_documents", PrimaryDocument),
        ("primary_document_lines", PrimaryDocumentLine),
        ("stock_issues", StockIssue),
        ("stock_issue_lines", StockIssueLine),
        ("work_logs", WorkLog),
        ("write_off_acts", WriteOffAct),
        ("write_off_lines", WriteOffLine),
        ("ppe_issuances", PPEIssuance),
        ("ppe_issuance_lines", PPEIssuanceLine),
        ("stock_movements", StockMovement),
        ("document_records", DocumentRecord),
        ("audit_logs", AuditLog),
    ]


def load_backup_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Некорректный JSON-файл резервной копии: {exc}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("tables"), dict):
        raise ValueError("Файл резервной копии не содержит ожидаемую структуру tables.")
    return payload


def _reset_model_sequences(models: list[Any]) -> None:
    sql_list = connection.ops.sequence_reset_sql(no_style(), models)
    if not sql_list:
        return
    with connection.cursor() as cursor:
        for sql in sql_list:
            cursor.execute(sql)


@transaction.atomic
def restore_backup_payload(*, payload: dict[str, Any], user=None, ip_address: str | None = None, source_name: str = "backup") -> dict[str, int]:
    tables = payload.get("tables")
    if not isinstance(tables, dict):
        raise ValueError("В файле резервной копии отсутствует раздел tables.")

    model_tables = _backup_model_tables()
    restore_models = [model for _key, model in model_tables]
    for _key, model in reversed(model_tables):
        model.objects.all().delete()

    restored_counts: dict[str, int] = {}
    for key, model in model_tables:
        rows = tables.get(key, [])
        if not isinstance(rows, list):
            raise ValueError(f"Таблица {key} в резервной копии имеет некорректный формат.")
        objects = [model(**row) for row in rows]
        if objects:
            model.objects.bulk_create(objects)
        restored_counts[key] = len(objects)

    _reset_model_sequences(restore_models)

    actor = None
    actor_id = getattr(user, "pk", None)
    if actor_id:
        actor = get_user_model().objects.filter(pk=actor_id).first()
    audit(actor, "restore", "database", None, f"Выполнено восстановление из {source_name}", ip_address)
    return restored_counts


def restore_backup_file(path: Path, *, user=None, ip_address: str | None = None) -> dict[str, int]:
    payload = load_backup_payload(path)
    return restore_backup_payload(payload=payload, user=user, ip_address=ip_address, source_name=path.name)


def create_backup_payload() -> dict[str, Any]:
    models = _backup_model_tables()
    payload: dict[str, Any] = {"created_at": timezone.now().isoformat(), "database": settings.POSTGRES_DB, "tables": {}}
    for key, model in models:
        payload["tables"][key] = json.loads(json.dumps(list(model.objects.values()), default=str))
    return payload


def write_backup_file(*, user) -> Path:
    timestamp = timezone.now().strftime("%Y%m%d_%H%M%S")
    path = settings.BACKUPS_DIR / f"ais_backup_{timestamp}.json"
    path.write_text(json.dumps(create_backup_payload(), ensure_ascii=False, indent=2), encoding="utf-8")
    audit(user, "backup", "database", None, f"Создана резервная копия {path.name}")
    return path


def backup_files() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in sorted(settings.BACKUPS_DIR.glob("*.json"), reverse=True):
        stat = path.stat()
        items.append(
            {
                "name": path.name,
                "path": str(path),
                "size": stat.st_size,
                "modified_at": timezone.datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return items
