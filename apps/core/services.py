from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.color import no_style
from django.db import connection, transaction
from django.db.models import DecimalField, Q, Sum, Value
from django.db.models.functions import Coalesce
from django.utils import timezone

from .access import ACCOUNTING_VISIBLE_STATUSES
from .models import (
    AuditLog,
    ConstructionObject,
    DocumentRecord,
    DocumentStatus,
    DocumentType,
    FormDraft,
    Material,
    MaterialNorm,
    Notification,
    NotificationType,
    PPEIssuance,
    PPEIssuanceLine,
    PrimaryDocument,
    PrimaryDocumentLine,
    ProcurementRequest,
    ProcurementRequestLine,
    SiteMaterialRequest,
    SiteMaterialRequestLine,
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
    WorkAcceptanceAct,
    WorkLog,
    WriteOffAct,
    WriteOffLine,
    WriteOffTemplateVariant,
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


def _scoped_site_name(*, user, site_name: str | None = None, fallback: str = "") -> str:
    resolved_site = (site_name or "").strip()
    if getattr(user, "role", None) != RoleChoices.SITE_MANAGER:
        return resolved_site or fallback

    user_site = _user_site_name(user)
    if not user_site:
        raise ValueError("За начальником участка не закреплен контур участка.")
    if resolved_site and resolved_site.casefold() != user_site.casefold():
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
REWORK_METADATA_KEYS = {
    "last_rework_reason",
    "last_rework_by",
    "last_rework_by_id",
    "last_rework_at",
    "rework_history",
}
SUPPLIER_CONFIRM_ROLES = {RoleChoices.SUPPLIER}
PPE_ISSUED_STATUSES = {
    DocumentStatus.SUPPLY_CONFIRMED,
    DocumentStatus.SENT_ACCOUNTING,
    DocumentStatus.ACCEPTED,
}
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
        "creators": {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE},
        "approvers": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "senders": {RoleChoices.ADMIN, RoleChoices.DIRECTOR},
        "viewers": {RoleChoices.SUPPLIER, RoleChoices.ACCOUNTING},
    },
    "site_material_request": {
        "creators": {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER},
        "approvers": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE},
        "senders": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE},
        "viewers": {RoleChoices.PROCUREMENT},
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
        "creators": {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER},
        "approvers": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE},
        "senders": {RoleChoices.ADMIN, RoleChoices.WAREHOUSE},
        "viewers": {RoleChoices.ACCOUNTING},
    },
    "work_acceptance": {
        "creators": {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER},
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
    elif _entity_type == "site_material_request":
        ENTITY_WORKFLOW_TRANSITIONS[_entity_type] = {
            DocumentStatus.DRAFT: {DocumentStatus.APPROVAL: _route["creators"]},
            DocumentStatus.APPROVAL: {
                DocumentStatus.ACCEPTED: _route["approvers"],
                DocumentStatus.REWORK: _route["approvers"],
            },
            DocumentStatus.REWORK: {DocumentStatus.APPROVAL: _route["creators"]},
        }
    elif _entity_type == "ppe_issuance":
        ENTITY_WORKFLOW_TRANSITIONS[_entity_type] = {
            DocumentStatus.DRAFT: {DocumentStatus.APPROVAL: _route["creators"]},
            DocumentStatus.APPROVAL: {
                DocumentStatus.SUPPLY_CONFIRMED: _route["approvers"],
                DocumentStatus.REWORK: _route["approvers"],
            },
            DocumentStatus.SUPPLY_CONFIRMED: {
                DocumentStatus.SENT_ACCOUNTING: _route["senders"],
                DocumentStatus.REWORK: _route["senders"],
            },
            DocumentStatus.SENT_ACCOUNTING: {
                DocumentStatus.ACCEPTED: WORKFLOW_ACCOUNTING_ROLES,
                DocumentStatus.REWORK: WORKFLOW_ACCOUNTING_ROLES,
            },
            DocumentStatus.REWORK: {DocumentStatus.APPROVAL: _route["creators"]},
        }
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


def workflow_status_label(entity_type: str, status: str) -> str:
    if entity_type == "ppe_issuance" and status == DocumentStatus.SUPPLY_CONFIRMED:
        return "Выдача подтверждена"
    return STATUS_LABELS.get(status, status)


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
            allowed.append((status, workflow_status_label(record.entity_type, status)))
            seen.add(status)

    for target_status in (DocumentStatus.ACCEPTED, DocumentStatus.REWORK):
        path = _automatic_transition_path(user_role, record.entity_type, record.status, target_status)
        if path and target_status not in seen:
            allowed.append((target_status, workflow_status_label(record.entity_type, target_status)))
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

    current_label = workflow_status_label(entity_type, current_status)
    target_label = workflow_status_label(entity_type, new_status)
    raise ValueError(f"Переход из статуса '{current_label}' в '{target_label}' для вашей роли недоступен.")


def filter_queryset_for_user(user, queryset):
    role = getattr(user, "role", None)
    if not role:
        return queryset.none()

    model = queryset.model

    if model is Notification:
        if not getattr(user, "is_authenticated", False):
            return queryset.none()
        return queryset.filter(user=user)

    if role in {RoleChoices.ADMIN, RoleChoices.DIRECTOR}:
        return queryset

    if role == RoleChoices.PROCUREMENT:
        if model in {Supplier, ConstructionObject, SupplyContract, ProcurementRequest, SupplierDocument, PrimaryDocument, SiteMaterialRequest}:
            return queryset
        if model is DocumentRecord:
            return queryset.filter(entity_type__in=["procurement_request", "supplier_document", "primary_document", "supply_contract"])
        return queryset.none()

    if role == RoleChoices.WAREHOUSE:
        if model in {Material, SiteMaterialRequest, StockReceipt, StockIssue, ProcurementRequest, PPEIssuance}:
            return queryset
        if model is DocumentRecord:
            return queryset.filter(entity_type__in=["site_material_request", "stock_receipt", "stock_issue", "procurement_request", "ppe_issuance"])
        return queryset.none()

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
                    | Q(site_material_requests__site_name__iexact=site_name)
                    | Q(stock_issues__site_name__iexact=site_name)
                    | Q(write_off_acts__site_name__iexact=site_name)
                    | Q(acceptance_acts__site_name__iexact=site_name)
                    | Q(work_logs__site_name__iexact=site_name)
                )
            return queryset.filter(filters).distinct()
        if model is ProcurementRequest:
            filters = Q(requested_by=user)
            if site_name:
                filters |= Q(site_name__iexact=site_name) | Q(site_request__site_name__iexact=site_name)
            return queryset.filter(filters).distinct()
        if model is SiteMaterialRequest:
            if not site_name:
                return queryset.none()
            return queryset.filter(site_name__iexact=site_name)
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
        if model is Worker:
            if not site_name:
                return queryset.none()
            return queryset.filter(site_name__iexact=site_name)
        if model is WorkAcceptanceAct:
            if not site_name:
                return queryset.none()
            return queryset.filter(site_name__iexact=site_name)
        if model is DocumentRecord:
            filters = Q(created_by=user)
            if site_name:
                filters |= Q(metadata_json__site_name__iexact=site_name) | Q(object_name__iexact=site_name)
            return queryset.filter(filters, entity_type__in=["site_material_request", "write_off", "ppe_issuance", "work_acceptance"]).distinct()
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


def _item_rows_from_json(raw_text: str) -> list[dict[str, Any]]:
    payload = (raw_text or "").strip()
    if not payload:
        return []

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError("Позиции должны быть переданы как структурированный JSON-список.") from exc

    if not isinstance(data, list):
        raise ValueError("Структурированный список позиций должен быть списком.")

    rows: list[dict[str, Any]] = []
    for index, item in enumerate(data, start=1):
        if not isinstance(item, dict):
            raise ValueError(f"Строка #{index} должна быть объектом.")
        rows.append(item)
    return rows


def parse_line_items(raw_text: str, *, require_price: bool = False) -> list[dict[str, Any]]:
    structured_rows = _item_rows_from_json(raw_text)
    lines: list[dict[str, Any]] = []

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

    if not lines:
        raise ValueError("Не указаны позиции документа.")
    return lines


def parse_ppe_lines(raw_text: str) -> list[dict[str, Any]]:
    structured_rows = _item_rows_from_json(raw_text)
    lines: list[dict[str, Any]] = []

    for index, item in enumerate(structured_rows, start=1):
        employee_number = str(item.get("employee_number", "")).strip()
        worker_name = str(item.get("worker_name", "")).strip()
        material_code = str(item.get("material_code", "")).strip()
        material_name = str(item.get("material_name", "")).strip()
        quantity = decimalize(item.get("quantity"))
        service_life_months = int(decimalize(item.get("service_life_months")))
        clothing_size = str(item.get("clothing_size", "") or "").strip()
        shoe_size = str(item.get("shoe_size", "") or "").strip()
        line_ref = f"строка #{index}"
        if not employee_number and not worker_name:
            raise ValueError(f"ФИО работника или табельный номер обязательны для {line_ref}.")
        if not material_code and not material_name:
            raise ValueError(f"Наименование или код спецодежды обязательны для {line_ref}.")
        if quantity <= 0:
            raise ValueError(f"Количество СИЗ должно быть больше нуля для {line_ref}.")
        if service_life_months <= 0:
            raise ValueError(f"Срок службы СИЗ должен быть больше нуля для {line_ref}.")
        lines.append(
            {
                "employee_number": employee_number,
                "worker_name": worker_name,
                "material_code": material_code,
                "material_name": material_name,
                "quantity": quantity,
                "service_life_months": service_life_months,
                "clothing_size": clothing_size,
                "shoe_size": shoe_size,
            }
        )

    if not lines:
        raise ValueError("Не указаны позиции выдачи спецодежды.")
    return lines


def _prepare_ppe_issuance_lines(*, user, site_name: str, raw_items: str) -> list[tuple[Worker, Material, dict[str, Any]]]:
    prepared_lines: list[tuple[Worker, Material, dict[str, Any]]] = []
    for item in parse_ppe_lines(raw_items):
        worker_lookup = (item.get("employee_number") or item.get("worker_name") or "").strip()
        material_lookup = (item.get("material_code") or item.get("material_name") or "").strip()
        worker = Worker.objects.filter(Q(employee_number__iexact=worker_lookup) | Q(full_name__iexact=worker_lookup)).first()
        if not worker and item.get("worker_name"):
            worker = Worker.objects.filter(full_name__iexact=item["worker_name"]).first()
        material = Material.objects.filter(
            Q(code__iexact=material_lookup) | Q(name__iexact=material_lookup),
            is_ppe=True,
        ).first()
        if not material and item.get("material_name"):
            material = Material.objects.filter(name__iexact=item["material_name"], is_ppe=True).first()
        if not worker:
            raise ValueError(f"Работник {worker_lookup} не найден.")
        if not material:
            raise ValueError(f"Материал {material_lookup} не найден в перечне СИЗ.")
        if getattr(user, "role", None) == RoleChoices.SITE_MANAGER:
            worker_site_name = (worker.site_name or "").strip()
            if worker_site_name.casefold() != site_name.casefold():
                raise ValueError("Начальник участка может оформлять спецодежду только сотрудникам своего участка.")
        prepared_lines.append((worker, material, item))
    return prepared_lines


def _ppe_material_quantities(issuance: PPEIssuance) -> list[tuple[Material, Decimal]]:
    totals: dict[int, Decimal] = {}
    materials: dict[int, Material] = {}
    for line in issuance.lines.select_related("material"):
        totals[line.material_id] = totals.get(line.material_id, Decimal("0")) + Decimal(line.quantity or 0)
        materials[line.material_id] = line.material
    return [(materials[material_id], quantity) for material_id, quantity in totals.items()]


def _clear_ppe_issuance_confirmation(issuance: PPEIssuance) -> None:
    StockMovement.objects.filter(source_type="ppe_issuance", source_id=issuance.id).delete()
    if issuance.confirmed_by_id or issuance.confirmed_at:
        issuance.confirmed_by = None
        issuance.confirmed_at = None
        issuance.save(update_fields=["confirmed_by", "confirmed_at", "updated_at"])


def _confirm_ppe_issuance(*, issuance: PPEIssuance, user) -> None:
    material_quantities = _ppe_material_quantities(issuance)
    if not material_quantities:
        raise ValueError("В ведомости СИЗ нет строк для подтверждения выдачи.")

    StockMovement.objects.filter(source_type="ppe_issuance", source_id=issuance.id).delete()
    for material, quantity in material_quantities:
        _ensure_available_stock(
            material=material,
            location_name=settings.WAREHOUSE_NAME,
            required_quantity=quantity,
            reason="подтверждение выдачи СИЗ кладовщиком",
        )

    issuance.confirmed_by = user
    issuance.confirmed_at = timezone.now()
    issuance.save(update_fields=["confirmed_by", "confirmed_at", "updated_at"])

    for line in issuance.lines.select_related("worker", "material"):
        StockMovement.objects.create(
            movement_date=issuance.issue_date,
            material=line.material,
            quantity_delta=-line.quantity,
            location_name=settings.WAREHOUSE_NAME,
            source_type="ppe_issuance",
            source_id=issuance.id,
            unit_price=line.material.price,
            created_by=user,
            notes=f"Выдача {line.worker.full_name}",
        )
        _notify_low_stock_for_material(line.material)


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
    existing_record = DocumentRecord.objects.filter(entity_type=entity_type, entity_id=entity_id).only("metadata_json").first()
    if existing_record is not None:
        existing_metadata = existing_record.metadata_json or {}
        for key in REWORK_METADATA_KEYS:
            if key in existing_metadata and key not in merged_metadata:
                merged_metadata[key] = existing_metadata[key]
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


def _stock_balance_excluding_source(material: Material, location_name: str, source_type: str, source_id: int) -> Decimal:
    aggregate = (
        StockMovement.objects.filter(material=material, location_name__iexact=location_name)
        .exclude(source_type=source_type, source_id=source_id)
        .aggregate(
            total=Coalesce(
                Sum("quantity_delta"),
                Value(Decimal("0"), output_field=DecimalField(max_digits=14, decimal_places=3)),
            )
        )
    )
    return aggregate["total"] or Decimal("0")


def _ensure_available_stock_for_rework(
    *,
    material: Material,
    location_name: str,
    required_quantity: Decimal,
    source_type: str,
    source_id: int,
    reason: str,
) -> None:
    available_quantity = _stock_balance_excluding_source(material, location_name, source_type, source_id)
    if available_quantity < required_quantity:
        raise ValueError(
            f"Недостаточно остатка по материалу {material.code} на локации «{location_name}». "
            f"Доступно без текущего документа: {available_quantity}, требуется: {required_quantity}. Операция: {reason}."
        )


def _notification_record(entity_type: str, entity_id: int | None) -> DocumentRecord | None:
    if not entity_type or not entity_id:
        return None
    return DocumentRecord.objects.filter(entity_type=entity_type, entity_id=entity_id).first()


def _notification_users_for_roles(
    roles: Iterable[str],
    *,
    supplier: Supplier | None = None,
    site_name: str | None = None,
    include_admin: bool = True,
) -> list[Any]:
    role_set = set(roles)
    if include_admin:
        role_set.add(RoleChoices.ADMIN)
    if not role_set:
        return []

    queryset = get_user_model().objects.filter(is_active=True, role__in=role_set)
    role_filter = Q()
    unrestricted_roles = set(role_set)

    if RoleChoices.SUPPLIER in unrestricted_roles:
        unrestricted_roles.remove(RoleChoices.SUPPLIER)
        supplier_filter = Q(role=RoleChoices.SUPPLIER)
        if supplier is not None:
            supplier_filter &= Q(supplier=supplier)
        role_filter |= supplier_filter

    if RoleChoices.SITE_MANAGER in unrestricted_roles:
        unrestricted_roles.remove(RoleChoices.SITE_MANAGER)
        site_filter = Q(role=RoleChoices.SITE_MANAGER)
        if site_name:
            site_filter &= Q(site_name__iexact=site_name)
        role_filter |= site_filter

    if unrestricted_roles:
        role_filter |= Q(role__in=unrestricted_roles)

    return list(queryset.filter(role_filter).order_by("id"))


def _dedupe_notification_users(users: Iterable[Any], *, exclude_user=None) -> list[Any]:
    recipients: list[Any] = []
    seen: set[int] = set()
    excluded_id = getattr(exclude_user, "pk", None)
    for user in users:
        user_id = getattr(user, "pk", None)
        if not user_id or user_id == excluded_id or user_id in seen:
            continue
        seen.add(user_id)
        recipients.append(user)
    return recipients


def create_notification(
    *,
    user,
    kind: str,
    title: str,
    message: str = "",
    entity_type: str = "",
    entity_id: int | None = None,
    document_record: DocumentRecord | None = None,
) -> Notification | None:
    if not getattr(user, "pk", None):
        return None
    if document_record is None:
        document_record = _notification_record(entity_type, entity_id)
    if document_record is not None:
        entity_type = entity_type or document_record.entity_type
        entity_id = entity_id or document_record.entity_id
    return Notification.objects.create(
        user=user,
        kind=kind,
        title=title,
        message=message,
        entity_type=entity_type,
        entity_id=entity_id,
        document_record=document_record,
    )


def notify_users(
    users: Iterable[Any],
    *,
    kind: str,
    title: str,
    message: str = "",
    entity_type: str = "",
    entity_id: int | None = None,
    document_record: DocumentRecord | None = None,
    exclude_user=None,
) -> list[Notification]:
    notifications: list[Notification] = []
    for recipient in _dedupe_notification_users(users, exclude_user=exclude_user):
        notification = create_notification(
            user=recipient,
            kind=kind,
            title=title,
            message=message,
            entity_type=entity_type,
            entity_id=entity_id,
            document_record=document_record,
        )
        if notification is not None:
            notifications.append(notification)
    return notifications


def notification_summary(user, *, limit: int = 6) -> dict[str, Any]:
    if not getattr(user, "is_authenticated", False):
        return {"notification_count": 0, "notification_items": []}
    queryset = Notification.objects.select_related("document_record").filter(user=user, is_read=False).order_by("-created_at", "-id")
    return {
        "notification_count": queryset.count(),
        "notification_items": list(queryset[:limit]),
    }


def mark_notification_read(*, user, notification_id: int) -> bool:
    updated = Notification.objects.filter(user=user, pk=notification_id, is_read=False).update(is_read=True, read_at=timezone.now())
    return bool(updated)


def mark_all_notifications_read(*, user) -> int:
    return Notification.objects.filter(user=user, is_read=False).update(is_read=True, read_at=timezone.now())


def _record_supplier(record: DocumentRecord) -> Supplier | None:
    supplier_id = (record.metadata_json or {}).get("supplier_id")
    if not supplier_id:
        return None
    return Supplier.objects.filter(pk=supplier_id).first()


def _record_site_name(record: DocumentRecord) -> str:
    metadata = record.metadata_json or {}
    return str(metadata.get("site_name") or record.object_name or "").strip()


def _status_notification_roles(record: DocumentRecord, status: str) -> set[str]:
    route = WORKFLOW_ROUTE_MAP.get(record.entity_type, DEFAULT_WORKFLOW_ROUTE)
    if status == DocumentStatus.APPROVAL:
        return set(route.get("approvers", set()))
    if status == DocumentStatus.SUPPLY_CONFIRMED and record.entity_type == "ppe_issuance":
        return set(route.get("senders", set()))
    if status in {DocumentStatus.UPLOADED, DocumentStatus.SUPPLY_CONFIRMED}:
        return set(route.get("reviewers", route.get("approvers", set())))
    if status == DocumentStatus.APPROVED:
        return set(route.get("senders", set()))
    if status == DocumentStatus.SENT_ACCOUNTING:
        return {RoleChoices.ACCOUNTING}
    return set()


def _status_notification_title(status: str, *, entity_type: str = "") -> str:
    if entity_type == "ppe_issuance" and status == DocumentStatus.SUPPLY_CONFIRMED:
        return "Выдача СИЗ подтверждена"
    return {
        DocumentStatus.APPROVAL: "Документ ожидает утверждения",
        DocumentStatus.APPROVED: "Документ утвержден",
        DocumentStatus.SENT_ACCOUNTING: "Документ передан в бухгалтерию",
        DocumentStatus.ACCEPTED: "Документ принят",
        DocumentStatus.REWORK: "Документ возвращен на доработку",
        DocumentStatus.UPLOADED: "Документ поставщика загружен",
        DocumentStatus.SUPPLY_CONFIRMED: "Поставка подтверждена",
    }.get(status, "Статус документа изменен")


def _status_notification_kind(status: str) -> str:
    action_statuses = {
        DocumentStatus.APPROVAL,
        DocumentStatus.REWORK,
        DocumentStatus.SENT_ACCOUNTING,
        DocumentStatus.UPLOADED,
        DocumentStatus.SUPPLY_CONFIRMED,
    }
    return NotificationType.ACTION_REQUIRED if status in action_statuses else NotificationType.STATUS_CHANGED


def _store_rework_reason(*, record: DocumentRecord, actor, reason: str) -> DocumentRecord:
    reason = reason.strip()
    if not reason:
        return record

    metadata = dict(record.metadata_json or {})
    now = timezone.now()
    actor_label = getattr(actor, "full_name_or_username", "") or getattr(actor, "username", "") or ""
    history = metadata.get("rework_history")
    if not isinstance(history, list):
        history = []
    history = history[-9:]
    history.append(
        {
            "reason": reason,
            "by": actor_label,
            "by_id": getattr(actor, "pk", None),
            "at": now.isoformat(),
        }
    )
    metadata.update(
        {
            "last_rework_reason": reason,
            "last_rework_by": actor_label,
            "last_rework_by_id": getattr(actor, "pk", None),
            "last_rework_at": now.isoformat(),
            "rework_history": history,
        }
    )
    record.metadata_json = metadata
    record.save(update_fields=["metadata_json"])
    return record


def _notify_status_event(*, actor, record: DocumentRecord, previous_status: str | None = None) -> None:
    recipients: list[Any] = []
    roles = _status_notification_roles(record, record.status)
    if roles:
        recipients.extend(
            _notification_users_for_roles(
                roles,
                supplier=_record_supplier(record),
                site_name=_record_site_name(record),
                include_admin=True,
            )
        )
    if record.status in {DocumentStatus.APPROVED, DocumentStatus.ACCEPTED, DocumentStatus.REWORK} and record.created_by_id:
        recipients.append(record.created_by)

    previous_label = workflow_status_label(record.entity_type, previous_status) if previous_status else ""
    status_label = workflow_status_label(record.entity_type, record.status)
    if previous_label:
        message = f"{record.doc_type} {record.doc_number}: {previous_label} -> {status_label}."
    else:
        message = f"{record.doc_type} {record.doc_number}: статус {status_label}."
    if record.object_name:
        message = f"{message} Объект/основание: {record.object_name}."
    rework_reason = (record.metadata_json or {}).get("last_rework_reason", "")
    if record.status == DocumentStatus.REWORK and rework_reason:
        message = f"{message} Причина: {rework_reason}."

    notify_users(
        recipients,
        kind=_status_notification_kind(record.status),
        title=_status_notification_title(record.status, entity_type=record.entity_type),
        message=message,
        entity_type=record.entity_type,
        entity_id=record.entity_id,
        document_record=record,
        exclude_user=actor,
    )


def _notify_initial_document_status(*, actor, entity_type: str, entity_id: int) -> None:
    record = _notification_record(entity_type, entity_id)
    if record is None:
        return
    if record.status in {
        DocumentStatus.APPROVAL,
        DocumentStatus.REWORK,
        DocumentStatus.UPLOADED,
        DocumentStatus.SUPPLY_CONFIRMED,
        DocumentStatus.SENT_ACCOUNTING,
    }:
        _notify_status_event(actor=actor, record=record)


def notify_initial_document_status(*, actor, entity_type: str, entity_id: int) -> None:
    _notify_initial_document_status(actor=actor, entity_type=entity_type, entity_id=entity_id)


REWORK_MODEL_MAP = {
    "site_material_request": SiteMaterialRequest,
    "procurement_request": ProcurementRequest,
    "primary_document": PrimaryDocument,
    "supplier_document": SupplierDocument,
    "stock_receipt": StockReceipt,
    "stock_issue": StockIssue,
    "write_off": WriteOffAct,
    "ppe_issuance": PPEIssuance,
    "work_acceptance": WorkAcceptanceAct,
}


def rework_target_status(user, record: DocumentRecord) -> str:
    if record.entity_type == "supplier_document" and getattr(user, "role", None) == RoleChoices.SUPPLIER:
        return DocumentStatus.SUPPLY_CONFIRMED
    return DocumentStatus.APPROVAL


def can_rework_document(user, record: DocumentRecord) -> bool:
    if record.status != DocumentStatus.REWORK:
        return False
    target_status = rework_target_status(user, record)
    return target_status in {value for value, _label in workflow_allowed_statuses(user, record)}


def _notify_document_event(
    *,
    actor,
    roles: Iterable[str],
    title: str,
    message: str,
    entity_type: str,
    entity_id: int,
    supplier: Supplier | None = None,
    site_name: str | None = None,
    include_admin: bool = False,
) -> None:
    record = _notification_record(entity_type, entity_id)
    recipients = _notification_users_for_roles(roles, supplier=supplier, site_name=site_name, include_admin=include_admin)
    notify_users(
        recipients,
        kind=NotificationType.DOCUMENT_CREATED,
        title=title,
        message=message,
        entity_type=entity_type,
        entity_id=entity_id,
        document_record=record,
        exclude_user=actor,
    )


def _notify_low_stock_for_material(material: Material) -> None:
    warehouse_balance = stock_balance(material, settings.WAREHOUSE_NAME)
    if warehouse_balance > material.min_stock:
        return
    recipients = _notification_users_for_roles({RoleChoices.WAREHOUSE, RoleChoices.PROCUREMENT}, include_admin=True)
    for recipient in _dedupe_notification_users(recipients):
        already_open = Notification.objects.filter(
            user=recipient,
            kind=NotificationType.LOW_STOCK,
            entity_type="material",
            entity_id=material.id,
            is_read=False,
        ).exists()
        if already_open:
            continue
        create_notification(
            user=recipient,
            kind=NotificationType.LOW_STOCK,
            title="Низкий остаток материала",
            message=(
                f"{material.code} - {material.name}: на складе {warehouse_balance} {material.unit}, "
                f"минимум {material.min_stock} {material.unit}."
            ),
            entity_type="material",
            entity_id=material.id,
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


def _site_request_line_items(site_request: SiteMaterialRequest | None) -> list[dict[str, Any]]:
    if not site_request:
        return []
    return [
        {
            "material_code": line.material.code,
            "quantity": line.quantity,
            "unit_price": line.unit_price,
            "notes": line.notes,
        }
        for line in site_request.lines.select_related("material")
    ]


def _line_items_from_text_or_site_request(cleaned_data: dict[str, Any]) -> list[dict[str, Any]]:
    raw_items = (cleaned_data.get("items") or "").strip()
    if raw_items:
        return parse_line_items(raw_items)
    line_items = _site_request_line_items(cleaned_data.get("site_request"))
    if line_items:
        return line_items
    raise ValueError("Заполните позиции или выберите заявку участка.")


def _stock_receipt_line_items(cleaned_data: dict[str, Any]) -> list[dict[str, Any]]:
    raw_items = (cleaned_data.get("items") or "").strip()
    if raw_items:
        return parse_line_items(raw_items)

    primary_document = cleaned_data.get("primary_document")
    if primary_document:
        return [
            {
                "material_code": line.material.code,
                "quantity": line.quantity,
                "unit_price": line.unit_price,
                "notes": line.notes,
            }
            for line in primary_document.lines.select_related("material")
        ]

    raise ValueError("Заполните позиции или выберите товарную накладную / УПД.")


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
def create_site_material_request(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> SiteMaterialRequest:
    cleaned_data = {
        **cleaned_data,
        "site_name": _scoped_site_name(
            user=user,
            site_name=cleaned_data.get("site_name"),
            fallback=getattr(user, "site_name", "") or "Участок",
        ),
    }
    request = SiteMaterialRequest.objects.create(
        number=generate_number("SMR-REQ"),
        request_date=cleaned_data["request_date"],
        site_name=cleaned_data["site_name"],
        contract=cleaned_data.get("contract"),
        requested_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for item in parse_line_items(cleaned_data["items"]):
        material = _get_material_or_raise(item["material_code"])
        SiteMaterialRequestLine.objects.create(
            request=request,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"] or material.price,
            notes=item["notes"],
        )
    audit(user, "create", "site_material_request", request.id, f"Создана заявка участка {request.number}", ip_address)
    _notify_initial_document_status(actor=user, entity_type="site_material_request", entity_id=request.id)
    return request


@transaction.atomic
def create_procurement_request(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> ProcurementRequest:
    site_request = cleaned_data.get("site_request")
    cleaned_data = {
        **cleaned_data,
        "site_name": _scoped_site_name(
            user=user,
            site_name=cleaned_data.get("site_name") or (site_request.site_name if site_request else ""),
            fallback=getattr(user, "site_name", "") or "Участок",
        ),
    }
    line_items = _line_items_from_text_or_site_request(cleaned_data)
    request = ProcurementRequest.objects.create(
        number=generate_number("REQ"),
        request_date=cleaned_data["request_date"],
        site_name=cleaned_data["site_name"] or getattr(user, "site_name", "") or "Участок",
        contract=cleaned_data.get("contract") or (site_request.contract if site_request else None),
        site_request=site_request,
        supplier=cleaned_data.get("supplier"),
        requested_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        ProcurementRequestLine.objects.create(
            request=request,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"],
            notes=item["notes"],
        )
    audit(user, "create", "procurement_request", request.id, f"Создана заявка {request.number}", ip_address)
    _notify_initial_document_status(actor=user, entity_type="procurement_request", entity_id=request.id)
    if getattr(user, "role", None) != RoleChoices.PROCUREMENT:
        _notify_document_event(
            actor=user,
            roles={RoleChoices.PROCUREMENT},
            title="Новая заявка на закупку",
            message=f"Заявка {request.number} по участку {request.site_name} ожидает обработки снабжением.",
            entity_type="procurement_request",
            entity_id=request.id,
        )
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
    _notify_initial_document_status(actor=user, entity_type="supplier_document", entity_id=document.id)
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
    _notify_initial_document_status(actor=user, entity_type="primary_document", entity_id=document.id)
    return document


@transaction.atomic
def create_stock_receipt(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> StockReceipt:
    line_items = _stock_receipt_line_items(cleaned_data)
    primary_document = cleaned_data.get("primary_document")
    supplier = cleaned_data.get("supplier") or (primary_document.supplier if primary_document else None)
    if not supplier:
        raise ValueError("Не удалось определить поставщика для приходного ордера.")
    supplier_document = cleaned_data.get("supplier_document")
    if supplier_document and supplier_document.supplier_id != supplier.id:
        raise ValueError("Документ поставщика должен принадлежать выбранному поставщику.")
    if primary_document and primary_document.supplier_id != supplier.id:
        raise ValueError("Товарная накладная должна принадлежать выбранному поставщику.")

    receipt = StockReceipt.objects.create(
        number=generate_number("REC"),
        receipt_date=cleaned_data["receipt_date"],
        supplier=supplier,
        supplier_document=supplier_document,
        primary_document=primary_document,
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
    _notify_initial_document_status(actor=user, entity_type="stock_receipt", entity_id=receipt.id)
    _notify_document_event(
        actor=user,
        roles={RoleChoices.PROCUREMENT},
        title="Материалы поступили на склад",
        message=f"Приход {receipt.number} от поставщика {receipt.supplier.name} отражен на складе.",
        entity_type="stock_receipt",
        entity_id=receipt.id,
    )
    for line in receipt.lines.select_related("material"):
        _notify_low_stock_for_material(line.material)
    return receipt


@transaction.atomic
def create_stock_issue(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> StockIssue:
    site_request = cleaned_data.get("site_request")
    line_items = _line_items_from_text_or_site_request(cleaned_data)
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

    issue_site_name = cleaned_data.get("site_name") or (site_request.site_name if site_request else "")
    if not issue_site_name:
        raise ValueError("Укажите участок или выберите заявку участка.")

    issue = StockIssue.objects.create(
        number=generate_number("ISS"),
        issue_date=cleaned_data["issue_date"],
        site_name=issue_site_name,
        contract=cleaned_data.get("contract") or (site_request.contract if site_request else None),
        site_request=site_request,
        stock_receipt=cleaned_data.get("stock_receipt"),
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
    _notify_initial_document_status(actor=user, entity_type="stock_issue", entity_id=issue.id)
    _notify_document_event(
        actor=user,
        roles={RoleChoices.SITE_MANAGER},
        title="Материалы отпущены на участок",
        message=f"По требованию {issue.number} материалы переданы на участок {issue.site_name}.",
        entity_type="stock_issue",
        entity_id=issue.id,
        site_name=issue.site_name,
    )
    for line in issue.lines.select_related("material"):
        _notify_low_stock_for_material(line.material)
    return issue


@transaction.atomic
def create_writeoff(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> WriteOffAct:
    cleaned_data = {**cleaned_data, "site_name": _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))}
    contract = cleaned_data["contract"]
    work_type = (cleaned_data.get("work_type") or contract.work_type or "").strip()
    work_volume = cleaned_data.get("work_volume") or contract.planned_volume
    volume_unit = (cleaned_data.get("volume_unit") or contract.volume_unit or "").strip()
    if not work_type:
        raise ValueError("Укажите вид работ или заполните его в договоре СМР.")
    if work_volume is None:
        raise ValueError("Укажите объем работ или заполните плановый объем в договоре СМР.")
    if work_volume <= 0:
        raise ValueError("Объем работ должен быть больше нуля.")

    norms = list(MaterialNorm.objects.select_related("material").filter(work_type=work_type).order_by("material__code"))
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
        contract=contract,
        template_variant=cleaned_data.get("template_variant") or WriteOffTemplateVariant.CONTRACT,
        site_name=cleaned_data["site_name"],
        work_type=work_type,
        work_volume=work_volume,
        volume_unit=volume_unit,
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
    _notify_initial_document_status(actor=user, entity_type="write_off", entity_id=act.id)
    return act


@transaction.atomic
def create_ppe_issuance(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> PPEIssuance:
    cleaned_data = {**cleaned_data, "site_name": _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))}
    prepared_lines = _prepare_ppe_issuance_lines(user=user, site_name=cleaned_data["site_name"], raw_items=cleaned_data["items"])

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
            clothing_size=item.get("clothing_size", ""),
            shoe_size=item.get("shoe_size", ""),
        )
    audit(user, "create", "ppe_issuance", issuance.id, f"Создана выдача спецодежды {issuance.number}", ip_address)
    _notify_initial_document_status(actor=user, entity_type="ppe_issuance", entity_id=issuance.id)
    return issuance


@transaction.atomic
def create_work_acceptance(*, user, cleaned_data: dict[str, Any], ip_address: str | None = None) -> WorkAcceptanceAct:
    contract = cleaned_data["contract"]
    site_name = _scoped_site_name(
        user=user,
        site_name=cleaned_data.get("site_name"),
        fallback=contract.object.name if contract.object else getattr(user, "site_name", "") or "Участок",
    )
    act = WorkAcceptanceAct.objects.create(
        number=generate_number("ACC"),
        act_date=cleaned_data["act_date"],
        contract=contract,
        site_name=site_name,
        work_description=cleaned_data.get("work_description") or contract.subject,
        accepted_volume=cleaned_data.get("accepted_volume") or contract.planned_volume,
        volume_unit=cleaned_data.get("volume_unit") or contract.volume_unit,
        amount=cleaned_data.get("amount") or contract.amount,
        created_by=user,
        status=validate_initial_document_status(cleaned_data["status"]),
        notes=cleaned_data.get("notes", ""),
    )
    audit(user, "create", "work_acceptance", act.id, f"Создан акт сдачи-приемки {act.number}", ip_address)
    _notify_initial_document_status(actor=user, entity_type="work_acceptance", entity_id=act.id)
    return act


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


def _update_site_material_request(instance: SiteMaterialRequest, *, user, cleaned_data: dict[str, Any]) -> None:
    site_name = _scoped_site_name(
        user=user,
        site_name=cleaned_data.get("site_name"),
        fallback=getattr(user, "site_name", "") or instance.site_name,
    )
    instance.request_date = cleaned_data["request_date"]
    instance.site_name = site_name
    instance.contract = cleaned_data.get("contract")
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    for item in parse_line_items(cleaned_data["items"]):
        material = _get_material_or_raise(item["material_code"])
        SiteMaterialRequestLine.objects.create(
            request=instance,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"] or material.price,
            notes=item["notes"],
        )


def _update_procurement_request(instance: ProcurementRequest, *, user, cleaned_data: dict[str, Any]) -> None:
    site_request = cleaned_data.get("site_request")
    site_name = _scoped_site_name(
        user=user,
        site_name=cleaned_data.get("site_name") or (site_request.site_name if site_request else ""),
        fallback=getattr(user, "site_name", "") or instance.site_name,
    )
    line_items = _line_items_from_text_or_site_request({**cleaned_data, "site_name": site_name})
    instance.request_date = cleaned_data["request_date"]
    instance.site_name = site_name
    instance.contract = cleaned_data.get("contract") or (site_request.contract if site_request else None)
    instance.site_request = site_request
    instance.supplier = cleaned_data.get("supplier")
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        ProcurementRequestLine.objects.create(
            request=instance,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"],
            notes=item["notes"],
        )


def _update_supplier_document(instance: SupplierDocument, *, user, cleaned_data: dict[str, Any]) -> None:
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
        raise ValueError("Не удалось определить поставщика. Укажите поставщика или привяжите пользователя к поставщику.")

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

    instance.supplier = supplier
    instance.request = request
    instance.supply_contract = supply_contract
    instance.doc_type = cleaned_data["doc_type"]
    instance.doc_number = cleaned_data.get("doc_number") or instance.doc_number or generate_number(document_type.prefix if document_type else "SUPDOC")
    instance.doc_date = cleaned_data["doc_date"]
    instance.amount = cleaned_data.get("amount") or Decimal("0")
    instance.vat_amount = cleaned_data.get("vat_amount") or Decimal("0")
    attachment = cleaned_data.get("attachment")
    if attachment:
        instance.attachment = attachment
    instance.notes = cleaned_data.get("notes", "")
    instance.save()


def _update_primary_document(instance: PrimaryDocument, *, user, cleaned_data: dict[str, Any]) -> None:
    document_type = cleaned_data["document_type"]
    if not document_type.is_active or not document_type.available_for_generation:
        raise ValueError("Выбранный тип документа недоступен для генерации.")

    line_items = _primary_document_line_items(cleaned_data, document_type=document_type)
    calculated_amount = sum((item["quantity"] * item["unit_price"] for item in line_items), Decimal("0"))
    instance.document_type = document_type
    instance.doc_date = cleaned_data["doc_date"]
    instance.supplier = _primary_document_supplier(cleaned_data=cleaned_data, user=user)
    instance.procurement_request = cleaned_data.get("request")
    instance.supply_contract = cleaned_data.get("supply_contract")
    instance.stock_receipt = cleaned_data.get("stock_receipt")
    instance.site_name = _primary_document_site_name(cleaned_data=cleaned_data, user=user)
    instance.basis_reference = _primary_document_basis_reference(cleaned_data)
    instance.amount = cleaned_data.get("amount") or calculated_amount
    instance.vat_amount = cleaned_data.get("vat_amount") or Decimal("0")
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        PrimaryDocumentLine.objects.create(
            document=instance,
            material=material,
            quantity=item["quantity"],
            unit_price=item["unit_price"] or material.price,
            notes=item["notes"],
        )


def _update_stock_receipt(instance: StockReceipt, *, user, cleaned_data: dict[str, Any]) -> None:
    line_items = _stock_receipt_line_items(cleaned_data)
    primary_document = cleaned_data.get("primary_document")
    supplier = cleaned_data.get("supplier") or (primary_document.supplier if primary_document else None)
    if not supplier:
        raise ValueError("Не удалось определить поставщика для приходного ордера.")
    supplier_document = cleaned_data.get("supplier_document")
    if supplier_document and supplier_document.supplier_id != supplier.id:
        raise ValueError("Документ поставщика должен принадлежать выбранному поставщику.")
    if primary_document and primary_document.supplier_id != supplier.id:
        raise ValueError("Товарная накладная должна принадлежать выбранному поставщику.")

    prepared_lines: list[tuple[Material, dict[str, Any], Decimal]] = []
    material_quantities: dict[int, Decimal] = {}
    materials_by_id: dict[int, Material] = {}
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        unit_price = item["unit_price"] or material.price
        prepared_lines.append((material, item, unit_price))
        material_quantities[material.id] = material_quantities.get(material.id, Decimal("0")) + item["quantity"]
        materials_by_id[material.id] = material

    for material_id, quantity in material_quantities.items():
        material = materials_by_id[material_id]
        balance_without_receipt = _stock_balance_excluding_source(material, settings.WAREHOUSE_NAME, "stock_receipt", instance.id)
        if balance_without_receipt + quantity < 0:
            raise ValueError(
                f"Нельзя сохранить приход {instance.number}: после правки остаток {material.code} на складе станет отрицательным."
            )

    instance.receipt_date = cleaned_data["receipt_date"]
    instance.supplier = supplier
    instance.supplier_document = supplier_document
    instance.primary_document = primary_document
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    StockMovement.objects.filter(source_type="stock_receipt", source_id=instance.id).delete()
    for material, item, unit_price in prepared_lines:
        StockReceiptLine.objects.create(receipt=instance, material=material, quantity=item["quantity"], unit_price=unit_price, notes=item["notes"])
        StockMovement.objects.create(
            movement_date=instance.receipt_date,
            material=material,
            quantity_delta=item["quantity"],
            location_name=settings.WAREHOUSE_NAME,
            source_type="stock_receipt",
            source_id=instance.id,
            unit_price=unit_price,
            created_by=user,
            notes=instance.notes,
        )
        _notify_low_stock_for_material(material)


def _update_stock_issue(instance: StockIssue, *, user, cleaned_data: dict[str, Any]) -> None:
    site_request = cleaned_data.get("site_request")
    line_items = _line_items_from_text_or_site_request(cleaned_data)
    issue_site_name = cleaned_data.get("site_name") or (site_request.site_name if site_request else "")
    if not issue_site_name:
        raise ValueError("Укажите участок или выберите заявку участка.")

    prepared_lines: list[tuple[Material, dict[str, Any], Decimal]] = []
    material_quantities: dict[int, Decimal] = {}
    materials_by_id: dict[int, Material] = {}
    for item in line_items:
        material = _get_material_or_raise(item["material_code"])
        prepared_lines.append((material, item, item["unit_price"] or material.price))
        material_quantities[material.id] = material_quantities.get(material.id, Decimal("0")) + item["quantity"]
        materials_by_id[material.id] = material

    for material_id, quantity in material_quantities.items():
        _ensure_available_stock_for_rework(
            material=materials_by_id[material_id],
            location_name=settings.WAREHOUSE_NAME,
            required_quantity=quantity,
            source_type="stock_issue",
            source_id=instance.id,
            reason="доработка отпуска материалов",
        )

    instance.issue_date = cleaned_data["issue_date"]
    instance.site_name = issue_site_name
    instance.contract = cleaned_data.get("contract") or (site_request.contract if site_request else None)
    instance.site_request = site_request
    instance.stock_receipt = cleaned_data.get("stock_receipt")
    instance.received_by_name = cleaned_data["received_by_name"]
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    StockMovement.objects.filter(source_type="stock_issue", source_id=instance.id).delete()
    for material, item, unit_price in prepared_lines:
        StockIssueLine.objects.create(issue=instance, material=material, quantity=item["quantity"], unit_price=unit_price, notes=item["notes"])
        StockMovement.objects.create(
            movement_date=instance.issue_date,
            material=material,
            quantity_delta=-item["quantity"],
            location_name=settings.WAREHOUSE_NAME,
            source_type="stock_issue",
            source_id=instance.id,
            unit_price=unit_price,
            created_by=user,
            notes=instance.notes,
        )
        StockMovement.objects.create(
            movement_date=instance.issue_date,
            material=material,
            quantity_delta=item["quantity"],
            location_name=instance.site_name,
            source_type="stock_issue",
            source_id=instance.id,
            unit_price=unit_price,
            created_by=user,
            notes=instance.notes,
        )
        _notify_low_stock_for_material(material)


def _update_writeoff(instance: WriteOffAct, *, user, cleaned_data: dict[str, Any]) -> None:
    site_name = _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))
    contract = cleaned_data["contract"]
    work_type = (cleaned_data.get("work_type") or contract.work_type or "").strip()
    work_volume = cleaned_data.get("work_volume") or contract.planned_volume
    volume_unit = (cleaned_data.get("volume_unit") or contract.volume_unit or "").strip()
    if not work_type:
        raise ValueError("Укажите вид работ или заполните его в договоре СМР.")
    if work_volume is None:
        raise ValueError("Укажите объем работ или заполните плановый объем в договоре СМР.")
    if work_volume <= 0:
        raise ValueError("Объем работ должен быть больше нуля.")

    norms = list(MaterialNorm.objects.select_related("material").filter(work_type=work_type).order_by("material__code"))
    if not norms:
        raise ValueError("Для выбранного вида работ не настроены нормы расхода материалов.")

    prepared_lines: list[tuple[MaterialNorm, Decimal]] = []
    for norm in norms:
        quantity = (work_volume * norm.norm_per_unit).quantize(Decimal("0.001"))
        _ensure_available_stock_for_rework(
            material=norm.material,
            location_name=site_name,
            required_quantity=quantity,
            source_type="write_off",
            source_id=instance.id,
            reason="доработка списания материалов",
        )
        prepared_lines.append((norm, quantity))

    instance.act_date = cleaned_data["act_date"]
    instance.contract = contract
    instance.template_variant = cleaned_data.get("template_variant") or WriteOffTemplateVariant.CONTRACT
    instance.site_name = site_name
    instance.work_type = work_type
    instance.work_volume = work_volume
    instance.volume_unit = volume_unit
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    StockMovement.objects.filter(source_type="write_off", source_id=instance.id).delete()
    for norm, quantity in prepared_lines:
        WriteOffLine.objects.create(
            act=instance,
            material=norm.material,
            norm_per_unit=norm.norm_per_unit,
            calculated_quantity=quantity,
            actual_quantity=quantity,
            unit_price=norm.material.price,
            notes=norm.notes,
        )
        StockMovement.objects.create(
            movement_date=instance.act_date,
            material=norm.material,
            quantity_delta=-quantity,
            location_name=instance.site_name,
            source_type="write_off",
            source_id=instance.id,
            unit_price=norm.material.price,
            created_by=user,
            notes=f"Списание по акту: {instance.work_type}",
        )


def _update_ppe_issuance(instance: PPEIssuance, *, user, cleaned_data: dict[str, Any]) -> None:
    site_name = _scoped_site_name(user=user, site_name=cleaned_data.get("site_name"))
    prepared_lines = _prepare_ppe_issuance_lines(user=user, site_name=site_name, raw_items=cleaned_data["items"])

    instance.issue_date = cleaned_data["issue_date"]
    instance.site_name = site_name
    instance.season = cleaned_data.get("season", "")
    instance.notes = cleaned_data.get("notes", "")
    instance.save()

    instance.lines.all().delete()
    _clear_ppe_issuance_confirmation(instance)
    for worker, material, item in prepared_lines:
        PPEIssuanceLine.objects.create(
            issuance=instance,
            worker=worker,
            material=material,
            quantity=item["quantity"],
            service_life_months=item["service_life_months"],
            issue_start_date=instance.issue_date,
            clothing_size=item.get("clothing_size", ""),
            shoe_size=item.get("shoe_size", ""),
        )


def _update_work_acceptance(instance: WorkAcceptanceAct, *, user, cleaned_data: dict[str, Any]) -> None:
    contract = cleaned_data["contract"]
    site_name = _scoped_site_name(
        user=user,
        site_name=cleaned_data.get("site_name"),
        fallback=contract.object.name if contract.object else getattr(user, "site_name", "") or instance.site_name,
    )
    instance.act_date = cleaned_data["act_date"]
    instance.contract = contract
    instance.site_name = site_name
    instance.work_description = cleaned_data.get("work_description") or contract.subject
    instance.accepted_volume = cleaned_data.get("accepted_volume") or contract.planned_volume
    instance.volume_unit = cleaned_data.get("volume_unit") or contract.volume_unit
    instance.amount = cleaned_data.get("amount") or contract.amount
    instance.notes = cleaned_data.get("notes", "")
    instance.save()


REWORK_UPDATE_HANDLERS = {
    "site_material_request": _update_site_material_request,
    "procurement_request": _update_procurement_request,
    "supplier_document": _update_supplier_document,
    "primary_document": _update_primary_document,
    "stock_receipt": _update_stock_receipt,
    "stock_issue": _update_stock_issue,
    "write_off": _update_writeoff,
    "ppe_issuance": _update_ppe_issuance,
    "work_acceptance": _update_work_acceptance,
}


@transaction.atomic
def update_rework_document(*, user, record: DocumentRecord, cleaned_data: dict[str, Any], ip_address: str | None = None) -> DocumentRecord:
    record = DocumentRecord.objects.select_for_update().get(pk=record.pk)
    if not can_rework_document(user, record):
        raise ValueError("Для этого документа доработка недоступна.")

    model_class = REWORK_MODEL_MAP.get(record.entity_type)
    handler = REWORK_UPDATE_HANDLERS.get(record.entity_type)
    if not model_class or not handler:
        raise ValueError("Для этого типа документа доработка через форму не поддерживается.")

    instance = model_class.objects.select_for_update().get(pk=record.entity_id)
    handler(instance, user=user, cleaned_data=cleaned_data)
    audit(user, "update", record.entity_type, instance.pk, f"Доработан документ {record.doc_number}", ip_address)

    updated_record = DocumentRecord.objects.get(entity_type=record.entity_type, entity_id=record.entity_id)
    target_status = rework_target_status(user, updated_record)
    if updated_record.status != target_status:
        updated_record = transition_document(user=user, record=updated_record, new_status=target_status, ip_address=ip_address)
    return updated_record


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
def transition_document(
    *,
    user,
    record: DocumentRecord,
    new_status: str,
    ip_address: str | None = None,
    rework_reason: str = "",
) -> DocumentRecord:
    model_map = {
        "smr_contract": SMRContract,
        "supply_contract": SupplyContract,
        "site_material_request": SiteMaterialRequest,
        "procurement_request": ProcurementRequest,
        "primary_document": PrimaryDocument,
        "supplier_document": SupplierDocument,
        "stock_receipt": StockReceipt,
        "stock_issue": StockIssue,
        "write_off": WriteOffAct,
        "ppe_issuance": PPEIssuance,
        "work_acceptance": WorkAcceptanceAct,
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
    rework_reason = (rework_reason or "").strip()
    for status in transition_path:
        previous_status = instance.status
        instance.status = status
        if hasattr(instance, "updated_at"):
            instance.save(update_fields=["status", "updated_at"])
        else:
            instance.save(update_fields=["status"])
        if previous_status != status:
            audit_details = f"{previous_status} -> {status}"
            if status == DocumentStatus.REWORK and rework_reason:
                audit_details = f"{audit_details}. Причина: {rework_reason}"
            audit(user, "status_change", record.entity_type, instance.pk, audit_details, ip_address)
            if record.entity_type == "ppe_issuance":
                if status == DocumentStatus.SUPPLY_CONFIRMED:
                    _confirm_ppe_issuance(issuance=instance, user=user)
                elif status == DocumentStatus.REWORK:
                    _clear_ppe_issuance_confirmation(instance)
            updated_record = DocumentRecord.objects.get(entity_type=record.entity_type, entity_id=instance.pk)
            if status == DocumentStatus.REWORK and rework_reason:
                updated_record = _store_rework_reason(record=updated_record, actor=user, reason=rework_reason)
            _notify_status_event(actor=user, record=updated_record, previous_status=previous_status)
        if record.entity_type == "work_acceptance" and status == DocumentStatus.ACCEPTED and instance.contract.status != DocumentStatus.ACCEPTED:
            instance.contract.status = DocumentStatus.ACCEPTED
            instance.contract.save(update_fields=["status", "updated_at"])
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

    qs = PPEIssuanceLine.objects.select_related("issuance", "worker", "material").filter(issuance__status__in=PPE_ISSUED_STATUSES)
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
        related_contracts = filter_queryset_for_user(user, SMRContract.objects.all())
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
    if role == RoleChoices.PROCUREMENT:
        scoped_records = filter_queryset_for_user(user, DocumentRecord.objects.all())
        return {
            "contracts": SupplyContract.objects.count(),
            "pending": scoped_records.exclude(status=DocumentStatus.ACCEPTED).count(),
            "supplier_docs": scoped_records.filter(entity_type__in=["supplier_document", "primary_document"]).count(),
            "site_tasks": ProcurementRequest.objects.filter(site_request__isnull=False).count(),
            "alerts": SiteMaterialRequest.objects.filter(status__in=[DocumentStatus.DRAFT, DocumentStatus.APPROVAL, DocumentStatus.REWORK]).count(),
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


ARCHIVE_ENTITY_TYPES_BY_ROLE = {
    RoleChoices.SITE_MANAGER: {"site_material_request", "write_off", "ppe_issuance"},
    RoleChoices.PROCUREMENT: {"procurement_request", "supplier_document", "primary_document"},
    RoleChoices.WAREHOUSE: {"procurement_request", "stock_receipt", "stock_issue", "ppe_issuance"},
    RoleChoices.SUPPLIER: {"procurement_request", "supplier_document", "primary_document"},
}


def document_records(
    filters: dict[str, Any],
    *,
    user=None,
    archived_only: bool = False,
    active_only: bool = False,
) -> list[DocumentRecord]:
    qs = DocumentRecord.objects.select_related("created_by").all()
    if user is not None:
        qs = filter_queryset_for_user(user, qs)
    if archived_only:
        qs = qs.filter(status=DocumentStatus.ACCEPTED, doc_date__lte=today())
        role = getattr(user, "role", None)
        allowed_entity_types = ARCHIVE_ENTITY_TYPES_BY_ROLE.get(role)
        if allowed_entity_types:
            qs = qs.filter(entity_type__in=allowed_entity_types)
    if active_only:
        qs = qs.exclude(status=DocumentStatus.ACCEPTED)
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
        ("site_material_requests", SiteMaterialRequest),
        ("site_material_request_lines", SiteMaterialRequestLine),
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
        ("work_acceptance_acts", WorkAcceptanceAct),
        ("write_off_acts", WriteOffAct),
        ("write_off_lines", WriteOffLine),
        ("ppe_issuances", PPEIssuance),
        ("ppe_issuance_lines", PPEIssuanceLine),
        ("stock_movements", StockMovement),
        ("document_records", DocumentRecord),
        ("notifications", Notification),
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
