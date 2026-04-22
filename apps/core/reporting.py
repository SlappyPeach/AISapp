from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.db.models import Q
from django.utils import timezone

from .access import ACCOUNTING_VISIBLE_STATUSES
from .models import (
    DocumentStatus,
    Material,
    PPEIssuanceLine,
    RoleChoices,
    SMRContract,
    StockMovement,
    StockReceiptLine,
    SupplierDocument,
    WorkLog,
    WriteOffLine,
)


REPORT_TITLES = {
    "stock": "Отчет об остатках материалов за период",
    "purchases": "Отчет о закупках материалов за период",
    "writeoffs": "Отчет о списании материалов по объектам",
    "work": "Отчет по работе участков",
    "summary": "Сводный отчет для бухгалтерии",
    "ppe": "Отчет по выданной спецодежде и срокам службы",
    "movements": "Отчет о движении материалов за период",
    "consumption": "Статистический отчет по расходу материалов",
    "work_stats": "Статистический отчет по выполненным работам",
}

REPORT_CHOICES = [
    ("stock", "Остатки за период"),
    ("purchases", "Закупки"),
    ("writeoffs", "Списания по объектам"),
    ("work", "Работа участков"),
    ("summary", "Сводный (бухгалтерия)"),
    ("movements", "Движение материалов"),
    ("consumption", "Статистика по расходу материалов"),
    ("work_stats", "Статистика по выполненным работам"),
    ("ppe", "Выданная спецодежда"),
]

STATUS_LABELS = dict(DocumentStatus.choices)
MOVEMENT_TYPE_LABELS = {
    "stock_receipt": "Поступление на склад",
    "stock_issue": "Перемещение (требование-накладная)",
    "write_off": "Списание материалов",
    "ppe_issuance": "Выдача СИЗ",
}


def report_period(filters: dict[str, Any]) -> tuple[date, date]:
    current_day = timezone.localdate()
    return filters.get("date_from") or current_day.replace(day=1), filters.get("date_to") or current_day


def _user_site_name(user) -> str:
    return (getattr(user, "site_name", "") or "").strip()


def _period_label(date_from: date, date_to: date) -> str:
    return f"{date_from.isoformat()} - {date_to.isoformat()}"


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _to_float(value: Any) -> float:
    return float(_to_decimal(value))


def _status_label(status: str | None) -> str:
    if not status:
        return "-"
    return STATUS_LABELS.get(status, status)


def _movement_label(source_type: str) -> str:
    return MOVEMENT_TYPE_LABELS.get(source_type, source_type)


def _apply_movement_filters(queryset, filters: dict[str, Any]):
    material_code = (filters.get("material_code") or "").strip()
    location_name = (filters.get("location_name") or "").strip()
    if material_code:
        queryset = queryset.filter(material__code__icontains=material_code)
    if location_name:
        queryset = queryset.filter(location_name__icontains=location_name)
    return queryset


def _apply_contract_filters(queryset, filters: dict[str, Any], *, contract_field: str = "contract__number"):
    contract_number = (filters.get("contract_number") or "").strip()
    object_name = (filters.get("object_name") or "").strip()
    if contract_number:
        queryset = queryset.filter(**{f"{contract_field}__icontains": contract_number})
    if object_name:
        queryset = queryset.filter(contract__object__name__icontains=object_name)
    return queryset


def _deviation_days(*, plan_date: date | None, actual_date: date | None, fallback_date: date) -> int:
    if not plan_date:
        return 0
    if actual_date:
        return (actual_date - plan_date).days
    if fallback_date > plan_date:
        return (fallback_date - plan_date).days
    return 0


def _execution_status(*, plan_date: date | None, actual_date: date | None) -> str:
    if not actual_date:
        return "Не выполнено"
    if not plan_date:
        return "Выполнено"
    return "В срок" if actual_date <= plan_date else "С опозданием"


def _ppe_control_window(filters: dict[str, Any]) -> tuple[date, date]:
    current_day = timezone.localdate()
    date_from = filters.get("date_from") or current_day
    date_to = filters.get("date_to") or (current_day + timedelta(days=PPEIssuanceLine.REPLACEMENT_WARNING_DAYS))
    if date_to < date_from:
        return date_to, date_from
    return date_from, date_to


def _stock_locations(filters: dict[str, Any], *, user=None) -> list[str]:
    location_name = (filters.get("location_name") or "").strip()
    user_role = getattr(user, "role", None)
    if user_role == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        if not site_name:
            return []
        if location_name and location_name.casefold() not in site_name.casefold():
            return []
        return [site_name]

    if not location_name:
        return [settings.WAREHOUSE_NAME]

    names = set(
        StockMovement.objects.filter(location_name__icontains=location_name).values_list("location_name", flat=True).distinct()
    )
    if location_name.casefold() in settings.WAREHOUSE_NAME.casefold():
        names.add(settings.WAREHOUSE_NAME)
    return sorted(names)


def report_stock(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    locations = _stock_locations(filters, user=user)
    if not locations:
        return []

    material_code = (filters.get("material_code") or "").strip()
    materials_qs = Material.objects.order_by("code")
    if material_code:
        materials_qs = materials_qs.filter(code__icontains=material_code)
    materials = {material.id: material for material in materials_qs}

    movements_qs = StockMovement.objects.select_related("material").filter(movement_date__lte=date_to, location_name__in=locations)
    if material_code:
        movements_qs = movements_qs.filter(material__code__icontains=material_code)

    stats: dict[tuple[str, int], dict[str, Decimal]] = defaultdict(
        lambda: {
            "opening": Decimal("0"),
            "incoming": Decimal("0"),
            "outgoing": Decimal("0"),
            "closing": Decimal("0"),
        }
    )

    for movement in movements_qs:
        key = (movement.location_name, movement.material_id)
        row = stats[key]
        quantity = _to_decimal(movement.quantity_delta)
        row["closing"] += quantity
        if movement.movement_date < date_from:
            row["opening"] += quantity
        elif quantity >= 0:
            row["incoming"] += quantity
        else:
            row["outgoing"] += abs(quantity)
        if movement.material_id not in materials:
            materials[movement.material_id] = movement.material

    if settings.WAREHOUSE_NAME in locations:
        for material in materials.values():
            stats.setdefault(
                (settings.WAREHOUSE_NAME, material.id),
                {
                    "opening": Decimal("0"),
                    "incoming": Decimal("0"),
                    "outgoing": Decimal("0"),
                    "closing": Decimal("0"),
                },
            )

    def _sort_key(key: tuple[str, int]) -> tuple[int, str, str]:
        location_name, material_id = key
        material = materials.get(material_id)
        material_code_value = material.code if material else ""
        warehouse_priority = 0 if location_name == settings.WAREHOUSE_NAME else 1
        return warehouse_priority, location_name, material_code_value

    rows: list[dict[str, Any]] = []
    total_opening = Decimal("0")
    total_incoming = Decimal("0")
    total_outgoing = Decimal("0")
    total_closing = Decimal("0")
    total_closing_amount = Decimal("0")

    for key in sorted(stats, key=_sort_key):
        location_name, material_id = key
        material = materials.get(material_id)
        if not material:
            continue
        item = stats[key]
        opening = item["opening"]
        incoming = item["incoming"]
        outgoing = item["outgoing"]
        closing = item["closing"]

        # Для участков с нулевыми значениями не добавляем пустые строки.
        if location_name != settings.WAREHOUSE_NAME and opening == incoming == outgoing == closing == Decimal("0"):
            continue

        unit_price = _to_decimal(material.price)
        closing_amount = closing * unit_price
        min_stock = _to_decimal(material.min_stock)
        min_stock_mark = "Да" if closing <= min_stock else "Нет"

        rows.append(
            {
                "Период": period,
                "Место хранения": location_name,
                "Код материала": material.code,
                "Наименование материала": material.name,
                "Ед. изм.": material.unit,
                "Остаток на начало": _to_float(opening),
                "Поступило за период": _to_float(incoming),
                "Израсходовано за период": _to_float(outgoing),
                "Остаток на конец": _to_float(closing),
                "Цена за единицу": _to_float(unit_price),
                "Сумма остатка": _to_float(closing_amount),
                "Мин. остаток": _to_float(min_stock),
                "Минимальный остаток достигнут": min_stock_mark,
            }
        )
        total_opening += opening
        total_incoming += incoming
        total_outgoing += outgoing
        total_closing += closing
        total_closing_amount += closing_amount

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Место хранения": "",
                "Код материала": "",
                "Наименование материала": "ИТОГО ПО ОСТАТКАМ",
                "Ед. изм.": "",
                "Остаток на начало": _to_float(total_opening),
                "Поступило за период": _to_float(total_incoming),
                "Израсходовано за период": _to_float(total_outgoing),
                "Остаток на конец": _to_float(total_closing),
                "Цена за единицу": "",
                "Сумма остатка": _to_float(total_closing_amount),
                "Мин. остаток": "",
                "Минимальный остаток достигнут": "",
            }
        )
    return rows


def report_purchases(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    qs = StockReceiptLine.objects.select_related(
        "material",
        "receipt__supplier",
        "receipt__supplier_document",
        "receipt__supplier_document__supply_contract",
        "receipt__supplier_document__supply_contract__related_smr_contract__object",
        "receipt__supplier_document__request",
    ).filter(receipt__receipt_date__range=(date_from, date_to))

    user_role = getattr(user, "role", None)
    if user_role == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        if site_name:
            qs = qs.filter(
                Q(receipt__supplier_document__request__site_name__iexact=site_name) | Q(receipt__created_by=user)
            )
        else:
            qs = qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        qs = qs.filter(receipt__status__in=ACCOUNTING_VISIBLE_STATUSES)

    material_code = (filters.get("material_code") or "").strip()
    supplier_name = (filters.get("supplier_name") or "").strip()
    contract_number = (filters.get("contract_number") or "").strip()
    object_name = (filters.get("object_name") or "").strip()
    if material_code:
        qs = qs.filter(material__code__icontains=material_code)
    if supplier_name:
        qs = qs.filter(receipt__supplier__name__icontains=supplier_name)
    if contract_number:
        qs = qs.filter(receipt__supplier_document__supply_contract__number__icontains=contract_number)
    if object_name:
        qs = qs.filter(receipt__supplier_document__supply_contract__related_smr_contract__object__name__icontains=object_name)

    rows: list[dict[str, Any]] = []
    total_quantity = Decimal("0")
    total_amount = Decimal("0")
    for line in qs.order_by("-receipt__receipt_date", "material__code", "id"):
        supplier_document = line.receipt.supplier_document
        supply_contract = supplier_document.supply_contract if supplier_document else None
        quantity = _to_decimal(line.quantity)
        unit_price = _to_decimal(line.unit_price)
        position_amount = quantity * unit_price
        rows.append(
            {
                "Период": period,
                "Дата поступления": line.receipt.receipt_date.isoformat(),
                "Код материала": line.material.code,
                "Наименование материала": line.material.name,
                "Ед. изм.": line.material.unit,
                "Поставщик": line.receipt.supplier.name,
                "Договор поставки №": supply_contract.number if supply_contract else "-",
                "Дата договора поставки": supply_contract.contract_date.isoformat() if supply_contract else "-",
                "Документ поставки №": supplier_document.doc_number if supplier_document else "-",
                "Дата документа поставки": supplier_document.doc_date.isoformat() if supplier_document else "-",
                "Количество": _to_float(quantity),
                "Цена за единицу": _to_float(unit_price),
                "Сумма по позиции": _to_float(position_amount),
            }
        )
        total_quantity += quantity
        total_amount += position_amount

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Дата поступления": "",
                "Код материала": "",
                "Наименование материала": "ИТОГО ПО ЗАКУПКАМ",
                "Ед. изм.": "",
                "Поставщик": "",
                "Договор поставки №": "",
                "Дата договора поставки": "",
                "Документ поставки №": "",
                "Дата документа поставки": "",
                "Количество": _to_float(total_quantity),
                "Цена за единицу": "",
                "Сумма по позиции": _to_float(total_amount),
            }
        )
    return rows


def report_writeoffs(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    qs = WriteOffLine.objects.select_related("act__contract__object", "material").filter(act__act_date__range=(date_from, date_to))

    user_role = getattr(user, "role", None)
    if user_role == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        qs = qs.filter(act__site_name__iexact=site_name) if site_name else qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        qs = qs.filter(act__status__in=ACCOUNTING_VISIBLE_STATUSES)

    qs = _apply_contract_filters(qs, filters, contract_field="act__contract__number")
    material_code = (filters.get("material_code") or "").strip()
    if material_code:
        qs = qs.filter(material__code__icontains=material_code)
    location_name = (filters.get("location_name") or "").strip()
    if location_name:
        qs = qs.filter(act__site_name__icontains=location_name)

    lines = list(qs.order_by("-act__act_date", "act__contract__object__name", "material__code"))
    object_totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for line in lines:
        object_name = line.act.contract.object.name if line.act.contract.object else "Без объекта"
        object_totals[object_name] += _to_decimal(line.actual_quantity)

    rows: list[dict[str, Any]] = []
    total_quantity = Decimal("0")
    total_amount = Decimal("0")
    for line in lines:
        object_name = line.act.contract.object.name if line.act.contract.object else "Без объекта"
        actual_quantity = _to_decimal(line.actual_quantity)
        calculated_quantity = _to_decimal(line.calculated_quantity)
        unit_price = _to_decimal(line.unit_price)
        line_amount = actual_quantity * unit_price
        rows.append(
            {
                "Период": period,
                "Объект строительства": object_name,
                "Договор СМР №": line.act.contract.number,
                "Участок": line.act.site_name,
                "Код материала": line.material.code,
                "Наименование материала": line.material.name,
                "Ед. изм.": line.material.unit,
                "Вид работ": line.act.work_type,
                "Норма расхода на единицу": _to_float(line.norm_per_unit),
                "Расчетное количество": _to_float(calculated_quantity),
                "Фактическое списание": _to_float(actual_quantity),
                "Цена за единицу": _to_float(unit_price),
                "Сумма списания": _to_float(line_amount),
                "Акт списания №": line.act.number,
                "Дата акта": line.act.act_date.isoformat(),
                "Итого по объекту (кол-во)": _to_float(object_totals[object_name]),
            }
        )
        total_quantity += actual_quantity
        total_amount += line_amount

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Объект строительства": "",
                "Договор СМР №": "",
                "Участок": "",
                "Код материала": "",
                "Наименование материала": "ИТОГО ПО СПИСАНИЯМ",
                "Ед. изм.": "",
                "Вид работ": "",
                "Норма расхода на единицу": "",
                "Расчетное количество": "",
                "Фактическое списание": _to_float(total_quantity),
                "Цена за единицу": "",
                "Сумма списания": _to_float(total_amount),
                "Акт списания №": "",
                "Дата акта": "",
                "Итого по объекту (кол-во)": "",
            }
        )
    return rows


def report_work(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    qs = WorkLog.objects.select_related("contract__object", "created_by").filter(
        Q(actual_date__range=(date_from, date_to)) | Q(plan_date__range=(date_from, date_to))
    )
    if getattr(user, "role", None) == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        qs = qs.filter(site_name__iexact=site_name) if site_name else qs.none()

    location_name = (filters.get("location_name") or "").strip()
    object_name = (filters.get("object_name") or "").strip()
    contract_number = (filters.get("contract_number") or "").strip()
    if location_name:
        qs = qs.filter(site_name__icontains=location_name)
    if object_name:
        qs = qs.filter(contract__object__name__icontains=object_name)
    if contract_number:
        qs = qs.filter(contract__number__icontains=contract_number)

    rows: list[dict[str, Any]] = []
    total_planned = Decimal("0")
    total_actual = Decimal("0")
    delayed_count = 0
    overdue_or_not_done_count = 0
    in_time_count = 0
    today_value = timezone.localdate()
    for log in qs.order_by("-actual_date", "-plan_date", "site_name", "id"):
        planned_volume = _to_decimal(log.planned_volume)
        actual_volume = _to_decimal(log.actual_volume)
        deviation_days = _deviation_days(plan_date=log.plan_date, actual_date=log.actual_date, fallback_date=today_value)
        status = _execution_status(plan_date=log.plan_date, actual_date=log.actual_date)
        if status == "С опозданием":
            delayed_count += 1
        elif status == "Не выполнено":
            overdue_or_not_done_count += 1
        else:
            in_time_count += 1

        rows.append(
            {
                "Период": period,
                "Начальник участка": log.created_by.full_name_or_username,
                "Участок": log.site_name,
                "Объект строительства": log.contract.object.name if log.contract and log.contract.object else "",
                "Договор СМР №": log.contract.number if log.contract else "",
                "Вид работ": log.work_type,
                "Плановый объем": _to_float(planned_volume),
                "Фактический объем": _to_float(actual_volume),
                "Ед. изм.": log.volume_unit,
                "Плановая дата": log.plan_date.isoformat() if log.plan_date else "-",
                "Фактическая дата": log.actual_date.isoformat() if log.actual_date else "-",
                "Отклонение (дн.)": deviation_days,
                "Статус выполнения": status,
            }
        )
        total_planned += planned_volume
        total_actual += actual_volume

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Начальник участка": "",
                "Участок": "",
                "Объект строительства": "",
                "Договор СМР №": "",
                "Вид работ": "ИТОГО ПО РАБОТЕ УЧАСТКОВ",
                "Плановый объем": _to_float(total_planned),
                "Фактический объем": _to_float(total_actual),
                "Ед. изм.": "",
                "Плановая дата": "",
                "Фактическая дата": "",
                "Отклонение (дн.)": "",
                "Статус выполнения": f"В срок: {in_time_count}; С опозданием: {delayed_count}; Не выполнено: {overdue_or_not_done_count}",
            }
        )
    return rows


def _summary_row(
    *,
    period: str,
    section: str,
    metric: str,
    counterparty_or_object: str = "",
    document_ref: str = "",
    quantity: Any = "",
    amount: Any = "",
    status_or_note: str = "",
) -> dict[str, Any]:
    return {
        "Период": period,
        "Раздел": section,
        "Показатель": metric,
        "Контрагент/Объект": counterparty_or_object,
        "Документ": document_ref,
        "Количество": quantity,
        "Сумма": amount,
        "Статус/Комментарий": status_or_note,
    }


def report_summary_scoped_v2(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    user_role = getattr(user, "role", None)
    site_name = _user_site_name(user)
    rows: list[dict[str, Any]] = []

    grand_total_amount = Decimal("0")

    # 1. Действующие договоры СМР.
    contracts_qs = SMRContract.objects.select_related("object").filter(contract_date__lte=date_to).filter(
        Q(end_date__isnull=True) | Q(end_date__gte=date_from)
    )
    if user_role == RoleChoices.SITE_MANAGER:
        if site_name:
            contracts_qs = contracts_qs.filter(
                Q(created_by=user)
                | Q(procurement_requests__site_name__iexact=site_name)
                | Q(stock_issues__site_name__iexact=site_name)
                | Q(write_off_acts__site_name__iexact=site_name)
                | Q(work_logs__site_name__iexact=site_name)
            ).distinct()
        else:
            contracts_qs = contracts_qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        contracts_qs = contracts_qs.filter(status__in=ACCOUNTING_VISIBLE_STATUSES)

    contract_number = (filters.get("contract_number") or "").strip()
    object_name = (filters.get("object_name") or "").strip()
    if contract_number:
        contracts_qs = contracts_qs.filter(number__icontains=contract_number)
    if object_name:
        contracts_qs = contracts_qs.filter(object__name__icontains=object_name)

    section_total = Decimal("0")
    for contract in contracts_qs.order_by("number"):
        amount = _to_decimal(contract.amount)
        rows.append(
            _summary_row(
                period=period,
                section="Договоры СМР",
                metric="Действующий договор",
                counterparty_or_object=contract.object.name if contract.object else "",
                document_ref=contract.number,
                quantity="",
                amount=_to_float(amount),
                status_or_note=f"Заказчик: {contract.customer_name}; Статус: {_status_label(contract.status)}",
            )
        )
        section_total += amount
    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Договоры СМР",
            metric="Итого по разделу",
            amount=_to_float(section_total),
        )
    )
    grand_total_amount += section_total

    # 2. Поступление материалов за период (по позициям прихода).
    receipts_qs = StockReceiptLine.objects.select_related("material", "receipt__supplier", "receipt__supplier_document__request").filter(
        receipt__receipt_date__range=(date_from, date_to)
    )
    if user_role == RoleChoices.SITE_MANAGER:
        if site_name:
            receipts_qs = receipts_qs.filter(
                Q(receipt__supplier_document__request__site_name__iexact=site_name) | Q(receipt__created_by=user)
            )
        else:
            receipts_qs = receipts_qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        receipts_qs = receipts_qs.filter(receipt__status__in=ACCOUNTING_VISIBLE_STATUSES)

    supplier_name = (filters.get("supplier_name") or "").strip()
    material_code = (filters.get("material_code") or "").strip()
    if supplier_name:
        receipts_qs = receipts_qs.filter(receipt__supplier__name__icontains=supplier_name)
    if material_code:
        receipts_qs = receipts_qs.filter(material__code__icontains=material_code)

    grouped_receipts: dict[tuple[str, str], dict[str, Decimal | str]] = defaultdict(
        lambda: {"quantity": Decimal("0"), "amount": Decimal("0"), "unit": ""}
    )
    for line in receipts_qs:
        key = (line.receipt.supplier.name, f"{line.material.code} {line.material.name}")
        grouped_receipts[key]["quantity"] += _to_decimal(line.quantity)
        grouped_receipts[key]["amount"] += _to_decimal(line.quantity) * _to_decimal(line.unit_price)
        grouped_receipts[key]["unit"] = line.material.unit

    section_total = Decimal("0")
    for (supplier, material_label), item in sorted(grouped_receipts.items()):
        amount = _to_decimal(item["amount"])
        rows.append(
            _summary_row(
                period=period,
                section="Поступление материалов",
                metric=material_label,
                counterparty_or_object=supplier,
                quantity=_to_float(item["quantity"]),
                amount=_to_float(amount),
                status_or_note=f"Ед. изм.: {item['unit']}",
            )
        )
        section_total += amount
    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Поступление материалов",
            metric="Итого по разделу",
            amount=_to_float(section_total),
        )
    )
    grand_total_amount += section_total

    # 3. Движение материалов (приход/расход/остаток).
    warehouse_or_site = site_name if user_role == RoleChoices.SITE_MANAGER and site_name else settings.WAREHOUSE_NAME
    movement_qs = StockMovement.objects.select_related("material").filter(location_name=warehouse_or_site, movement_date__lte=date_to)
    if material_code:
        movement_qs = movement_qs.filter(material__code__icontains=material_code)

    movement_stats: dict[int, dict[str, Decimal]] = defaultdict(
        lambda: {"opening": Decimal("0"), "incoming": Decimal("0"), "outgoing": Decimal("0"), "closing": Decimal("0")}
    )
    movement_materials: dict[int, Material] = {}
    for movement in movement_qs:
        movement_materials[movement.material_id] = movement.material
        quantity = _to_decimal(movement.quantity_delta)
        movement_stats[movement.material_id]["closing"] += quantity
        if movement.movement_date < date_from:
            movement_stats[movement.material_id]["opening"] += quantity
        elif quantity >= 0:
            movement_stats[movement.material_id]["incoming"] += quantity
        else:
            movement_stats[movement.material_id]["outgoing"] += abs(quantity)

    section_total = Decimal("0")
    for material_id in sorted(movement_stats, key=lambda item: movement_materials[item].code):
        stats = movement_stats[material_id]
        material = movement_materials[material_id]
        closing = stats["closing"]
        closing_amount = closing * _to_decimal(material.price)
        rows.append(
            _summary_row(
                period=period,
                section="Движение материалов",
                metric=f"{material.code} {material.name}",
                counterparty_or_object=warehouse_or_site,
                quantity=_to_float(closing),
                amount=_to_float(closing_amount),
                status_or_note=f"Приход: {_to_float(stats['incoming'])}; Расход: {_to_float(stats['outgoing'])}; Остаток на начало: {_to_float(stats['opening'])}",
            )
        )
        section_total += closing_amount
    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Движение материалов",
            metric="Итого по разделу",
            amount=_to_float(section_total),
        )
    )
    grand_total_amount += section_total

    # 4. Списание материалов по объектам.
    writeoff_qs = WriteOffLine.objects.select_related("act__contract__object").filter(act__act_date__range=(date_from, date_to))
    if user_role == RoleChoices.SITE_MANAGER:
        writeoff_qs = writeoff_qs.filter(act__site_name__iexact=site_name) if site_name else writeoff_qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        writeoff_qs = writeoff_qs.filter(act__status__in=ACCOUNTING_VISIBLE_STATUSES)
    if object_name:
        writeoff_qs = writeoff_qs.filter(act__contract__object__name__icontains=object_name)
    if contract_number:
        writeoff_qs = writeoff_qs.filter(act__contract__number__icontains=contract_number)
    if material_code:
        writeoff_qs = writeoff_qs.filter(material__code__icontains=material_code)

    grouped_writeoffs: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"quantity": Decimal("0"), "amount": Decimal("0")})
    for line in writeoff_qs:
        object_label = line.act.contract.object.name if line.act.contract.object else "Без объекта"
        grouped_writeoffs[object_label]["quantity"] += _to_decimal(line.actual_quantity)
        grouped_writeoffs[object_label]["amount"] += _to_decimal(line.actual_quantity) * _to_decimal(line.unit_price)

    section_total = Decimal("0")
    for object_label, item in sorted(grouped_writeoffs.items()):
        amount = _to_decimal(item["amount"])
        rows.append(
            _summary_row(
                period=period,
                section="Списание материалов",
                metric="Списание по объекту",
                counterparty_or_object=object_label,
                quantity=_to_float(item["quantity"]),
                amount=_to_float(amount),
                status_or_note="По актам списания",
            )
        )
        section_total += amount
    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Списание материалов",
            metric="Итого по разделу",
            amount=_to_float(section_total),
        )
    )
    grand_total_amount += section_total

    # 5. Закупки (поставщики и суммы по договорам поставки).
    purchases_qs = SupplierDocument.objects.select_related("supplier", "supply_contract", "request").filter(doc_date__range=(date_from, date_to))
    if user_role == RoleChoices.SITE_MANAGER:
        if site_name:
            purchases_qs = purchases_qs.filter(Q(request__site_name__iexact=site_name) | Q(uploaded_by=user))
        else:
            purchases_qs = purchases_qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        purchases_qs = purchases_qs.filter(status__in=ACCOUNTING_VISIBLE_STATUSES)
    if supplier_name:
        purchases_qs = purchases_qs.filter(supplier__name__icontains=supplier_name)
    if contract_number:
        purchases_qs = purchases_qs.filter(supply_contract__number__icontains=contract_number)
    if object_name:
        purchases_qs = purchases_qs.filter(supply_contract__related_smr_contract__object__name__icontains=object_name)

    grouped_purchases: dict[tuple[str, str], Decimal] = defaultdict(lambda: Decimal("0"))
    for document in purchases_qs:
        supplier_label = document.supplier.name
        contract_label = document.supply_contract.number if document.supply_contract else "-"
        grouped_purchases[(supplier_label, contract_label)] += _to_decimal(document.amount)

    section_total = Decimal("0")
    for (supplier_label, contract_label), amount in sorted(grouped_purchases.items()):
        rows.append(
            _summary_row(
                period=period,
                section="Закупки",
                metric="Сумма по договору поставки",
                counterparty_or_object=supplier_label,
                document_ref=contract_label,
                amount=_to_float(amount),
                status_or_note="По документам поставщика",
            )
        )
        section_total += amount
    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Закупки",
            metric="Итого по разделу",
            amount=_to_float(section_total),
        )
    )
    grand_total_amount += section_total

    # 6. Выданная спецодежда.
    ppe_qs = PPEIssuanceLine.objects.select_related("issuance", "material").filter(issuance__issue_date__range=(date_from, date_to))
    if user_role == RoleChoices.SITE_MANAGER:
        ppe_qs = ppe_qs.filter(issuance__site_name__iexact=site_name) if site_name else ppe_qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        ppe_qs = ppe_qs.filter(issuance__status__in=ACCOUNTING_VISIBLE_STATUSES)
    location_name = (filters.get("location_name") or "").strip()
    if location_name:
        ppe_qs = ppe_qs.filter(issuance__site_name__icontains=location_name)

    grouped_ppe: dict[str, dict[str, Decimal]] = defaultdict(lambda: {"quantity": Decimal("0"), "amount": Decimal("0")})
    for line in ppe_qs:
        key = f"{line.material.code} {line.material.name}"
        quantity = _to_decimal(line.quantity)
        grouped_ppe[key]["quantity"] += quantity
        grouped_ppe[key]["amount"] += quantity * _to_decimal(line.material.price)

    section_total = Decimal("0")
    for material_label, item in sorted(grouped_ppe.items()):
        amount = _to_decimal(item["amount"])
        rows.append(
            _summary_row(
                period=period,
                section="Выданная спецодежда",
                metric=material_label,
                counterparty_or_object="СИЗ",
                quantity=_to_float(item["quantity"]),
                amount=_to_float(amount),
                status_or_note="По ведомостям выдачи",
            )
        )
        section_total += amount
    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Выданная спецодежда",
            metric="Итого по разделу",
            amount=_to_float(section_total),
        )
    )
    grand_total_amount += section_total

    rows.append(
        _summary_row(
            period="ИТОГО",
            section="Сводный отчет",
            metric="ИТОГОВАЯ СУММА ПО ВСЕМ СТАТЬЯМ",
            amount=_to_float(grand_total_amount),
            status_or_note="Для отражения в бухгалтерском учете",
        )
    )
    return rows


def report_summary(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    return report_summary_scoped_v2(filters, user=user)


def report_summary_scoped(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    return report_summary_scoped_v2(filters, user=user)


def report_ppe_scoped(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    if date_to < date_from:
        date_from, date_to = date_to, date_from
    period = _period_label(date_from, date_to)
    qs = PPEIssuanceLine.objects.select_related("issuance", "worker", "material").filter(
        issuance__issue_date__range=(date_from, date_to)
    )
    user_role = getattr(user, "role", None)
    if user_role == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        qs = qs.filter(issuance__site_name__iexact=site_name) if site_name else qs.none()
    elif user_role == RoleChoices.ACCOUNTING:
        qs = qs.filter(issuance__status__in=ACCOUNTING_VISIBLE_STATUSES)

    location_name = (filters.get("location_name") or "").strip()
    if location_name:
        qs = qs.filter(issuance__site_name__icontains=location_name)
    material_code = (filters.get("material_code") or "").strip()
    if material_code:
        qs = qs.filter(material__code__icontains=material_code)

    rows: list[dict[str, Any]] = []
    total_quantity = Decimal("0")
    ok_items = 0
    expired_items = 0
    expiring_items = 0
    replacement_required = 0
    for line in qs.order_by("-issuance__issue_date", "worker__full_name", "material__code"):
        due_date = line.replacement_due_date
        issue_start_date = line.replacement_start_date
        days_until_replacement = line.days_until_replacement
        quantity = _to_decimal(line.quantity)

        if line.replacement_status == line.REPLACEMENT_STATUS_EXPIRED:
            expired_items += 1
        elif line.replacement_status == line.REPLACEMENT_STATUS_EXPIRING:
            expiring_items += 1
        else:
            ok_items += 1

        if line.needs_replacement:
            replacement_required += 1

        rows.append(
            {
                "Период": period,
                "Дата выдачи": line.issuance.issue_date.isoformat(),
                "Дата начала эксплуатации": issue_start_date.isoformat() if issue_start_date else "",
                "Плановая дата замены": due_date.isoformat() if due_date else "",
                "Дней до замены": days_until_replacement if due_date and days_until_replacement is not None else "",
                "Статус срока": line.replacement_status_label,
                "Требуется замена": "Да" if line.needs_replacement else "Нет",
                "Предупреждение": line.replacement_warning,
                "Ведомость №": line.issuance.number,
                "Участок": line.issuance.site_name,
                "Табельный номер": line.worker.employee_number,
                "ФИО работника": line.worker.full_name,
                "Код СИЗ": line.material.code,
                "Наименование СИЗ": line.material.name,
                "Количество": _to_float(quantity),
                "Ед. изм.": line.material.unit,
                "Срок службы (мес.)": line.service_life_months,
            }
        )
        total_quantity += quantity

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Дата выдачи": "",
                "Дата начала эксплуатации": "",
                "Плановая дата замены": "",
                "Дней до замены": "",
                "Статус срока": f"В норме: {ok_items}; Истекает: {expiring_items}; Просрочено: {expired_items}",
                "Требуется замена": f"Да: {replacement_required}",
                "Предупреждение": "",
                "Ведомость №": "",
                "Участок": "",
                "Табельный номер": "",
                "ФИО работника": "",
                "Код СИЗ": "",
                "Наименование СИЗ": "ИТОГО ПО ВЫДАННОЙ СПЕЦОДЕЖДЕ",
                "Количество": _to_float(total_quantity),
                "Ед. изм.": "",
                "Срок службы (мес.)": "",
            }
        )
    return rows


def report_ppe(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    return report_ppe_scoped(filters, user=user)


def report_material_movements_scoped(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    qs = StockMovement.objects.select_related("material", "created_by").filter(movement_date__range=(date_from, date_to))
    if getattr(user, "role", None) == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        qs = qs.filter(location_name__iexact=site_name) if site_name else qs.none()

    qs = _apply_movement_filters(qs, filters)
    rows: list[dict[str, Any]] = []
    total_incoming = Decimal("0")
    total_outgoing = Decimal("0")
    total_amount = Decimal("0")
    for movement in qs.order_by("-movement_date", "-id"):
        quantity = _to_decimal(movement.quantity_delta)
        amount = quantity * _to_decimal(movement.unit_price)
        if quantity >= 0:
            total_incoming += quantity
        else:
            total_outgoing += abs(quantity)
        total_amount += amount
        rows.append(
            {
                "Период": period,
                "Дата операции": movement.movement_date.isoformat(),
                "Код материала": movement.material.code,
                "Наименование материала": movement.material.name,
                "Место хранения": movement.location_name,
                "Тип операции": _movement_label(movement.source_type),
                "Количество (+/-)": _to_float(quantity),
                "Ед. изм.": movement.material.unit,
                "Цена за единицу": _to_float(movement.unit_price),
                "Сумма операции": _to_float(amount),
                "Ответственный": movement.created_by.full_name_or_username,
            }
        )

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Дата операции": "",
                "Код материала": "",
                "Наименование материала": "ИТОГО ПО ДВИЖЕНИЮ МАТЕРИАЛОВ",
                "Место хранения": "",
                "Тип операции": f"Приход: {_to_float(total_incoming)}; Расход: {_to_float(total_outgoing)}",
                "Количество (+/-)": _to_float(total_incoming - total_outgoing),
                "Ед. изм.": "",
                "Цена за единицу": "",
                "Сумма операции": _to_float(total_amount),
                "Ответственный": "",
            }
        )
    return rows


def report_material_movements(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    return report_material_movements_scoped(filters, user=user)


def report_material_consumption_scoped(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    qs = StockMovement.objects.select_related("material").filter(
        movement_date__range=(date_from, date_to),
        quantity_delta__lt=0,
        source_type__in=["write_off", "ppe_issuance"],
    )
    if getattr(user, "role", None) == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        qs = qs.filter(location_name__iexact=site_name) if site_name else qs.none()
    qs = _apply_movement_filters(qs, filters)

    grouped: dict[tuple[str, int], dict[str, Any]] = defaultdict(
        lambda: {
            "quantity": Decimal("0"),
            "amount": Decimal("0"),
            "operations": 0,
            "unit_price_sum": Decimal("0"),
            "material": None,
        }
    )
    for movement in qs:
        key = (movement.location_name, movement.material_id)
        quantity = abs(_to_decimal(movement.quantity_delta))
        amount = quantity * _to_decimal(movement.unit_price)
        grouped[key]["quantity"] += quantity
        grouped[key]["amount"] += amount
        grouped[key]["operations"] += 1
        grouped[key]["unit_price_sum"] += _to_decimal(movement.unit_price)
        grouped[key]["material"] = movement.material

    total_quantity = sum((_to_decimal(item["quantity"]) for item in grouped.values()), Decimal("0"))
    total_amount = sum((_to_decimal(item["amount"]) for item in grouped.values()), Decimal("0"))

    rows: list[dict[str, Any]] = []
    for (location_name, _material_id), item in sorted(grouped.items(), key=lambda row: (row[0][0], row[1]["material"].code)):
        material = item["material"]
        quantity = _to_decimal(item["quantity"])
        amount = _to_decimal(item["amount"])
        average_price = Decimal("0")
        if item["operations"]:
            average_price = _to_decimal(item["unit_price_sum"]) / Decimal(item["operations"])
        share_percent = Decimal("0")
        if total_amount > 0:
            share_percent = (amount / total_amount) * Decimal("100")
        rows.append(
            {
                "Период": period,
                "Участок/склад": location_name,
                "Код материала": material.code,
                "Наименование материала": material.name,
                "Ед. изм.": material.unit,
                "Расход (кол-во)": _to_float(quantity),
                "Средняя цена": _to_float(average_price),
                "Сумма расхода": _to_float(amount),
                "Доля в общем расходе (%)": _to_float(share_percent.quantize(Decimal("0.01"))),
                "Количество операций": item["operations"],
            }
        )

    if rows:
        rows.append(
            {
                "Период": "ИТОГО",
                "Участок/склад": "",
                "Код материала": "",
                "Наименование материала": "ИТОГО ПО РАСХОДУ МАТЕРИАЛОВ",
                "Ед. изм.": "",
                "Расход (кол-во)": _to_float(total_quantity),
                "Средняя цена": "",
                "Сумма расхода": _to_float(total_amount),
                "Доля в общем расходе (%)": 100.0 if total_amount > 0 else 0.0,
                "Количество операций": sum(item["operations"] for item in grouped.values()),
            }
        )
    return rows


def report_material_consumption(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    return report_material_consumption_scoped(filters, user=user)


def report_work_statistics_scoped(filters: dict[str, Any], *, user=None) -> list[dict[str, Any]]:
    date_from, date_to = report_period(filters)
    period = _period_label(date_from, date_to)
    qs = WorkLog.objects.select_related("contract__object").filter(
        Q(actual_date__range=(date_from, date_to)) | Q(plan_date__range=(date_from, date_to))
    )
    if getattr(user, "role", None) == RoleChoices.SITE_MANAGER:
        site_name = _user_site_name(user)
        qs = qs.filter(site_name__iexact=site_name) if site_name else qs.none()

    location_name = (filters.get("location_name") or "").strip()
    object_name = (filters.get("object_name") or "").strip()
    contract_number = (filters.get("contract_number") or "").strip()
    if location_name:
        qs = qs.filter(site_name__icontains=location_name)
    if object_name:
        qs = qs.filter(contract__object__name__icontains=object_name)
    if contract_number:
        qs = qs.filter(contract__number__icontains=contract_number)

    grouped: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(
        lambda: {
            "entries": 0,
            "planned_volume": Decimal("0"),
            "actual_volume": Decimal("0"),
            "deviation_days": 0,
            "deviation_count": 0,
            "in_time": 0,
            "delayed": 0,
            "not_done": 0,
            "unit": "",
        }
    )
    today_value = timezone.localdate()
    for log in qs:
        object_label = log.contract.object.name if log.contract and log.contract.object else ""
        key = (log.site_name, object_label, log.work_type)
        item = grouped[key]
        item["entries"] += 1
        item["planned_volume"] += _to_decimal(log.planned_volume)
        item["actual_volume"] += _to_decimal(log.actual_volume)
        item["unit"] = item["unit"] or log.volume_unit
        status = _execution_status(plan_date=log.plan_date, actual_date=log.actual_date)
        if status == "В срок":
            item["in_time"] += 1
        elif status == "С опозданием":
            item["delayed"] += 1
        else:
            item["not_done"] += 1
        if log.plan_date:
            item["deviation_days"] += _deviation_days(plan_date=log.plan_date, actual_date=log.actual_date, fallback_date=today_value)
            item["deviation_count"] += 1

    total_entries = 0
    total_planned = Decimal("0")
    total_actual = Decimal("0")
    total_in_time = 0
    total_delayed = 0
    total_not_done = 0
    total_deviation_days = 0
    total_deviation_count = 0

    rows: list[dict[str, Any]] = []
    for (site_label, object_label, work_type), item in sorted(grouped.items()):
        planned_volume = _to_decimal(item["planned_volume"])
        actual_volume = _to_decimal(item["actual_volume"])
        completion_percent = Decimal("0")
        if planned_volume > 0:
            completion_percent = (actual_volume / planned_volume) * Decimal("100")
        average_deviation = Decimal("0")
        if item["deviation_count"]:
            average_deviation = Decimal(item["deviation_days"]) / Decimal(item["deviation_count"])

        rows.append(
            {
                "Период": period,
                "Участок": site_label,
                "Объект строительства": object_label,
                "Вид работ": work_type,
                "Количество записей": item["entries"],
                "Плановый объем": _to_float(planned_volume),
                "Фактический объем": _to_float(actual_volume),
                "Ед. изм.": item["unit"],
                "Выполнение (%)": _to_float(completion_percent.quantize(Decimal("0.01"))),
                "Среднее отклонение (дн.)": _to_float(average_deviation.quantize(Decimal("0.01"))),
                "Выполнено в срок": item["in_time"],
                "С опозданием": item["delayed"],
                "Не выполнено": item["not_done"],
            }
        )
        total_entries += item["entries"]
        total_planned += planned_volume
        total_actual += actual_volume
        total_in_time += item["in_time"]
        total_delayed += item["delayed"]
        total_not_done += item["not_done"]
        total_deviation_days += item["deviation_days"]
        total_deviation_count += item["deviation_count"]

    if rows:
        total_completion = Decimal("0")
        if total_planned > 0:
            total_completion = (total_actual / total_planned) * Decimal("100")
        total_average_deviation = Decimal("0")
        if total_deviation_count:
            total_average_deviation = Decimal(total_deviation_days) / Decimal(total_deviation_count)
        rows.append(
            {
                "Период": "ИТОГО",
                "Участок": "",
                "Объект строительства": "",
                "Вид работ": "ИТОГО ПО СТАТИСТИКЕ РАБОТ",
                "Количество записей": total_entries,
                "Плановый объем": _to_float(total_planned),
                "Фактический объем": _to_float(total_actual),
                "Ед. изм.": "",
                "Выполнение (%)": _to_float(total_completion.quantize(Decimal("0.01"))),
                "Среднее отклонение (дн.)": _to_float(total_average_deviation.quantize(Decimal("0.01"))),
                "Выполнено в срок": total_in_time,
                "С опозданием": total_delayed,
                "Не выполнено": total_not_done,
            }
        )
    return rows


REPORT_PROVIDERS = {
    "stock": report_stock,
    "purchases": report_purchases,
    "writeoffs": report_writeoffs,
    "work": report_work,
    "summary": report_summary_scoped_v2,
    "ppe": report_ppe_scoped,
    "movements": report_material_movements_scoped,
    "consumption": report_material_consumption_scoped,
    "work_stats": report_work_statistics_scoped,
}
