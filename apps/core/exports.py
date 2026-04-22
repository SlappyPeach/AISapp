from __future__ import annotations

import mimetypes
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable

from django.conf import settings

from .models import (
    DocumentRecord,
    PPEIssuance,
    PrimaryDocument,
    ProcurementRequest,
    SMRContract,
    StockIssue,
    StockReceipt,
    SupplierDocument,
    SupplyContract,
    WriteOffAct,
)
from .reporting import REPORT_PROVIDERS


def money(value: Any) -> str:
    numeric = float(value or 0)
    return f"{numeric:,.2f}".replace(",", " ").replace(".", ",")


def _load_xlsxwriter():
    try:
        import xlsxwriter
    except ImportError as exc:
        raise RuntimeError("Экспорт XLSX недоступен: пакет xlsxwriter не установлен или поврежден.") from exc
    return xlsxwriter


def _load_docx_dependencies():
    try:
        from docx import Document
        from docx.enum.text import WD_ALIGN_PARAGRAPH
        from docx.shared import Pt
    except ImportError as exc:
        raise RuntimeError("Экспорт DOCX недоступен: пакет python-docx или lxml не установлен либо поврежден.") from exc
    return Document, WD_ALIGN_PARAGRAPH, Pt


class Exporter:
    def content_type(self, path: Path) -> str:
        guessed, _ = mimetypes.guess_type(path.name)
        return guessed or "application/octet-stream"

    def export_document(self, entity_type: str, entity_id: int) -> Path:
        handlers = {
            "smr_contract": self._export_smr_contract,
            "supply_contract": self._export_supply_contract,
            "procurement_request": self._export_procurement_request,
            "primary_document": self._export_primary_document,
            "stock_receipt": self._export_stock_receipt,
            "stock_issue": self._export_stock_issue,
            "write_off": self._export_writeoff,
            "ppe_issuance": self._export_ppe_issuance,
            "supplier_document": self._export_supplier_document,
        }
        if entity_type not in handlers:
            raise ValueError("Для этого типа документа выгрузка не реализована.")
        path = handlers[entity_type](entity_id)
        DocumentRecord.objects.filter(entity_type=entity_type, entity_id=entity_id).update(file_path=str(path))
        return path

    def export_report(self, report_name: str, filters: dict[str, Any], *, user=None) -> Path:
        provider = REPORT_PROVIDERS[report_name]
        rows = provider(filters, user=user)
        date_from = filters.get("date_from") or datetime.now().date().replace(day=1)
        date_to = filters.get("date_to") or datetime.now().date()
        path = settings.EXPORTS_DIR / f"{report_name}_{date_from}_{date_to}.xlsx"

        xlsxwriter = _load_xlsxwriter()
        workbook = xlsxwriter.Workbook(str(path))
        worksheet = workbook.add_worksheet("Отчет")
        title_format = workbook.add_format({"bold": True, "font_size": 14})
        header_format = workbook.add_format({"bold": True, "bg_color": "#E6D7C6", "border": 1})
        cell_format = workbook.add_format({"border": 1})
        numeric_format = workbook.add_format({"border": 1, "num_format": "#,##0.00"})
        total_cell_format = workbook.add_format({"border": 1, "bold": True, "bg_color": "#F3E7D7"})
        total_numeric_format = workbook.add_format({"border": 1, "bold": True, "bg_color": "#F3E7D7", "num_format": "#,##0.00"})

        worksheet.write("A1", "Экспорт отчета АИС", title_format)
        worksheet.write("A2", f"Период: {date_from} - {date_to}")
        worksheet.freeze_panes(4, 0)

        if rows:
            headers = list(rows[0].keys())
            for col_index, header in enumerate(headers):
                worksheet.write(3, col_index, header, header_format)
            for row_index, row in enumerate(rows, start=4):
                first_header = headers[0]
                first_value = row.get(first_header, "")
                is_total_row = str(first_value).startswith("ИТОГО")
                for col_index, header in enumerate(headers):
                    value = row.get(header)
                    if isinstance(value, Decimal):
                        value = float(value)
                    if isinstance(value, (int, float)):
                        worksheet.write_number(
                            row_index,
                            col_index,
                            float(value),
                            total_numeric_format if is_total_row else numeric_format,
                        )
                    else:
                        worksheet.write(
                            row_index,
                            col_index,
                            value,
                            total_cell_format if is_total_row else cell_format,
                        )
            for col_index, header in enumerate(headers):
                max_len = max(len(str(header)), *(len(str(item.get(header, ""))) for item in rows))
                worksheet.set_column(col_index, col_index, min(max_len + 2, 45))
            worksheet.autofilter(3, 0, 3 + len(rows), len(headers) - 1)
        else:
            worksheet.write("A4", "Нет данных за выбранный период.")

        workbook.close()
        return path

    def _doc_path(self, prefix: str, number: str) -> Path:
        safe_number = number.replace("/", "_").replace("\\", "_").replace(" ", "_")
        return settings.EXPORTS_DIR / f"{prefix}_{safe_number}.docx"

    def _prepare_doc(self, title: str, subtitle: str = ""):
        Document, WD_ALIGN_PARAGRAPH, Pt = _load_docx_dependencies()
        document = Document()
        style = document.styles["Normal"]
        style.font.name = "Times New Roman"
        style.font.size = Pt(11)

        title_paragraph = document.add_paragraph()
        title_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        title_run = title_paragraph.add_run(title)
        title_run.bold = True
        title_run.font.size = Pt(14)

        if subtitle:
            subtitle_paragraph = document.add_paragraph()
            subtitle_paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            subtitle_paragraph.add_run(subtitle).italic = True

        document.add_paragraph()
        return document

    def _add_meta(self, document, items: Iterable[tuple[str, str]]) -> None:
        for label, value in items:
            paragraph = document.add_paragraph()
            paragraph.add_run(f"{label}: ").bold = True
            paragraph.add_run(value)

    def _add_table(self, document, headers: list[str], rows: list[list[str]]) -> None:
        table = document.add_table(rows=1, cols=len(headers))
        table.style = "Table Grid"
        for index, header in enumerate(headers):
            table.rows[0].cells[index].text = header
        for row in rows:
            cells = table.add_row().cells
            for index, value in enumerate(row):
                cells[index].text = value

    def _add_signature(self, document, left_label: str, right_label: str) -> None:
        document.add_paragraph()
        table = document.add_table(rows=1, cols=2)
        table.style = "Table Grid"
        table.cell(0, 0).text = f"{left_label}\n\n_____________________"
        table.cell(0, 1).text = f"{right_label}\n\n_____________________"

    def _organization_profile(self) -> dict[str, str]:
        profile = getattr(settings, "ORGANIZATION_PROFILE", {}) or {}
        if not isinstance(profile, dict):
            profile = {"name": str(profile)}
        return {
            "name": str(profile.get("name", "")).strip(),
            "tax_id": str(profile.get("tax_id", "")).strip(),
            "kpp": str(profile.get("kpp", "")).strip(),
            "ogrn": str(profile.get("ogrn", "")).strip(),
            "address": str(profile.get("address", "")).strip(),
            "bank_details": str(profile.get("bank_details", "")).strip(),
            "requisites": str(profile.get("requisites", "")).strip(),
        }

    def _organization_name(self) -> str:
        profile = self._organization_profile()
        return profile["name"]

    def _organization_requisites(self) -> str:
        profile = self._organization_profile()
        if profile["requisites"]:
            return profile["requisites"]

        parts: list[str] = []
        if profile["tax_id"]:
            parts.append(f"ИНН {profile['tax_id']}")
        if profile["kpp"]:
            parts.append(f"КПП {profile['kpp']}")
        if profile["ogrn"]:
            parts.append(f"ОГРН {profile['ogrn']}")
        if profile["address"]:
            parts.append(profile["address"])
        if profile["bank_details"]:
            parts.append(profile["bank_details"])
        return "; ".join(part for part in parts if part)

    def _supplier_requisites(self, supplier) -> str:
        if hasattr(supplier, "requisites_text"):
            return supplier.requisites_text()
        return ""

    def _export_smr_contract(self, entity_id: int) -> Path:
        contract = SMRContract.objects.select_related("object").get(pk=entity_id)
        customer_name = contract.resolved_customer_name() or "-"
        customer_requisites = contract.resolved_customer_requisites() or "-"
        contractor_name = contract.resolved_contractor_name() or "-"
        contractor_requisites = contract.resolved_contractor_requisites() or "-"
        doc = self._prepare_doc("ДОГОВОР НА ВЫПОЛНЕНИЕ СМР", f"№ {contract.number} от {contract.contract_date}")
        self._add_meta(
            doc,
            [
                ("Заказчик", customer_name),
                ("Реквизиты заказчика", customer_requisites),
                ("Подрядчик", contractor_name),
                ("Реквизиты подрядчика", contractor_requisites),
                ("Объект", contract.object.name if contract.object else ""),
                ("Предмет", contract.subject),
                ("Вид работ", contract.work_type or ""),
                ("Плановый объем", f"{contract.planned_volume or 0} {contract.volume_unit or ''}".strip()),
                ("Стоимость", f"{money(contract.amount)} руб."),
                ("Сроки", f"{contract.start_date or '-'} - {contract.end_date or '-'}"),
            ],
        )
        self._add_signature(doc, f"Заказчик: {customer_name}", f"Подрядчик: {contractor_name}")
        path = self._doc_path("smr_contract", contract.number)
        doc.save(path)
        return path

    def _export_supply_contract(self, entity_id: int) -> Path:
        contract = SupplyContract.objects.select_related("supplier", "related_smr_contract").get(pk=entity_id)
        buyer_name = self._organization_name() or "-"
        buyer_requisites = self._organization_requisites() or "-"
        supplier_requisites = self._supplier_requisites(contract.supplier) or "-"
        doc = self._prepare_doc("ДОГОВОР ПОСТАВКИ", f"№ {contract.number} от {contract.contract_date}")
        self._add_meta(
            doc,
            [
                ("Поставщик", contract.supplier.name),
                ("Реквизиты поставщика", supplier_requisites),
                ("Покупатель", buyer_name),
                ("Реквизиты покупателя", buyer_requisites),
                ("Связанный договор СМР", contract.related_smr_contract.number if contract.related_smr_contract else "-"),
                ("Сумма", f"{money(contract.amount)} руб."),
                ("Статус", contract.get_status_display()),
            ],
        )
        doc.add_paragraph(contract.terms or "Поставка материалов выполняется по заявкам снабженца через АИС.")
        self._add_signature(doc, f"Поставщик: {contract.supplier.name}", f"Покупатель: {buyer_name}")
        path = self._doc_path("supply_contract", contract.number)
        doc.save(path)
        return path

    def _export_procurement_request(self, entity_id: int) -> Path:
        request = ProcurementRequest.objects.select_related("contract", "supplier").prefetch_related("lines__material").get(pk=entity_id)
        doc = self._prepare_doc("ЗАЯВКА НА ЗАКУПКУ МАТЕРИАЛОВ", f"№ {request.number} от {request.request_date}")
        self._add_meta(
            doc,
            [
                ("Участок", request.site_name),
                ("Договор СМР", request.contract.number if request.contract else "-"),
                ("Поставщик", request.supplier.name if request.supplier else "-"),
                ("Статус", request.get_status_display()),
            ],
        )
        self._add_table(
            doc,
            ["Код", "Наименование", "Ед.", "Количество", "Цена", "Примечание"],
            [
                [
                    line.material.code,
                    line.material.name,
                    line.material.unit,
                    str(line.quantity),
                    money(line.unit_price),
                    line.notes or "",
                ]
                for line in request.lines.all()
            ],
        )
        self._add_signature(doc, "Начальник участка / снабженец", "Начальник монтажного объекта")
        path = self._doc_path("procurement_request", request.number)
        doc.save(path)
        return path

    def _export_primary_document(self, entity_id: int) -> Path:
        item = (
            PrimaryDocument.objects.select_related("document_type", "supplier", "procurement_request", "supply_contract", "stock_receipt")
            .prefetch_related("lines__material")
            .get(pk=entity_id)
        )
        receiver_name = self._organization_name() or "-"
        receiver_requisites = self._organization_requisites() or "-"
        supplier_requisites = self._supplier_requisites(item.supplier) or "-"
        title_map = {
            "invoice": "СЧЕТ НА ОПЛАТУ",
            "invoice_facture": "СЧЕТ-ФАКТУРА",
            "upd": "УПД",
            "vat_invoice": "СЧЕТ-ФАКТУРА",
            "goods_waybill": "ТОВАРНАЯ НАКЛАДНАЯ",
            "receipt_invoice": "ПРИХОДНАЯ НАКЛАДНАЯ",
        }
        doc = self._prepare_doc(title_map.get(item.document_type.code, item.document_type.name.upper()), f"№ {item.number} от {item.doc_date}")
        self._add_meta(
            doc,
            [
                ("Тип документа", item.document_type.name),
                ("Поставщик", item.supplier.name),
                ("Реквизиты поставщика", supplier_requisites),
                ("Получатель", receiver_name),
                ("Реквизиты получателя", receiver_requisites),
                ("Основание", item.basis_reference or "-"),
                ("Участок/склад", item.site_name or "-"),
                ("Сумма", f"{money(item.amount)} руб."),
                ("НДС", f"{money(item.vat_amount)} руб."),
                ("Статус", item.get_status_display()),
            ],
        )
        self._add_table(
            doc,
            ["Код", "Наименование", "Ед.", "Количество", "Цена", "Сумма", "Примечание"],
            [
                [
                    line.material.code,
                    line.material.name,
                    line.material.unit,
                    str(line.quantity),
                    money(line.unit_price),
                    money(line.quantity * line.unit_price),
                    line.notes or "",
                ]
                for line in item.lines.all()
            ],
        )
        self._add_signature(doc, f"Поставщик: {item.supplier.name}", f"Получатель: {receiver_name}")
        path = self._doc_path(item.document_type.code, item.number)
        doc.save(path)
        return path

    def _export_stock_receipt(self, entity_id: int) -> Path:
        receipt = StockReceipt.objects.select_related("supplier").prefetch_related("lines__material").get(pk=entity_id)
        doc = self._prepare_doc("ПРИХОДНЫЙ ОРДЕР", f"№ {receipt.number} от {receipt.receipt_date}")
        self._add_meta(
            doc,
            [
                ("Поставщик", receipt.supplier.name),
                ("Склад", settings.WAREHOUSE_NAME),
                ("Статус", receipt.get_status_display()),
            ],
        )
        self._add_table(
            doc,
            ["Код", "Наименование", "Ед.", "Количество", "Цена", "Сумма"],
            [
                [
                    line.material.code,
                    line.material.name,
                    line.material.unit,
                    str(line.quantity),
                    money(line.unit_price),
                    money(line.quantity * line.unit_price),
                ]
                for line in receipt.lines.all()
            ],
        )
        self._add_signature(doc, "Кладовщик", "Материально ответственное лицо")
        path = self._doc_path("stock_receipt", receipt.number)
        doc.save(path)
        return path

    def _export_stock_issue(self, entity_id: int) -> Path:
        issue = StockIssue.objects.select_related("contract").prefetch_related("lines__material").get(pk=entity_id)
        doc = self._prepare_doc("ТРЕБОВАНИЕ-НАКЛАДНАЯ", f"№ {issue.number} от {issue.issue_date}")
        self._add_meta(
            doc,
            [
                ("Участок", issue.site_name),
                ("Договор СМР", issue.contract.number if issue.contract else "-"),
                ("Получатель", issue.received_by_name),
                ("Статус", issue.get_status_display()),
            ],
        )
        self._add_table(
            doc,
            ["Код", "Наименование", "Ед.", "Количество", "Цена"],
            [
                [
                    line.material.code,
                    line.material.name,
                    line.material.unit,
                    str(line.quantity),
                    money(line.unit_price),
                ]
                for line in issue.lines.all()
            ],
        )
        self._add_signature(doc, "Кладовщик", "Начальник участка")
        path = self._doc_path("stock_issue", issue.number)
        doc.save(path)
        return path

    def _export_writeoff(self, entity_id: int) -> Path:
        act = WriteOffAct.objects.select_related("contract__object").prefetch_related("lines__material").get(pk=entity_id)
        doc = self._prepare_doc("АКТ СПИСАНИЯ МАТЕРИАЛОВ", f"№ {act.number} от {act.act_date}")
        self._add_meta(
            doc,
            [
                ("Договор", act.contract.number),
                ("Объект", act.contract.object.name if act.contract.object else ""),
                ("Участок", act.site_name),
                ("Вид работ", act.work_type),
                ("Объем работ", f"{act.work_volume} {act.volume_unit}".strip()),
                ("Статус", act.get_status_display()),
            ],
        )
        self._add_table(
            doc,
            ["Код", "Наименование", "Ед.", "Норма", "Расчет", "Факт"],
            [
                [
                    line.material.code,
                    line.material.name,
                    line.material.unit,
                    str(line.norm_per_unit),
                    str(line.calculated_quantity),
                    str(line.actual_quantity),
                ]
                for line in act.lines.all()
            ],
        )
        self._add_signature(doc, "Начальник участка", "Начальник монтажного объекта")
        path = self._doc_path("write_off", act.number)
        doc.save(path)
        return path

    def _export_ppe_issuance(self, entity_id: int) -> Path:
        issuance = PPEIssuance.objects.prefetch_related("lines__worker", "lines__material").get(pk=entity_id)
        doc = self._prepare_doc("ВЕДОМОСТЬ ВЫДАЧИ СПЕЦОДЕЖДЫ", f"№ {issuance.number} от {issuance.issue_date}")
        self._add_meta(
            doc,
            [
                ("Участок", issuance.site_name),
                ("Сезон", issuance.season),
                ("Статус", issuance.get_status_display()),
            ],
        )
        self._add_table(
            doc,
            ["Таб.№", "ФИО", "Материал", "Код", "Ед.", "Кол-во", "Срок службы, мес."],
            [
                [
                    line.worker.employee_number,
                    line.worker.full_name,
                    line.material.name,
                    line.material.code,
                    line.material.unit,
                    str(line.quantity),
                    str(line.service_life_months),
                ]
                for line in issuance.lines.all()
            ],
        )
        self._add_signature(doc, "Материально ответственное лицо", "Начальник участка")
        path = self._doc_path("ppe_issuance", issuance.number)
        doc.save(path)
        return path

    def _export_supplier_document(self, entity_id: int) -> Path:
        item = SupplierDocument.objects.get(pk=entity_id)
        if item.attachment and Path(item.attachment.path).exists():
            return Path(item.attachment.path)
        doc = self._prepare_doc("ДОКУМЕНТ ПОСТАВКИ", f"{item.doc_type} № {item.doc_number} от {item.doc_date}")
        self._add_meta(
            doc,
            [
                ("Поставщик", item.supplier.name),
                ("Сумма", f"{money(item.amount)} руб."),
                ("НДС", f"{money(item.vat_amount)} руб."),
                ("Комментарий", item.notes or ""),
            ],
        )
        path = self._doc_path("supplier_document", item.doc_number)
        doc.save(path)
        return path
