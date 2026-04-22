from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

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
from .services import sync_document_record, workflow_route_metadata


def _file_path(file_field) -> str:
    if not file_field:
        return ""
    try:
        return str(Path(file_field.path))
    except Exception:
        return ""


def _workflow_metadata(entity_type: str, metadata: dict | None = None) -> dict:
    payload = workflow_route_metadata(entity_type)
    if metadata:
        payload.update(metadata)
    return payload


@receiver(post_save, sender=SMRContract)
def sync_contract(sender, instance: SMRContract, **kwargs) -> None:
    sync_document_record(
        entity_type="smr_contract",
        entity_id=instance.id,
        doc_type="Договор СМР",
        doc_number=instance.number,
        doc_date=instance.contract_date,
        status=instance.status,
        title="Договор на выполнение СМР",
        counterparty=instance.customer_name,
        object_name=instance.object.name if instance.object else "",
        created_by=instance.created_by,
        metadata=_workflow_metadata(
            "smr_contract",
            {"amount": str(instance.amount), "object_id": instance.object_id, "site_name": instance.object.name if instance.object else ""},
        ),
        search_text=instance.subject,
    )


@receiver(post_save, sender=SupplyContract)
def sync_supply_contract(sender, instance: SupplyContract, **kwargs) -> None:
    sync_document_record(
        entity_type="supply_contract",
        entity_id=instance.id,
        doc_type="Договор поставки",
        doc_number=instance.number,
        doc_date=instance.contract_date,
        status=instance.status,
        title="Договор поставки материалов",
        counterparty=instance.supplier.name,
        object_name=instance.related_smr_contract.number if instance.related_smr_contract else "",
        metadata=_workflow_metadata(
            "supply_contract",
            {"amount": str(instance.amount), "supplier_id": instance.supplier_id, "contract_id": instance.related_smr_contract_id},
        ),
    )


@receiver(post_save, sender=ProcurementRequest)
def sync_procurement(sender, instance: ProcurementRequest, **kwargs) -> None:
    sync_document_record(
        entity_type="procurement_request",
        entity_id=instance.id,
        doc_type="Заявка поставщику",
        doc_number=instance.number,
        doc_date=instance.request_date,
        status=instance.status,
        title="Заявка на закупку материалов",
        counterparty=instance.supplier.name if instance.supplier else "",
        object_name=instance.contract.number if instance.contract else instance.site_name,
        created_by=instance.requested_by,
        metadata=_workflow_metadata(
            "procurement_request",
            {
                "supplier_id": instance.supplier_id,
                "site_name": instance.site_name,
                "contract_id": instance.contract_id,
            },
        ),
        search_text=instance.notes,
    )


@receiver(post_save, sender=PrimaryDocument)
def sync_primary_document(sender, instance: PrimaryDocument, **kwargs) -> None:
    sync_document_record(
        entity_type="primary_document",
        entity_id=instance.id,
        doc_type=instance.document_type.name,
        doc_number=instance.number,
        doc_date=instance.doc_date,
        status=instance.status,
        title=instance.document_type.name,
        counterparty=instance.supplier.name,
        object_name=instance.site_name or instance.basis_reference,
        created_by=instance.created_by,
        metadata=_workflow_metadata(
            "primary_document",
            {
                "amount": str(instance.amount),
                "vat_amount": str(instance.vat_amount),
                "document_type_code": instance.document_type.code,
                "supplier_id": instance.supplier_id,
                "site_name": instance.site_name,
                "request_id": instance.procurement_request_id,
                "stock_receipt_id": instance.stock_receipt_id,
                "supply_contract_id": instance.supply_contract_id,
            },
        ),
        search_text=f"{instance.basis_reference} {instance.notes}".strip(),
    )


@receiver(post_save, sender=SupplierDocument)
def sync_supplier_document(sender, instance: SupplierDocument, **kwargs) -> None:
    sync_document_record(
        entity_type="supplier_document",
        entity_id=instance.id,
        doc_type=instance.doc_type,
        doc_number=instance.doc_number,
        doc_date=instance.doc_date,
        status=instance.status,
        title="Документ поставки",
        counterparty=instance.supplier.name,
        created_by=instance.uploaded_by,
        file_path=_file_path(instance.attachment),
        metadata=_workflow_metadata(
            "supplier_document",
            {
                "amount": str(instance.amount),
                "vat_amount": str(instance.vat_amount),
                "supplier_id": instance.supplier_id,
                "request_id": instance.request_id,
                "supply_contract_id": instance.supply_contract_id,
                "site_name": instance.request.site_name if instance.request else "",
            },
        ),
        search_text=instance.notes,
    )


@receiver(post_save, sender=StockReceipt)
def sync_receipt(sender, instance: StockReceipt, **kwargs) -> None:
    sync_document_record(
        entity_type="stock_receipt",
        entity_id=instance.id,
        doc_type="Приходный ордер",
        doc_number=instance.number,
        doc_date=instance.receipt_date,
        status=instance.status,
        title="Приходный ордер на склад",
        counterparty=instance.supplier.name,
        object_name=settings.WAREHOUSE_NAME,
        created_by=instance.created_by,
        metadata=_workflow_metadata(
            "stock_receipt",
            {"supplier_id": instance.supplier_id, "site_name": settings.WAREHOUSE_NAME, "supplier_document_id": instance.supplier_document_id},
        ),
        search_text=instance.notes,
    )


@receiver(post_save, sender=StockIssue)
def sync_issue(sender, instance: StockIssue, **kwargs) -> None:
    sync_document_record(
        entity_type="stock_issue",
        entity_id=instance.id,
        doc_type="Требование-накладная",
        doc_number=instance.number,
        doc_date=instance.issue_date,
        status=instance.status,
        title="Отпуск материалов со склада",
        counterparty=instance.received_by_name,
        object_name=instance.contract.number if instance.contract else instance.site_name,
        created_by=instance.issued_by,
        metadata=_workflow_metadata("stock_issue", {"site_name": instance.site_name, "contract_id": instance.contract_id}),
        search_text=instance.notes,
    )


@receiver(post_save, sender=WriteOffAct)
def sync_writeoff(sender, instance: WriteOffAct, **kwargs) -> None:
    sync_document_record(
        entity_type="write_off",
        entity_id=instance.id,
        doc_type="Акт списания",
        doc_number=instance.number,
        doc_date=instance.act_date,
        status=instance.status,
        title="Акт списания материалов",
        counterparty=instance.contract.number,
        object_name=instance.contract.object.name if instance.contract.object else instance.site_name,
        created_by=instance.created_by,
        metadata=_workflow_metadata(
            "write_off",
            {"work_type": instance.work_type, "work_volume": str(instance.work_volume), "site_name": instance.site_name, "contract_id": instance.contract_id},
        ),
        search_text=instance.notes,
    )


@receiver(post_save, sender=PPEIssuance)
def sync_ppe(sender, instance: PPEIssuance, **kwargs) -> None:
    sync_document_record(
        entity_type="ppe_issuance",
        entity_id=instance.id,
        doc_type="Ведомость спецодежды",
        doc_number=instance.number,
        doc_date=instance.issue_date,
        status=instance.status,
        title="Ведомость учета выдачи спецодежды",
        counterparty=instance.site_name,
        object_name=instance.site_name,
        created_by=instance.issued_by,
        metadata=_workflow_metadata("ppe_issuance", {"site_name": instance.site_name}),
        search_text=instance.notes,
    )


def _cleanup_record(entity_type: str, entity_id: int) -> None:
    DocumentRecord.objects.filter(entity_type=entity_type, entity_id=entity_id).delete()


@receiver(post_delete, sender=SMRContract)
def cleanup_contract(sender, instance: SMRContract, **kwargs) -> None:
    _cleanup_record("smr_contract", instance.id)


@receiver(post_delete, sender=SupplyContract)
def cleanup_supply_contract(sender, instance: SupplyContract, **kwargs) -> None:
    _cleanup_record("supply_contract", instance.id)


@receiver(post_delete, sender=ProcurementRequest)
def cleanup_procurement(sender, instance: ProcurementRequest, **kwargs) -> None:
    _cleanup_record("procurement_request", instance.id)


@receiver(post_delete, sender=PrimaryDocument)
def cleanup_primary_document(sender, instance: PrimaryDocument, **kwargs) -> None:
    _cleanup_record("primary_document", instance.id)


@receiver(post_delete, sender=SupplierDocument)
def cleanup_supplier_document(sender, instance: SupplierDocument, **kwargs) -> None:
    _cleanup_record("supplier_document", instance.id)


@receiver(post_delete, sender=StockReceipt)
def cleanup_receipt(sender, instance: StockReceipt, **kwargs) -> None:
    _cleanup_record("stock_receipt", instance.id)


@receiver(post_delete, sender=StockIssue)
def cleanup_issue(sender, instance: StockIssue, **kwargs) -> None:
    _cleanup_record("stock_issue", instance.id)


@receiver(post_delete, sender=WriteOffAct)
def cleanup_writeoff(sender, instance: WriteOffAct, **kwargs) -> None:
    _cleanup_record("write_off", instance.id)


@receiver(post_delete, sender=PPEIssuance)
def cleanup_ppe(sender, instance: PPEIssuance, **kwargs) -> None:
    _cleanup_record("ppe_issuance", instance.id)
