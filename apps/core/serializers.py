from __future__ import annotations

from rest_framework import serializers

from .models import (
    DocumentRecord,
    PPEIssuance,
    PPEIssuanceLine,
    PrimaryDocument,
    PrimaryDocumentLine,
    ProcurementRequest,
    ProcurementRequestLine,
    StockIssue,
    StockIssueLine,
    StockReceipt,
    StockReceiptLine,
    SupplyContract,
    SupplierDocument,
    WorkLog,
    WriteOffAct,
    WriteOffLine,
)


class ProcurementRequestLineSerializer(serializers.ModelSerializer):
    material_code = serializers.CharField(source="material.code", read_only=True)
    material_name = serializers.CharField(source="material.name", read_only=True)

    class Meta:
        model = ProcurementRequestLine
        fields = ["material_code", "material_name", "quantity", "unit_price", "notes"]


class ProcurementRequestSerializer(serializers.ModelSerializer):
    supplier_name = serializers.CharField(source="supplier.name", read_only=True)
    contract_number = serializers.CharField(source="contract.number", read_only=True)
    requested_by_name = serializers.CharField(source="requested_by.full_name_or_username", read_only=True)
    lines = ProcurementRequestLineSerializer(many=True, read_only=True)

    class Meta:
        model = ProcurementRequest
        fields = ["id", "number", "request_date", "site_name", "supplier_name", "contract_number", "requested_by_name", "status", "notes", "lines"]


class SupplyContractSerializer(serializers.ModelSerializer):
    supplier_name = serializers.CharField(source="supplier.name", read_only=True)
    related_smr_contract_number = serializers.CharField(source="related_smr_contract.number", read_only=True)

    class Meta:
        model = SupplyContract
        fields = ["id", "number", "contract_date", "supplier_name", "related_smr_contract_number", "amount", "status", "terms"]


class SupplierDocumentSerializer(serializers.ModelSerializer):
    supplier_name = serializers.CharField(source="supplier.name", read_only=True)

    class Meta:
        model = SupplierDocument
        fields = ["id", "doc_type", "doc_number", "doc_date", "supplier_name", "amount", "vat_amount", "status", "notes", "attachment"]


class PrimaryDocumentLineSerializer(serializers.ModelSerializer):
    material_code = serializers.CharField(source="material.code", read_only=True)
    material_name = serializers.CharField(source="material.name", read_only=True)

    class Meta:
        model = PrimaryDocumentLine
        fields = ["material_code", "material_name", "quantity", "unit_price", "notes"]


class PrimaryDocumentSerializer(serializers.ModelSerializer):
    document_type_name = serializers.CharField(source="document_type.name", read_only=True)
    supplier_name = serializers.CharField(source="supplier.name", read_only=True)
    request_number = serializers.CharField(source="procurement_request.number", read_only=True)
    supply_contract_number = serializers.CharField(source="supply_contract.number", read_only=True)
    stock_receipt_number = serializers.CharField(source="stock_receipt.number", read_only=True)
    lines = PrimaryDocumentLineSerializer(many=True, read_only=True)

    class Meta:
        model = PrimaryDocument
        fields = [
            "id",
            "document_type_name",
            "number",
            "doc_date",
            "supplier_name",
            "request_number",
            "supply_contract_number",
            "stock_receipt_number",
            "site_name",
            "basis_reference",
            "amount",
            "vat_amount",
            "status",
            "notes",
            "lines",
        ]


class StockReceiptLineSerializer(serializers.ModelSerializer):
    material_code = serializers.CharField(source="material.code", read_only=True)
    material_name = serializers.CharField(source="material.name", read_only=True)

    class Meta:
        model = StockReceiptLine
        fields = ["material_code", "material_name", "quantity", "unit_price", "notes"]


class StockReceiptSerializer(serializers.ModelSerializer):
    supplier_name = serializers.CharField(source="supplier.name", read_only=True)
    lines = StockReceiptLineSerializer(many=True, read_only=True)

    class Meta:
        model = StockReceipt
        fields = ["id", "number", "receipt_date", "supplier_name", "status", "notes", "lines"]


class StockIssueLineSerializer(serializers.ModelSerializer):
    material_code = serializers.CharField(source="material.code", read_only=True)
    material_name = serializers.CharField(source="material.name", read_only=True)

    class Meta:
        model = StockIssueLine
        fields = ["material_code", "material_name", "quantity", "unit_price", "notes"]


class StockIssueSerializer(serializers.ModelSerializer):
    contract_number = serializers.CharField(source="contract.number", read_only=True)
    lines = StockIssueLineSerializer(many=True, read_only=True)

    class Meta:
        model = StockIssue
        fields = ["id", "number", "issue_date", "site_name", "contract_number", "received_by_name", "status", "notes", "lines"]


class WriteOffLineSerializer(serializers.ModelSerializer):
    material_code = serializers.CharField(source="material.code", read_only=True)
    material_name = serializers.CharField(source="material.name", read_only=True)

    class Meta:
        model = WriteOffLine
        fields = ["material_code", "material_name", "norm_per_unit", "calculated_quantity", "actual_quantity", "unit_price", "notes"]


class WriteOffSerializer(serializers.ModelSerializer):
    contract_number = serializers.CharField(source="contract.number", read_only=True)
    lines = WriteOffLineSerializer(many=True, read_only=True)

    class Meta:
        model = WriteOffAct
        fields = ["id", "number", "act_date", "site_name", "contract_number", "work_type", "work_volume", "volume_unit", "status", "notes", "lines"]


class PPEIssuanceLineSerializer(serializers.ModelSerializer):
    worker_name = serializers.CharField(source="worker.full_name", read_only=True)
    employee_number = serializers.CharField(source="worker.employee_number", read_only=True)
    material_name = serializers.CharField(source="material.name", read_only=True)
    material_code = serializers.CharField(source="material.code", read_only=True)

    class Meta:
        model = PPEIssuanceLine
        fields = ["worker_name", "employee_number", "material_name", "material_code", "quantity", "service_life_months", "issue_start_date", "notes"]


class PPEIssuanceSerializer(serializers.ModelSerializer):
    lines = PPEIssuanceLineSerializer(many=True, read_only=True)

    class Meta:
        model = PPEIssuance
        fields = ["id", "number", "issue_date", "site_name", "season", "status", "notes", "lines"]


class WorkLogSerializer(serializers.ModelSerializer):
    contract_number = serializers.CharField(source="contract.number", read_only=True)

    class Meta:
        model = WorkLog
        fields = ["id", "site_name", "contract_number", "work_type", "planned_volume", "actual_volume", "volume_unit", "plan_date", "actual_date", "status", "notes"]


class DocumentRecordSerializer(serializers.ModelSerializer):
    created_by_name = serializers.CharField(source="created_by.full_name_or_username", read_only=True)

    class Meta:
        model = DocumentRecord
        fields = [
            "id",
            "entity_type",
            "entity_id",
            "doc_type",
            "doc_number",
            "doc_date",
            "status",
            "title",
            "counterparty",
            "object_name",
            "created_by_name",
            "file_path",
            "metadata_json",
        ]


class MetricSerializer(serializers.Serializer):
    contracts = serializers.IntegerField()
    pending = serializers.IntegerField()
    supplier_docs = serializers.IntegerField()
    site_tasks = serializers.IntegerField()
    alerts = serializers.IntegerField()


class WarehouseBalanceSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    code = serializers.CharField()
    name = serializers.CharField()
    unit = serializers.CharField()
    min_stock = serializers.DecimalField(max_digits=14, decimal_places=3)
    warehouse_balance = serializers.DecimalField(max_digits=14, decimal_places=3)


class SiteBalanceSerializer(serializers.Serializer):
    location_name = serializers.CharField()
    code = serializers.CharField()
    name = serializers.CharField()
    unit = serializers.CharField()
    quantity = serializers.DecimalField(max_digits=14, decimal_places=3)
