from __future__ import annotations

from rest_framework.exceptions import PermissionDenied
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.routers import DefaultRouter
from rest_framework.views import APIView
from rest_framework.viewsets import ReadOnlyModelViewSet

from .access import ROLE_SET_INTERNAL, ROLE_SET_SUPPLIER_PORTAL
from .models import DocumentRecord, PPEIssuance, PrimaryDocument, ProcurementRequest, RoleChoices, StockIssue, StockReceipt, SupplierDocument, SupplyContract, WorkLog, WriteOffAct
from .serializers import (
    DocumentRecordSerializer,
    MetricSerializer,
    PPEIssuanceSerializer,
    PrimaryDocumentSerializer,
    ProcurementRequestSerializer,
    SiteBalanceSerializer,
    StockIssueSerializer,
    StockReceiptSerializer,
    SupplyContractSerializer,
    SupplierDocumentSerializer,
    WarehouseBalanceSerializer,
    WorkLogSerializer,
    WriteOffSerializer,
)
from .services import dashboard_metrics, filter_queryset_for_user, low_stock_alerts, site_balances, warehouse_balances



def _require_api_roles(request, allowed_roles: set[str]) -> None:
    if getattr(request.user, "role", None) not in allowed_roles:
        raise PermissionDenied("Недостаточно прав для доступа к API.")


class RoleScopedReadOnlyViewSet(ReadOnlyModelViewSet):
    permission_classes = [IsAuthenticated]
    allowed_roles: set[str] = set()

    def get_scoped_queryset(self):
        _require_api_roles(self.request, set(self.allowed_roles))
        return filter_queryset_for_user(self.request.user, self.queryset.all())

    def get_queryset(self):
        return self.get_scoped_queryset()


class RoleScopedAPIView(APIView):
    permission_classes = [IsAuthenticated]
    allowed_roles: set[str] = ROLE_SET_INTERNAL

    def initial(self, request, *args, **kwargs):
        super().initial(request, *args, **kwargs)
        _require_api_roles(request, set(self.allowed_roles))


class DocumentRecordViewSet(RoleScopedReadOnlyViewSet):
    serializer_class = DocumentRecordSerializer
    queryset = DocumentRecord.objects.select_related("created_by").order_by("-doc_date", "-id")
    allowed_roles = ROLE_SET_INTERNAL | ROLE_SET_SUPPLIER_PORTAL

    def get_queryset(self):
        queryset = self.get_scoped_queryset()
        status = self.request.query_params.get("status")
        if status:
            queryset = queryset.filter(status=status)
        entity_type = self.request.query_params.get("entity_type")
        if entity_type:
            queryset = queryset.filter(entity_type=entity_type)
        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(search_text__icontains=search)
        return queryset


class ProcurementRequestViewSet(RoleScopedReadOnlyViewSet):
    queryset = ProcurementRequest.objects.select_related("supplier", "contract", "requested_by").prefetch_related("lines__material").order_by("-request_date", "-id")
    serializer_class = ProcurementRequestSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SITE_MANAGER, RoleChoices.SUPPLIER}


class SupplyContractViewSet(RoleScopedReadOnlyViewSet):
    queryset = SupplyContract.objects.select_related("supplier", "related_smr_contract").order_by("-contract_date", "-id")
    serializer_class = SupplyContractSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SUPPLIER}


class SupplierDocumentViewSet(RoleScopedReadOnlyViewSet):
    queryset = SupplierDocument.objects.select_related("supplier", "request", "supply_contract").order_by("-doc_date", "-id")
    serializer_class = SupplierDocumentSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.SITE_MANAGER, RoleChoices.SUPPLIER}


class PrimaryDocumentViewSet(RoleScopedReadOnlyViewSet):
    queryset = (
        PrimaryDocument.objects.select_related("document_type", "supplier", "procurement_request", "supply_contract", "stock_receipt")
        .prefetch_related("lines__material")
        .order_by("-doc_date", "-id")
    )
    serializer_class = PrimaryDocumentSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER, RoleChoices.SUPPLIER}


class StockReceiptViewSet(RoleScopedReadOnlyViewSet):
    queryset = StockReceipt.objects.select_related("supplier", "supplier_document").prefetch_related("lines__material").order_by("-receipt_date", "-id")
    serializer_class = StockReceiptSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE}


class StockIssueViewSet(RoleScopedReadOnlyViewSet):
    queryset = StockIssue.objects.select_related("contract").prefetch_related("lines__material").order_by("-issue_date", "-id")
    serializer_class = StockIssueSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER}


class WriteOffViewSet(RoleScopedReadOnlyViewSet):
    queryset = WriteOffAct.objects.select_related("contract", "contract__object").prefetch_related("lines__material").order_by("-act_date", "-id")
    serializer_class = WriteOffSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER}


class PPEIssuanceViewSet(RoleScopedReadOnlyViewSet):
    queryset = PPEIssuance.objects.prefetch_related("lines__worker", "lines__material").order_by("-issue_date", "-id")
    serializer_class = PPEIssuanceSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER}


class WorkLogViewSet(RoleScopedReadOnlyViewSet):
    queryset = WorkLog.objects.select_related("contract").order_by("-actual_date", "-plan_date", "-id")
    serializer_class = WorkLogSerializer
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.SITE_MANAGER}


class DashboardMetricsAPIView(RoleScopedAPIView):
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.DIRECTOR, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER}

    def get(self, request):
        serializer = MetricSerializer(dashboard_metrics(user=request.user))
        return Response(serializer.data)


class WarehouseBalanceAPIView(RoleScopedAPIView):
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.DIRECTOR, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE}

    def get(self, request):
        serializer = WarehouseBalanceSerializer(warehouse_balances(), many=True)
        return Response(serializer.data)


class SiteBalanceAPIView(RoleScopedAPIView):
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.DIRECTOR, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE, RoleChoices.SITE_MANAGER}

    def get(self, request):
        site_name = (request.user.site_name or "").strip() if request.user.role == RoleChoices.SITE_MANAGER else None
        serializer = SiteBalanceSerializer(site_balances(site_name=site_name), many=True)
        return Response(serializer.data)


class LowStockAlertsAPIView(RoleScopedAPIView):
    allowed_roles = {RoleChoices.ADMIN, RoleChoices.DIRECTOR, RoleChoices.PROCUREMENT, RoleChoices.WAREHOUSE}

    def get(self, request):
        serializer = WarehouseBalanceSerializer(low_stock_alerts(), many=True)
        return Response(serializer.data)


router = DefaultRouter()
router.register("documents", DocumentRecordViewSet, basename="api-documents")
router.register("procurement-requests", ProcurementRequestViewSet, basename="api-procurement-requests")
router.register("supply-contracts", SupplyContractViewSet, basename="api-supply-contracts")
router.register("supplier-documents", SupplierDocumentViewSet, basename="api-supplier-documents")
router.register("primary-documents", PrimaryDocumentViewSet, basename="api-primary-documents")
router.register("stock-receipts", StockReceiptViewSet, basename="api-stock-receipts")
router.register("stock-issues", StockIssueViewSet, basename="api-stock-issues")
router.register("writeoffs", WriteOffViewSet, basename="api-writeoffs")
router.register("ppe-issuances", PPEIssuanceViewSet, basename="api-ppe-issuances")
router.register("worklogs", WorkLogViewSet, basename="api-worklogs")
