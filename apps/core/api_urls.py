from django.urls import include, path

from .api import DashboardMetricsAPIView, LowStockAlertsAPIView, SiteBalanceAPIView, WarehouseBalanceAPIView, router


urlpatterns = [
    path("metrics/", DashboardMetricsAPIView.as_view(), name="api-metrics"),
    path("warehouse-balances/", WarehouseBalanceAPIView.as_view(), name="api-warehouse-balances"),
    path("site-balances/", SiteBalanceAPIView.as_view(), name="api-site-balances"),
    path("low-stock-alerts/", LowStockAlertsAPIView.as_view(), name="api-low-stock-alerts"),
    path("", include(router.urls)),
]
