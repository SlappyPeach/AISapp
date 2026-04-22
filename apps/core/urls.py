from django.urls import path

from . import views


urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("catalogs/<slug:slug>/", views.catalog_page, name="catalog-page"),
    path("operations/<slug:slug>/", views.operation_page, name="operation-page"),
    path("operations/<slug:slug>/draft/", views.operation_draft, name="operation-draft"),
    path("archive/", views.archive, name="archive"),
    path("reports/", views.reports, name="reports"),
    path("backups/", views.backups, name="backups"),
    path("audit-log/", views.audit_log, name="audit-log"),
    path("exports/document/<str:entity_type>/<int:entity_id>/", views.export_document, name="export-document"),
    path("exports/report/", views.export_report, name="export-report"),
    path("backups/download/<str:backup_name>/", views.download_backup, name="download-backup"),
]
