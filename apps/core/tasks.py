from __future__ import annotations

from django.contrib.auth import get_user_model
from celery import shared_task

from .exports import Exporter
from .services import write_backup_file


@shared_task
def export_document_task(entity_type: str, entity_id: int) -> str:
    return str(Exporter().export_document(entity_type, entity_id))


@shared_task
def export_report_task(report_name: str, filters: dict) -> str:
    return str(Exporter().export_report(report_name, filters))


@shared_task
def create_backup_task(user_id: int) -> str:
    user = get_user_model().objects.get(pk=user_id)
    return str(write_backup_file(user=user))
