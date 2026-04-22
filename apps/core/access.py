from __future__ import annotations

from .models import DocumentStatus, RoleChoices


ROLE_SET_ALL = {choice for choice, _label in RoleChoices.choices}
ROLE_SET_SUPPLIER_PORTAL = {RoleChoices.SUPPLIER}
ROLE_SET_INTERNAL = ROLE_SET_ALL - ROLE_SET_SUPPLIER_PORTAL
ROLE_SET_OFFICE = {
    RoleChoices.ADMIN,
    RoleChoices.DIRECTOR,
    RoleChoices.PROCUREMENT,
    RoleChoices.WAREHOUSE,
}
ROLE_SET_BACKUP = {RoleChoices.ADMIN}
ROLE_SET_ARCHIVE = ROLE_SET_INTERNAL | ROLE_SET_SUPPLIER_PORTAL
ROLE_SET_REPORTS = ROLE_SET_INTERNAL
ROLE_SET_ARCHIVE_STATUS_UPDATE = ROLE_SET_ARCHIVE
ROLE_SET_AUDIT_LOG = {RoleChoices.ADMIN}
ACCOUNTING_VISIBLE_STATUSES = {
    DocumentStatus.APPROVED,
    DocumentStatus.SENT_ACCOUNTING,
    DocumentStatus.ACCEPTED,
}
ROLE_SET_STAFF = {
    RoleChoices.ADMIN,
    RoleChoices.ACCOUNTING,
    RoleChoices.PROCUREMENT,
    RoleChoices.WAREHOUSE,
    RoleChoices.DIRECTOR,
}


def can_access_archive(role: str | None) -> bool:
    return role in ROLE_SET_ARCHIVE


def can_access_reports(role: str | None) -> bool:
    return role in ROLE_SET_REPORTS


def can_access_backups(role: str | None) -> bool:
    return role in ROLE_SET_BACKUP


def can_update_archive_status(role: str | None) -> bool:
    return role in ROLE_SET_ARCHIVE_STATUS_UPDATE


def can_access_audit_log(role: str | None) -> bool:
    return role in ROLE_SET_AUDIT_LOG
