from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    root_dir: Path
    exports_dir: Path
    uploads_dir: Path
    backups_dir: Path
    legacy_dir: Path
    vendor_dir: Path
    host: str = "127.0.0.1"
    port: int = 8010
    app_title: str = "АИС учета материалов АО «СТ-1»"
    company_name: str = "АО «СТ-1»"
    warehouse_name: str = "Центральный склад"
    session_hours: int = 12
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "ais_db"
    postgres_user: str = "postgres"
    postgres_password: str = "root"
    postgres_sslmode: str = "prefer"
    postgres_connect_timeout: int = 10

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname={self.postgres_db} "
            f"user={self.postgres_user} "
            f"password={self.postgres_password} "
            f"sslmode={self.postgres_sslmode} "
            f"connect_timeout={self.postgres_connect_timeout}"
        )

    @property
    def postgres_admin_dsn(self) -> str:
        return (
            f"host={self.postgres_host} "
            f"port={self.postgres_port} "
            f"dbname=postgres "
            f"user={self.postgres_user} "
            f"password={self.postgres_password} "
            f"sslmode={self.postgres_sslmode} "
            f"connect_timeout={self.postgres_connect_timeout}"
        )


BASE_DIR = Path(__file__).resolve().parent.parent

CONFIG = AppConfig(
    root_dir=BASE_DIR,
    exports_dir=BASE_DIR / "exports",
    uploads_dir=BASE_DIR / "uploads",
    backups_dir=BASE_DIR / "backups",
    legacy_dir=BASE_DIR / "legacy",
    vendor_dir=BASE_DIR / "vendor",
)


ROLE_LABELS = {
    "director": "Начальник монтажного объекта",
    "procurement": "Снабженец",
    "warehouse": "Кладовщик",
    "site_manager": "Начальник участка",
    "accounting": "Бухгалтерия",
    "supplier": "Поставщик",
    "admin": "Администратор",
}


ROLE_NAV = {
    "director": ["/dashboard", "/catalogs?entity=contracts", "/procurement", "/writeoffs", "/reports", "/archive"],
    "procurement": ["/dashboard", "/catalogs?entity=suppliers", "/procurement", "/warehouse", "/reports", "/archive"],
    "warehouse": ["/dashboard", "/catalogs?entity=materials", "/warehouse", "/ppe", "/reports", "/archive"],
    "site_manager": ["/dashboard", "/catalogs?entity=workers", "/work", "/procurement", "/writeoffs", "/ppe", "/reports", "/archive"],
    "accounting": ["/dashboard", "/reports", "/archive"],
    "supplier": ["/dashboard", "/supplier", "/archive"],
    "admin": ["/dashboard", "/catalogs?entity=users", "/admin", "/archive"],
}
