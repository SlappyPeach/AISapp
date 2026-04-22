from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from psycopg import connect, sql

from .config import CONFIG
from .security import hash_password, new_token, verify_password


class CompatRow(Mapping[str, Any]):
    def __init__(self, columns: list[str], values: tuple[Any, ...] | list[Any]) -> None:
        self._columns = tuple(columns)
        self._values = tuple(values)
        self._data = dict(zip(self._columns, self._values))

    def __getitem__(self, key: str | int) -> Any:
        if isinstance(key, int):
            return self._values[key]
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)


class CursorWrapper:
    def __init__(self, cursor, lastrowid: int | None = None) -> None:
        self._cursor = cursor
        self.lastrowid = lastrowid

    def _columns(self) -> list[str]:
        if not self._cursor.description:
            return []
        return [column.name if hasattr(column, "name") else column[0] for column in self._cursor.description]

    def _wrap(self, row: Any) -> Any:
        if row is None:
            return None
        columns = self._columns()
        if not columns:
            return row
        return CompatRow(columns, row)

    def fetchone(self) -> CompatRow | None:
        return self._wrap(self._cursor.fetchone())

    def fetchall(self) -> list[CompatRow]:
        return [self._wrap(row) for row in self._cursor.fetchall()]


def normalize_query(query: str) -> str:
    return query.replace("?", "%s")


def split_sql_statements(script: str) -> list[str]:
    return [statement.strip() for statement in script.split(";") if statement.strip()]


class ConnectionWrapper:
    def __init__(self, raw_connection) -> None:
        self._raw = raw_connection

    def __enter__(self) -> ConnectionWrapper:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc_type:
            self._raw.rollback()
        else:
            self._raw.commit()
        self._raw.close()

    def execute(self, query: str, params: tuple[Any, ...] | list[Any] = ()) -> CursorWrapper:
        statement = normalize_query(query)
        lastrowid = None
        normalized_upper = statement.lstrip().upper()
        if normalized_upper.startswith("INSERT") and "RETURNING" not in normalized_upper and "INSERT INTO SESSIONS" not in normalized_upper:
            statement = statement.rstrip().rstrip(";") + " RETURNING id"
            cursor = self._raw.execute(statement, params)
            returned = cursor.fetchone()
            lastrowid = returned[0] if returned else None
            return CursorWrapper(cursor, lastrowid=lastrowid)
        cursor = self._raw.execute(statement, params)
        return CursorWrapper(cursor, lastrowid=lastrowid)

    def executemany(self, query: str, params_seq: list[tuple[Any, ...]] | tuple[tuple[Any, ...], ...]) -> None:
        with self._raw.cursor() as cursor:
            cursor.executemany(normalize_query(query), params_seq)

    def executescript(self, script: str) -> None:
        for statement in split_sql_statements(script):
            self._raw.execute(statement)

    def commit(self) -> None:
        self._raw.commit()

    def rollback(self) -> None:
        self._raw.rollback()

    def close(self) -> None:
        self._raw.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    full_name TEXT NOT NULL,
    role TEXT NOT NULL,
    site_name TEXT,
    supplier_id BIGINT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    session_id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS suppliers (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    tax_id TEXT,
    contact_person TEXT,
    phone TEXT,
    email TEXT,
    address TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS materials (
    id BIGSERIAL PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    unit TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL DEFAULT 0,
    min_stock DOUBLE PRECISION NOT NULL DEFAULT 0,
    category TEXT,
    is_ppe BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS construction_objects (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT,
    customer_name TEXT,
    description TEXT,
    start_date TEXT,
    end_date TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workers (
    id BIGSERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    employee_number TEXT NOT NULL UNIQUE,
    site_name TEXT,
    position TEXT,
    hire_date TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS material_norms (
    id BIGSERIAL PRIMARY KEY,
    work_type TEXT NOT NULL,
    material_id BIGINT NOT NULL,
    norm_per_unit DOUBLE PRECISION NOT NULL,
    unit TEXT,
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (material_id) REFERENCES materials(id)
);

CREATE TABLE IF NOT EXISTS smr_contracts (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    contract_date TEXT NOT NULL,
    object_id BIGINT,
    customer_name TEXT NOT NULL,
    subject TEXT NOT NULL,
    work_type TEXT,
    planned_volume DOUBLE PRECISION,
    volume_unit TEXT,
    amount DOUBLE PRECISION NOT NULL,
    vat_rate DOUBLE PRECISION NOT NULL DEFAULT 20,
    start_date TEXT,
    end_date TEXT,
    status TEXT NOT NULL DEFAULT 'draft',
    created_by BIGINT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (object_id) REFERENCES construction_objects(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS supply_contracts (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    contract_date TEXT NOT NULL,
    supplier_id BIGINT NOT NULL,
    related_smr_contract_id BIGINT,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'draft',
    terms TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
    FOREIGN KEY (related_smr_contract_id) REFERENCES smr_contracts(id)
);

CREATE TABLE IF NOT EXISTS procurement_requests (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    request_date TEXT NOT NULL,
    site_name TEXT NOT NULL,
    contract_id BIGINT,
    supplier_id BIGINT,
    requested_by BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (contract_id) REFERENCES smr_contracts(id),
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
    FOREIGN KEY (requested_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS procurement_request_lines (
    id BIGSERIAL PRIMARY KEY,
    request_id BIGINT NOT NULL,
    material_id BIGINT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (request_id) REFERENCES procurement_requests(id) ON DELETE CASCADE,
    FOREIGN KEY (material_id) REFERENCES materials(id)
);

CREATE TABLE IF NOT EXISTS supplier_documents (
    id BIGSERIAL PRIMARY KEY,
    supplier_id BIGINT NOT NULL,
    request_id BIGINT,
    supply_contract_id BIGINT,
    doc_type TEXT NOT NULL,
    doc_number TEXT NOT NULL,
    doc_date TEXT NOT NULL,
    amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    vat_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    uploaded_by BIGINT NOT NULL,
    file_name TEXT,
    stored_path TEXT,
    status TEXT NOT NULL DEFAULT 'uploaded',
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
    FOREIGN KEY (request_id) REFERENCES procurement_requests(id),
    FOREIGN KEY (supply_contract_id) REFERENCES supply_contracts(id),
    FOREIGN KEY (uploaded_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS stock_receipts (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    receipt_date TEXT NOT NULL,
    supplier_id BIGINT NOT NULL,
    supplier_document_id BIGINT,
    created_by BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(id),
    FOREIGN KEY (supplier_document_id) REFERENCES supplier_documents(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS stock_receipt_lines (
    id BIGSERIAL PRIMARY KEY,
    receipt_id BIGINT NOT NULL,
    material_id BIGINT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (receipt_id) REFERENCES stock_receipts(id) ON DELETE CASCADE,
    FOREIGN KEY (material_id) REFERENCES materials(id)
);

CREATE TABLE IF NOT EXISTS stock_issues (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    issue_date TEXT NOT NULL,
    site_name TEXT NOT NULL,
    contract_id BIGINT,
    issued_by BIGINT NOT NULL,
    received_by_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (contract_id) REFERENCES smr_contracts(id),
    FOREIGN KEY (issued_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS stock_issue_lines (
    id BIGSERIAL PRIMARY KEY,
    issue_id BIGINT NOT NULL,
    material_id BIGINT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (issue_id) REFERENCES stock_issues(id) ON DELETE CASCADE,
    FOREIGN KEY (material_id) REFERENCES materials(id)
);

CREATE TABLE IF NOT EXISTS work_logs (
    id BIGSERIAL PRIMARY KEY,
    site_name TEXT NOT NULL,
    contract_id BIGINT,
    work_type TEXT NOT NULL,
    planned_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    actual_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
    volume_unit TEXT,
    plan_date TEXT,
    actual_date TEXT,
    status TEXT NOT NULL DEFAULT 'planned',
    notes TEXT,
    created_by BIGINT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (contract_id) REFERENCES smr_contracts(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS write_off_acts (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    act_date TEXT NOT NULL,
    contract_id BIGINT NOT NULL,
    site_name TEXT NOT NULL,
    work_type TEXT NOT NULL,
    work_volume DOUBLE PRECISION NOT NULL,
    volume_unit TEXT,
    created_by BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (contract_id) REFERENCES smr_contracts(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS write_off_lines (
    id BIGSERIAL PRIMARY KEY,
    act_id BIGINT NOT NULL,
    material_id BIGINT NOT NULL,
    norm_per_unit DOUBLE PRECISION NOT NULL,
    calculated_quantity DOUBLE PRECISION NOT NULL,
    actual_quantity DOUBLE PRECISION NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    notes TEXT,
    FOREIGN KEY (act_id) REFERENCES write_off_acts(id) ON DELETE CASCADE,
    FOREIGN KEY (material_id) REFERENCES materials(id)
);

CREATE TABLE IF NOT EXISTS ppe_issuances (
    id BIGSERIAL PRIMARY KEY,
    number TEXT NOT NULL UNIQUE,
    issue_date TEXT NOT NULL,
    site_name TEXT NOT NULL,
    season TEXT,
    issued_by BIGINT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    notes TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (issued_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS ppe_issuance_lines (
    id BIGSERIAL PRIMARY KEY,
    issuance_id BIGINT NOT NULL,
    worker_id BIGINT NOT NULL,
    material_id BIGINT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    service_life_months INTEGER,
    issue_start_date TEXT,
    notes TEXT,
    FOREIGN KEY (issuance_id) REFERENCES ppe_issuances(id) ON DELETE CASCADE,
    FOREIGN KEY (worker_id) REFERENCES workers(id),
    FOREIGN KEY (material_id) REFERENCES materials(id)
);

CREATE TABLE IF NOT EXISTS stock_movements (
    id BIGSERIAL PRIMARY KEY,
    movement_date TEXT NOT NULL,
    material_id BIGINT NOT NULL,
    quantity_delta DOUBLE PRECISION NOT NULL,
    location_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_id BIGINT NOT NULL,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_by BIGINT NOT NULL,
    notes TEXT,
    FOREIGN KEY (material_id) REFERENCES materials(id),
    FOREIGN KEY (created_by) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS documents (
    id BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id BIGINT NOT NULL,
    doc_type TEXT NOT NULL,
    doc_number TEXT NOT NULL,
    doc_date TEXT NOT NULL,
    status TEXT NOT NULL,
    title TEXT NOT NULL,
    counterparty TEXT,
    object_name TEXT,
    created_by BIGINT,
    file_path TEXT,
    metadata_json TEXT,
    search_text TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(entity_type, entity_id)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id BIGINT,
    details TEXT,
    created_at TEXT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
"""


SIMPLE_TABLES = {
    "materials": "materials",
    "suppliers": "suppliers",
    "objects": "construction_objects",
    "workers": "workers",
    "norms": "material_norms",
    "contracts": "smr_contracts",
    "supply_contracts": "supply_contracts",
    "users": "users",
}


BACKUP_TABLES = [
    "suppliers",
    "users",
    "sessions",
    "materials",
    "construction_objects",
    "workers",
    "material_norms",
    "smr_contracts",
    "supply_contracts",
    "procurement_requests",
    "procurement_request_lines",
    "supplier_documents",
    "stock_receipts",
    "stock_receipt_lines",
    "stock_issues",
    "stock_issue_lines",
    "work_logs",
    "write_off_acts",
    "write_off_lines",
    "ppe_issuances",
    "ppe_issuance_lines",
    "stock_movements",
    "documents",
    "audit_log",
]


SERIAL_TABLES = [
    "users",
    "suppliers",
    "materials",
    "construction_objects",
    "workers",
    "material_norms",
    "smr_contracts",
    "supply_contracts",
    "procurement_requests",
    "procurement_request_lines",
    "supplier_documents",
    "stock_receipts",
    "stock_receipt_lines",
    "stock_issues",
    "stock_issue_lines",
    "work_logs",
    "write_off_acts",
    "write_off_lines",
    "ppe_issuances",
    "ppe_issuance_lines",
    "stock_movements",
    "documents",
    "audit_log",
]


def now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


def today_str() -> str:
    return datetime.now().date().isoformat()


def parse_float(value: str | None, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    return float(str(value).replace(",", "."))


class Database:
    def __init__(self) -> None:
        for path in [CONFIG.exports_dir, CONFIG.uploads_dir, CONFIG.backups_dir, CONFIG.legacy_dir, CONFIG.vendor_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def ensure_database(self) -> None:
        with connect(CONFIG.postgres_admin_dsn, autocommit=True) as conn:
            exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (CONFIG.postgres_db,)).fetchone()
            if not exists:
                conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(CONFIG.postgres_db)))

    def connect(self) -> ConnectionWrapper:
        return ConnectionWrapper(connect(CONFIG.postgres_dsn, autocommit=False))

    def init(self) -> None:
        self.ensure_database()
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._seed(conn)

    def _seed(self, conn: ConnectionWrapper) -> None:
        has_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if has_users:
            return

        created_at = now_str()
        conn.execute(
            "INSERT INTO suppliers (name, tax_id, contact_person, phone, email, address, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("ООО «ПромСнаб-Сервис»", "7708123456", "Иван Миронов", "+7 701 000 11 22", "supply@example.ru", "г. Байконур, ул. Складская, 10", created_at),
        )
        supplier_id = conn.execute("SELECT id FROM suppliers WHERE name = ?", ("ООО «ПромСнаб-Сервис»",)).fetchone()["id"]

        users = [
            ("director", "director123", "Алексей Белов", "director", "Монтажный объект №1", None),
            ("procurement", "procurement123", "Марина Соколова", "procurement", None, None),
            ("warehouse", "warehouse123", "Игорь Пахомов", "warehouse", None, None),
            ("site12", "site123", "Владимир Орлов", "site_manager", "Участок №12", None),
            ("accounting", "accounting123", "Бухгалтерия Москва", "accounting", None, None),
            ("supplier1", "supplier123", "Поставщик ПромСнаб", "supplier", None, supplier_id),
            ("admin", "admin123", "Системный администратор", "admin", None, None),
        ]
        for username, password, full_name, role, site_name, user_supplier_id in users:
            conn.execute(
                """
                INSERT INTO users (username, password_hash, full_name, role, site_name, supplier_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (username, hash_password(password), full_name, role, site_name, user_supplier_id, created_at),
            )

        materials = [
            ("БН00178", "Аргон", "м3", 550, 10, "Газы", False),
            ("Б00054", "Ветошь", "кг", 90, 15, "Расходные материалы", False),
            ("М012506", "Аптечка ФЭСТ перечень №2", "шт", 1800, 2, "Хозтовары", False),
            ("М13137", "Костюм мужской Велес утепленный", "комп", 7800, 4, "СИЗ", True),
            ("М13139", "Ботинки утепленные Неоград_2", "пара", 6200, 4, "СИЗ", True),
            ("БН00085", "Спецодежда х/б летняя", "комп", 4200, 5, "СИЗ", True),
            ("М00058", "Полуботинки рабочие", "пара", 3800, 5, "СИЗ", True),
            ("М13283", "Сверло по металлу 11 мм", "шт", 210, 30, "Инструмент", False),
        ]
        conn.executemany(
            "INSERT INTO materials (code, name, unit, price, min_stock, category, is_ppe, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(code, name, unit, price, min_stock, category, is_ppe, created_at) for code, name, unit, price, min_stock, category, is_ppe in materials],
        )

        conn.execute(
            """
            INSERT INTO construction_objects (name, address, customer_name, description, start_date, end_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "Сооружение 175, насосная станция пл.200",
                "г. Байконур, ул. Южная, д. 2",
                "ФГУП «ЦЭНКИ»",
                "Текущий ремонт и послепусковые работы",
                "2026-04-01",
                "2026-06-30",
                created_at,
            ),
        )
        object_id = conn.execute("SELECT id FROM construction_objects LIMIT 1").fetchone()["id"]
        director_id = conn.execute("SELECT id FROM users WHERE role = 'director'").fetchone()["id"]

        contract_number = "SMR-20260419-001"
        conn.execute(
            """
            INSERT INTO smr_contracts
            (number, contract_date, object_id, customer_name, subject, work_type, planned_volume, volume_unit, amount, vat_rate, start_date, end_date, status, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                contract_number,
                today_str(),
                object_id,
                "ФГУП «ЦЭНКИ»",
                "Ремонт сооружения 175 (насосная станция)",
                "Покраска",
                1200,
                "м2",
                645079,
                20,
                "2026-04-20",
                "2026-06-20",
                "approved",
                director_id,
                created_at,
            ),
        )
        contract_id = conn.execute("SELECT id FROM smr_contracts WHERE number = ?", (contract_number,)).fetchone()["id"]
        self.register_document(
            conn,
            entity_type="smr_contract",
            entity_id=contract_id,
            doc_type="Договор СМР",
            doc_number=contract_number,
            doc_date=today_str(),
            status="approved",
            title="Договор на выполнение СМР",
            counterparty="ФГУП «ЦЭНКИ»",
            object_name="Сооружение 175, насосная станция пл.200",
            created_by=director_id,
            metadata={"amount": 645079},
        )

        supply_number = "SUP-20260419-001"
        conn.execute(
            """
            INSERT INTO supply_contracts (number, contract_date, supplier_id, related_smr_contract_id, amount, status, terms, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                supply_number,
                today_str(),
                supplier_id,
                contract_id,
                250000,
                "approved",
                "Поставка лакокрасочных материалов и расходников.",
                created_at,
            ),
        )
        supply_id = conn.execute("SELECT id FROM supply_contracts WHERE number = ?", (supply_number,)).fetchone()["id"]
        self.register_document(
            conn,
            entity_type="supply_contract",
            entity_id=supply_id,
            doc_type="Договор поставки",
            doc_number=supply_number,
            doc_date=today_str(),
            status="approved",
            title="Договор поставки материалов",
            counterparty="ООО «ПромСнаб-Сервис»",
            object_name="Сооружение 175, насосная станция пл.200",
            created_by=director_id,
        )

        warehouse_id = conn.execute("SELECT id FROM users WHERE role = 'warehouse'").fetchone()["id"]
        site_manager_id = conn.execute("SELECT id FROM users WHERE role = 'site_manager'").fetchone()["id"]

        workers = [
            ("Шевцов Андрей Николаевич", "1656", "Участок №12", "Монтажник", "2025-11-01"),
            ("Иванов Сергей Петрович", "7543", "Участок №12", "Сварщик", "2025-11-15"),
        ]
        conn.executemany(
            "INSERT INTO workers (full_name, employee_number, site_name, position, hire_date, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            [(full_name, employee_number, site_name, position, hire_date, created_at) for full_name, employee_number, site_name, position, hire_date in workers],
        )

        norms = [
            ("Покраска", "Б00054", 0.035, "кг", "Норма ветоши на 1 м2 поверхности"),
            ("Покраска", "БН00178", 0.025, "м3", "Расход аргона по технологической карте"),
            ("Монтаж", "М13283", 0.4, "шт", "Сверла на 1 условную единицу монтажа"),
        ]
        for work_type, material_code, norm_per_unit, unit, notes in norms:
            material_id = conn.execute("SELECT id FROM materials WHERE code = ?", (material_code,)).fetchone()["id"]
            conn.execute(
                "INSERT INTO material_norms (work_type, material_id, norm_per_unit, unit, notes, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                (work_type, material_id, norm_per_unit, unit, notes, created_at),
            )

        material_rows = conn.execute("SELECT id, code, price FROM materials WHERE code IN ('БН00178', 'Б00054', 'М13283', 'М13137', 'М13139')").fetchall()
        price_by_code = {row["code"]: (row["id"], row["price"]) for row in material_rows}

        receipt_number = "REC-20260419-001"
        conn.execute(
            """
            INSERT INTO stock_receipts (number, receipt_date, supplier_id, created_by, status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (receipt_number, today_str(), supplier_id, warehouse_id, "approved", "Стартовый приход материалов", created_at),
        )
        receipt_id = conn.execute("SELECT id FROM stock_receipts WHERE number = ?", (receipt_number,)).fetchone()["id"]
        receipt_lines = [
            (receipt_id, price_by_code["БН00178"][0], 60, price_by_code["БН00178"][1], ""),
            (receipt_id, price_by_code["Б00054"][0], 100, price_by_code["Б00054"][1], ""),
            (receipt_id, price_by_code["М13283"][0], 50, price_by_code["М13283"][1], ""),
            (receipt_id, price_by_code["М13137"][0], 10, price_by_code["М13137"][1], ""),
            (receipt_id, price_by_code["М13139"][0], 10, price_by_code["М13139"][1], ""),
        ]
        conn.executemany(
            "INSERT INTO stock_receipt_lines (receipt_id, material_id, quantity, unit_price, notes) VALUES (?, ?, ?, ?, ?)",
            receipt_lines,
        )
        for _, material_id, quantity, unit_price, _ in receipt_lines:
            conn.execute(
                """
                INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (today_str(), material_id, quantity, CONFIG.warehouse_name, "stock_receipt", receipt_id, unit_price, warehouse_id, "Стартовый остаток"),
            )
        self.register_document(
            conn,
            entity_type="stock_receipt",
            entity_id=receipt_id,
            doc_type="Приходный ордер",
            doc_number=receipt_number,
            doc_date=today_str(),
            status="approved",
            title="Приход материалов на склад",
            counterparty="ООО «ПромСнаб-Сервис»",
            object_name=CONFIG.warehouse_name,
            created_by=warehouse_id,
        )

        ppe_number = "PPE-20260419-001"
        conn.execute(
            """
            INSERT INTO ppe_issuances (number, issue_date, site_name, season, issued_by, status, notes, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (ppe_number, today_str(), "Участок №12", "зимняя", site_manager_id, "approved", "Первичная выдача", created_at),
        )
        ppe_id = conn.execute("SELECT id FROM ppe_issuances WHERE number = ?", (ppe_number,)).fetchone()["id"]
        worker_rows = conn.execute("SELECT id, employee_number FROM workers").fetchall()
        worker_by_number = {row["employee_number"]: row["id"] for row in worker_rows}
        ppe_lines = [
            (ppe_id, worker_by_number["1656"], price_by_code["М13137"][0], 1, 24, today_str(), ""),
            (ppe_id, worker_by_number["7543"], price_by_code["М13139"][0], 1, 24, today_str(), ""),
        ]
        conn.executemany(
            """
            INSERT INTO ppe_issuance_lines (issuance_id, worker_id, material_id, quantity, service_life_months, issue_start_date, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ppe_lines,
        )
        for _, _, material_id, quantity, _, _, _ in ppe_lines:
            unit_price = conn.execute("SELECT price FROM materials WHERE id = ?", (material_id,)).fetchone()["price"]
            conn.execute(
                """
                INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (today_str(), material_id, -quantity, CONFIG.warehouse_name, "ppe_issuance", ppe_id, unit_price, site_manager_id, "Выдача СИЗ"),
            )
        self.register_document(
            conn,
            entity_type="ppe_issuance",
            entity_id=ppe_id,
            doc_type="Ведомость спецодежды",
            doc_number=ppe_number,
            doc_date=today_str(),
            status="approved",
            title="Ведомость выдачи спецодежды",
            counterparty="Участок №12",
            object_name="Участок №12",
            created_by=site_manager_id,
        )

    def fetch_one(self, query: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(query, params).fetchone()
            return dict(row) if row else None

    def fetch_all(self, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> int:
        with self.connect() as conn:
            cur = conn.execute(query, params)
            conn.commit()
            return int(cur.lastrowid or 0)

    def create_session(self, user_id: int) -> str:
        session_id = new_token()
        created_at = now_str()
        expires_at = (datetime.now() + timedelta(hours=CONFIG.session_hours)).isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
            conn.execute(
                "INSERT INTO sessions (session_id, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
                (session_id, user_id, created_at, expires_at),
            )
            conn.commit()
        return session_id

    def get_user_by_session(self, session_id: str | None) -> dict[str, Any] | None:
        if not session_id:
            return None
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT u.*
                FROM sessions s
                JOIN users u ON u.id = s.user_id
                WHERE s.session_id = ? AND s.expires_at >= ?
                """,
                (session_id, now_str()),
            ).fetchone()
            return dict(row) if row else None

    def delete_session(self, session_id: str | None) -> None:
        if not session_id:
            return
        with self.connect() as conn:
            conn.execute("DELETE FROM sessions WHERE session_id = ?", (session_id,))
            conn.commit()

    def authenticate(self, username: str, password: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM users WHERE username = ? AND is_active = TRUE", (username,)).fetchone()
            if row and verify_password(password, row["password_hash"]):
                self.audit(conn, row["id"], "login", "user", row["id"], f"Вход пользователя {username}")
                conn.commit()
                return dict(row)
            return None

    def audit(self, conn: ConnectionWrapper, user_id: int | None, action: str, entity_type: str, entity_id: int | None, details: str) -> None:
        conn.execute(
            "INSERT INTO audit_log (user_id, action, entity_type, entity_id, details, created_at) VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, action, entity_type, entity_id, details, now_str()),
        )

    def register_document(
        self,
        conn: ConnectionWrapper,
        *,
        entity_type: str,
        entity_id: int,
        doc_type: str,
        doc_number: str,
        doc_date: str,
        status: str,
        title: str,
        counterparty: str = "",
        object_name: str = "",
        created_by: int | None = None,
        file_path: str = "",
        metadata: dict[str, Any] | None = None,
        search_text: str = "",
    ) -> None:
        text = " ".join(part for part in [doc_type, doc_number, title, counterparty, object_name, search_text] if part)
        payload = json.dumps(metadata or {}, ensure_ascii=False)
        conn.execute(
            """
            INSERT INTO documents
            (entity_type, entity_id, doc_type, doc_number, doc_date, status, title, counterparty, object_name, created_by, file_path, metadata_json, search_text, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity_type, entity_id) DO UPDATE SET
                doc_type = excluded.doc_type,
                doc_number = excluded.doc_number,
                doc_date = excluded.doc_date,
                status = excluded.status,
                title = excluded.title,
                counterparty = excluded.counterparty,
                object_name = excluded.object_name,
                created_by = excluded.created_by,
                file_path = CASE WHEN excluded.file_path = '' THEN documents.file_path ELSE excluded.file_path END,
                metadata_json = excluded.metadata_json,
                search_text = excluded.search_text,
                updated_at = excluded.updated_at
            """,
            (
                entity_type,
                entity_id,
                doc_type,
                doc_number,
                doc_date,
                status,
                title,
                counterparty,
                object_name,
                created_by,
                file_path,
                payload,
                text.strip(),
                now_str(),
                now_str(),
            ),
        )

    def generate_number(self, conn: ConnectionWrapper, prefix: str) -> str:
        date_part = datetime.now().strftime("%Y%m%d")
        pattern = f"{prefix}-{date_part}-%"
        count = conn.execute("SELECT COUNT(*) FROM documents WHERE doc_number LIKE ?", (pattern,)).fetchone()[0]
        return f"{prefix}-{date_part}-{count + 1:03d}"

    def save_simple_record(self, entity: str, data: dict[str, str], user_id: int) -> int:
        table = SIMPLE_TABLES[entity]
        clean = {key: (None if value == "" else value) for key, value in data.items()}
        created_at = now_str()
        with self.connect() as conn:
            if entity == "users":
                password = clean.pop("password", None) or "changeme123"
                clean["password_hash"] = hash_password(password)
                clean["is_active"] = str(clean.get("is_active", "0")) in {"1", "true", "True"}
                clean["created_at"] = created_at
            else:
                if "is_ppe" in clean:
                    clean["is_ppe"] = str(clean.get("is_ppe", "0")) in {"1", "true", "True"}
                clean["created_at"] = created_at
            fields = ", ".join(clean.keys())
            placeholders = ", ".join("?" for _ in clean)
            values = tuple(clean.values())
            cur = conn.execute(f"INSERT INTO {table} ({fields}) VALUES ({placeholders})", values)
            entity_id = int(cur.lastrowid or 0)
            if entity == "contracts":
                self.register_document(
                    conn,
                    entity_type="smr_contract",
                    entity_id=entity_id,
                    doc_type="Договор СМР",
                    doc_number=str(clean["number"]),
                    doc_date=str(clean["contract_date"]),
                    status=str(clean.get("status") or "draft"),
                    title="Договор на выполнение СМР",
                    counterparty=str(clean.get("customer_name") or ""),
                    object_name=self._lookup_object_name(conn, clean.get("object_id")),
                    created_by=user_id,
                    metadata={"amount": clean.get("amount")},
                )
            if entity == "supply_contracts":
                self.register_document(
                    conn,
                    entity_type="supply_contract",
                    entity_id=entity_id,
                    doc_type="Договор поставки",
                    doc_number=str(clean["number"]),
                    doc_date=str(clean["contract_date"]),
                    status=str(clean.get("status") or "draft"),
                    title="Договор поставки материалов",
                    counterparty=self._lookup_supplier_name(conn, clean.get("supplier_id")),
                    created_by=user_id,
                    metadata={"amount": clean.get("amount")},
                )
            self.audit(conn, user_id, "create", table, entity_id, f"Создана запись в {table}")
            conn.commit()
            return entity_id

    def _lookup_object_name(self, conn: ConnectionWrapper, object_id: Any) -> str:
        if not object_id:
            return ""
        row = conn.execute("SELECT name FROM construction_objects WHERE id = ?", (object_id,)).fetchone()
        return row["name"] if row else ""

    def _lookup_supplier_name(self, conn: ConnectionWrapper, supplier_id: Any) -> str:
        if not supplier_id:
            return ""
        row = conn.execute("SELECT name FROM suppliers WHERE id = ?", (supplier_id,)).fetchone()
        return row["name"] if row else ""

    def get_lookup_data(self) -> dict[str, list[dict[str, Any]]]:
        with self.connect() as conn:
            return {
                "suppliers": [dict(row) for row in conn.execute("SELECT id, name FROM suppliers ORDER BY name").fetchall()],
                "materials": [dict(row) for row in conn.execute("SELECT id, code, name, unit, price, is_ppe FROM materials ORDER BY code").fetchall()],
                "objects": [dict(row) for row in conn.execute("SELECT id, name FROM construction_objects ORDER BY name").fetchall()],
                "contracts": [
                    dict(row)
                    for row in conn.execute(
                        "SELECT c.id, c.number, c.subject, o.name AS object_name FROM smr_contracts c LEFT JOIN construction_objects o ON o.id = c.object_id ORDER BY c.contract_date DESC"
                    ).fetchall()
                ],
                "supply_contracts": [dict(row) for row in conn.execute("SELECT id, number FROM supply_contracts ORDER BY contract_date DESC").fetchall()],
                "workers": [dict(row) for row in conn.execute("SELECT id, full_name, employee_number FROM workers ORDER BY full_name").fetchall()],
                "users": [dict(row) for row in conn.execute("SELECT id, full_name, role FROM users ORDER BY full_name").fetchall()],
            }

    def list_entity(self, entity: str) -> list[dict[str, Any]]:
        queries = {
            "materials": "SELECT * FROM materials ORDER BY code",
            "suppliers": "SELECT * FROM suppliers ORDER BY name",
            "objects": "SELECT * FROM construction_objects ORDER BY name",
            "workers": "SELECT * FROM workers ORDER BY full_name",
            "norms": """
                SELECT n.id, n.work_type, n.norm_per_unit, n.unit, n.notes, m.code AS material_code, m.name AS material_name
                FROM material_norms n
                JOIN materials m ON m.id = n.material_id
                ORDER BY n.work_type, m.code
            """,
            "contracts": """
                SELECT c.*, o.name AS object_name
                FROM smr_contracts c
                LEFT JOIN construction_objects o ON o.id = c.object_id
                ORDER BY c.contract_date DESC
            """,
            "supply_contracts": """
                SELECT c.*, s.name AS supplier_name, sc.number AS smr_number
                FROM supply_contracts c
                JOIN suppliers s ON s.id = c.supplier_id
                LEFT JOIN smr_contracts sc ON sc.id = c.related_smr_contract_id
                ORDER BY c.contract_date DESC
            """,
            "users": """
                SELECT u.id, u.username, u.full_name, u.role, u.site_name, u.is_active, s.name AS supplier_name
                FROM users u
                LEFT JOIN suppliers s ON s.id = u.supplier_id
                ORDER BY u.created_at DESC
            """,
        }
        return self.fetch_all(queries[entity])

    def parse_line_items(self, raw_text: str, require_price: bool = False) -> list[dict[str, Any]]:
        lines = []
        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split("|")]
            if len(parts) < 2:
                raise ValueError(f"Некорректная строка: {line}")
            item = {
                "material_code": parts[0],
                "quantity": parse_float(parts[1]),
                "unit_price": parse_float(parts[2], 0.0) if len(parts) >= 3 and parts[2] else 0.0,
                "notes": parts[3] if len(parts) >= 4 else "",
            }
            if require_price and item["unit_price"] <= 0:
                raise ValueError(f"Для строки '{line}' нужна цена.")
            lines.append(item)
        if not lines:
            raise ValueError("Не заполнены позиции документа.")
        return lines

    def create_procurement_request(self, user: dict[str, Any], form: dict[str, str]) -> int:
        with self.connect() as conn:
            number = form.get("number") or self.generate_number(conn, "REQ")
            request_date = form.get("request_date") or today_str()
            status = form.get("status") or "draft"
            cur = conn.execute(
                """
                INSERT INTO procurement_requests (number, request_date, site_name, contract_id, supplier_id, requested_by, status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    number,
                    request_date,
                    form.get("site_name") or user.get("site_name") or "Участок",
                    form.get("contract_id") or None,
                    form.get("supplier_id") or None,
                    user["id"],
                    status,
                    form.get("notes", ""),
                    now_str(),
                ),
            )
            request_id = int(cur.lastrowid or 0)
            items = self.parse_line_items(form.get("items", ""), require_price=False)
            for item in items:
                material = conn.execute("SELECT id, name FROM materials WHERE code = ?", (item["material_code"],)).fetchone()
                if not material:
                    raise ValueError(f"Материал с кодом {item['material_code']} не найден.")
                conn.execute(
                    """
                    INSERT INTO procurement_request_lines (request_id, material_id, quantity, unit_price, notes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (request_id, material["id"], item["quantity"], item["unit_price"], item["notes"]),
                )
            contract = conn.execute("SELECT number FROM smr_contracts WHERE id = ?", (form.get("contract_id"),)).fetchone() if form.get("contract_id") else None
            supplier = conn.execute("SELECT name FROM suppliers WHERE id = ?", (form.get("supplier_id"),)).fetchone() if form.get("supplier_id") else None
            self.register_document(
                conn,
                entity_type="procurement_request",
                entity_id=request_id,
                doc_type="Заявка поставщику",
                doc_number=number,
                doc_date=request_date,
                status=status,
                title="Заявка на закупку материалов",
                counterparty=supplier["name"] if supplier else "",
                object_name=contract["number"] if contract else "",
                created_by=user["id"],
                search_text=form.get("notes", ""),
            )
            self.audit(conn, user["id"], "create", "procurement_request", request_id, f"Создана заявка {number}")
            conn.commit()
            return request_id

    def list_procurement_requests(self, *, supplier_scope: int | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if supplier_scope:
            where = "WHERE r.supplier_id = ?"
            params.append(supplier_scope)
        return self.fetch_all(
            f"""
            SELECT r.*, c.number AS contract_number, s.name AS supplier_name, u.full_name AS author_name
            FROM procurement_requests r
            LEFT JOIN smr_contracts c ON c.id = r.contract_id
            LEFT JOIN suppliers s ON s.id = r.supplier_id
            JOIN users u ON u.id = r.requested_by
            {where}
            ORDER BY r.request_date DESC, r.id DESC
            """,
            tuple(params),
        )

    def request_lines(self, request_id: int) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT l.quantity, l.unit_price, l.notes, m.code AS material_code, m.name AS material_name, m.unit
            FROM procurement_request_lines l
            JOIN materials m ON m.id = l.material_id
            WHERE l.request_id = ?
            ORDER BY m.code
            """,
            (request_id,),
        )

    def create_supplier_document(self, user: dict[str, Any], form: dict[str, str], file_payload: dict[str, Any] | None) -> int:
        with self.connect() as conn:
            doc_number = form.get("doc_number") or self.generate_number(conn, "SUPDOC")
            doc_date = form.get("doc_date") or today_str()
            stored_path = ""
            file_name = ""
            if file_payload and file_payload.get("filename"):
                safe_name = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{Path(file_payload['filename']).name}"
                target = CONFIG.uploads_dir / safe_name
                target.write_bytes(file_payload["content"])
                stored_path = str(target)
                file_name = Path(file_payload["filename"]).name
            cur = conn.execute(
                """
                INSERT INTO supplier_documents
                (supplier_id, request_id, supply_contract_id, doc_type, doc_number, doc_date, amount, vat_amount, uploaded_by, file_name, stored_path, status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user["supplier_id"] or form.get("supplier_id"),
                    form.get("request_id") or None,
                    form.get("supply_contract_id") or None,
                    form.get("doc_type") or "Счет",
                    doc_number,
                    doc_date,
                    parse_float(form.get("amount"), 0.0),
                    parse_float(form.get("vat_amount"), 0.0),
                    user["id"],
                    file_name,
                    stored_path,
                    "uploaded",
                    form.get("notes", ""),
                    now_str(),
                ),
            )
            entity_id = int(cur.lastrowid or 0)
            supplier_name = self._lookup_supplier_name(conn, user.get("supplier_id") or form.get("supplier_id"))
            self.register_document(
                conn,
                entity_type="supplier_document",
                entity_id=entity_id,
                doc_type=form.get("doc_type") or "Счет",
                doc_number=doc_number,
                doc_date=doc_date,
                status="uploaded",
                title="Документ поставки",
                counterparty=supplier_name,
                created_by=user["id"],
                file_path=stored_path,
                metadata={"amount": form.get("amount"), "file_name": file_name},
                search_text=form.get("notes", ""),
            )
            self.audit(conn, user["id"], "upload", "supplier_document", entity_id, f"Загружен документ {doc_number}")
            conn.commit()
            return entity_id

    def list_supplier_documents(self, supplier_scope: int | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        where = ""
        if supplier_scope:
            where = "WHERE d.supplier_id = ?"
            params.append(supplier_scope)
        return self.fetch_all(
            f"""
            SELECT d.*, s.name AS supplier_name, r.number AS request_number
            FROM supplier_documents d
            JOIN suppliers s ON s.id = d.supplier_id
            LEFT JOIN procurement_requests r ON r.id = d.request_id
            {where}
            ORDER BY d.doc_date DESC, d.id DESC
            """,
            tuple(params),
        )

    def create_stock_receipt(self, user: dict[str, Any], form: dict[str, str]) -> int:
        with self.connect() as conn:
            number = form.get("number") or self.generate_number(conn, "REC")
            receipt_date = form.get("receipt_date") or today_str()
            status = form.get("status") or "draft"
            cur = conn.execute(
                """
                INSERT INTO stock_receipts (number, receipt_date, supplier_id, supplier_document_id, created_by, status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    number,
                    receipt_date,
                    form.get("supplier_id"),
                    form.get("supplier_document_id") or None,
                    user["id"],
                    status,
                    form.get("notes", ""),
                    now_str(),
                ),
            )
            receipt_id = int(cur.lastrowid or 0)
            items = self.parse_line_items(form.get("items", ""), require_price=False)
            for item in items:
                material = conn.execute("SELECT id, name, price FROM materials WHERE code = ?", (item["material_code"],)).fetchone()
                if not material:
                    raise ValueError(f"Материал с кодом {item['material_code']} не найден.")
                unit_price = item["unit_price"] or material["price"]
                conn.execute(
                    "INSERT INTO stock_receipt_lines (receipt_id, material_id, quantity, unit_price, notes) VALUES (?, ?, ?, ?, ?)",
                    (receipt_id, material["id"], item["quantity"], unit_price, item["notes"]),
                )
                conn.execute(
                    """
                    INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (receipt_date, material["id"], item["quantity"], CONFIG.warehouse_name, "stock_receipt", receipt_id, unit_price, user["id"], form.get("notes", "")),
                )
            supplier = conn.execute("SELECT name FROM suppliers WHERE id = ?", (form.get("supplier_id"),)).fetchone()
            self.register_document(
                conn,
                entity_type="stock_receipt",
                entity_id=receipt_id,
                doc_type="Приходный ордер",
                doc_number=number,
                doc_date=receipt_date,
                status=status,
                title="Приходный ордер на склад",
                counterparty=supplier["name"] if supplier else "",
                object_name=CONFIG.warehouse_name,
                created_by=user["id"],
                search_text=form.get("notes", ""),
            )
            self.audit(conn, user["id"], "create", "stock_receipt", receipt_id, f"Создан приход {number}")
            conn.commit()
            return receipt_id

    def list_stock_receipts(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT r.*, s.name AS supplier_name
            FROM stock_receipts r
            JOIN suppliers s ON s.id = r.supplier_id
            ORDER BY r.receipt_date DESC, r.id DESC
            """
        )

    def receipt_lines(self, receipt_id: int) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT l.quantity, l.unit_price, l.notes, m.code AS material_code, m.name AS material_name, m.unit
            FROM stock_receipt_lines l
            JOIN materials m ON m.id = l.material_id
            WHERE l.receipt_id = ?
            ORDER BY m.code
            """,
            (receipt_id,),
        )

    def create_stock_issue(self, user: dict[str, Any], form: dict[str, str]) -> int:
        with self.connect() as conn:
            number = form.get("number") or self.generate_number(conn, "ISS")
            issue_date = form.get("issue_date") or today_str()
            status = form.get("status") or "draft"
            site_name = form.get("site_name") or "Участок"
            cur = conn.execute(
                """
                INSERT INTO stock_issues (number, issue_date, site_name, contract_id, issued_by, received_by_name, status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    number,
                    issue_date,
                    site_name,
                    form.get("contract_id") or None,
                    user["id"],
                    form.get("received_by_name") or "",
                    status,
                    form.get("notes", ""),
                    now_str(),
                ),
            )
            issue_id = int(cur.lastrowid or 0)
            items = self.parse_line_items(form.get("items", ""), require_price=False)
            for item in items:
                material = conn.execute("SELECT id, name, price FROM materials WHERE code = ?", (item["material_code"],)).fetchone()
                if not material:
                    raise ValueError(f"Материал с кодом {item['material_code']} не найден.")
                unit_price = item["unit_price"] or material["price"]
                conn.execute(
                    "INSERT INTO stock_issue_lines (issue_id, material_id, quantity, unit_price, notes) VALUES (?, ?, ?, ?, ?)",
                    (issue_id, material["id"], item["quantity"], unit_price, item["notes"]),
                )
                conn.execute(
                    """
                    INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (issue_date, material["id"], -item["quantity"], CONFIG.warehouse_name, "stock_issue", issue_id, unit_price, user["id"], form.get("notes", "")),
                )
                conn.execute(
                    """
                    INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (issue_date, material["id"], item["quantity"], site_name, "stock_issue", issue_id, unit_price, user["id"], form.get("notes", "")),
                )
            contract = conn.execute("SELECT number FROM smr_contracts WHERE id = ?", (form.get("contract_id"),)).fetchone() if form.get("contract_id") else None
            self.register_document(
                conn,
                entity_type="stock_issue",
                entity_id=issue_id,
                doc_type="Требование-накладная",
                doc_number=number,
                doc_date=issue_date,
                status=status,
                title="Отпуск материалов со склада",
                counterparty=form.get("received_by_name") or "",
                object_name=contract["number"] if contract else site_name,
                created_by=user["id"],
                search_text=form.get("notes", ""),
            )
            self.audit(conn, user["id"], "create", "stock_issue", issue_id, f"Создан отпуск {number}")
            conn.commit()
            return issue_id

    def list_stock_issues(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT i.*, c.number AS contract_number
            FROM stock_issues i
            LEFT JOIN smr_contracts c ON c.id = i.contract_id
            ORDER BY i.issue_date DESC, i.id DESC
            """
        )

    def issue_lines(self, issue_id: int) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT l.quantity, l.unit_price, l.notes, m.code AS material_code, m.name AS material_name, m.unit
            FROM stock_issue_lines l
            JOIN materials m ON m.id = l.material_id
            WHERE l.issue_id = ?
            ORDER BY m.code
            """,
            (issue_id,),
        )

    def create_work_log(self, user: dict[str, Any], form: dict[str, str]) -> int:
        return self.execute(
            """
            INSERT INTO work_logs (site_name, contract_id, work_type, planned_volume, actual_volume, volume_unit, plan_date, actual_date, status, notes, created_by, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                form.get("site_name") or user.get("site_name") or "Участок",
                form.get("contract_id") or None,
                form.get("work_type") or "",
                parse_float(form.get("planned_volume"), 0.0),
                parse_float(form.get("actual_volume"), 0.0),
                form.get("volume_unit") or "",
                form.get("plan_date") or None,
                form.get("actual_date") or None,
                form.get("status") or "planned",
                form.get("notes", ""),
                user["id"],
                now_str(),
            ),
        )

    def list_work_logs(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT w.*, c.number AS contract_number
            FROM work_logs w
            LEFT JOIN smr_contracts c ON c.id = w.contract_id
            ORDER BY COALESCE(w.actual_date, w.plan_date) DESC, w.id DESC
            """
        )

    def create_write_off(self, user: dict[str, Any], form: dict[str, str]) -> int:
        with self.connect() as conn:
            number = form.get("number") or self.generate_number(conn, "WO")
            act_date = form.get("act_date") or today_str()
            work_type = form.get("work_type") or ""
            work_volume = parse_float(form.get("work_volume"), 0.0)
            if work_volume <= 0:
                raise ValueError("Объем работ должен быть больше нуля.")
            status = form.get("status") or "draft"
            cur = conn.execute(
                """
                INSERT INTO write_off_acts (number, act_date, contract_id, site_name, work_type, work_volume, volume_unit, created_by, status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    number,
                    act_date,
                    form.get("contract_id"),
                    form.get("site_name") or user.get("site_name") or "Участок",
                    work_type,
                    work_volume,
                    form.get("volume_unit") or "",
                    user["id"],
                    status,
                    form.get("notes", ""),
                    now_str(),
                ),
            )
            act_id = int(cur.lastrowid or 0)
            norms = conn.execute(
                """
                SELECT n.norm_per_unit, n.notes, m.id AS material_id, m.code, m.name, m.price
                FROM material_norms n
                JOIN materials m ON m.id = n.material_id
                WHERE n.work_type = ?
                ORDER BY m.code
                """,
                (work_type,),
            ).fetchall()
            if not norms:
                raise ValueError("Для выбранного вида работ не настроены нормы расхода.")
            site_name = form.get("site_name") or user.get("site_name") or "Участок"
            for norm in norms:
                quantity = round(work_volume * norm["norm_per_unit"], 3)
                conn.execute(
                    """
                    INSERT INTO write_off_lines (act_id, material_id, norm_per_unit, calculated_quantity, actual_quantity, unit_price, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (act_id, norm["material_id"], norm["norm_per_unit"], quantity, quantity, norm["price"], norm["notes"] or ""),
                )
                conn.execute(
                    """
                    INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (act_date, norm["material_id"], -quantity, site_name, "write_off", act_id, norm["price"], user["id"], f"Списание по норме: {work_type}"),
                )
            contract = conn.execute(
                """
                SELECT c.number, o.name AS object_name
                FROM smr_contracts c
                LEFT JOIN construction_objects o ON o.id = c.object_id
                WHERE c.id = ?
                """,
                (form.get("contract_id"),),
            ).fetchone()
            self.register_document(
                conn,
                entity_type="write_off",
                entity_id=act_id,
                doc_type="Акт списания",
                doc_number=number,
                doc_date=act_date,
                status=status,
                title="Акт списания материалов",
                counterparty=contract["number"] if contract else "",
                object_name=contract["object_name"] if contract else site_name,
                created_by=user["id"],
                metadata={"work_type": work_type, "work_volume": work_volume},
                search_text=form.get("notes", ""),
            )
            self.audit(conn, user["id"], "create", "write_off", act_id, f"Создан акт списания {number}")
            conn.commit()
            return act_id

    def list_write_offs(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT a.*, c.number AS contract_number, o.name AS object_name
            FROM write_off_acts a
            JOIN smr_contracts c ON c.id = a.contract_id
            LEFT JOIN construction_objects o ON o.id = c.object_id
            ORDER BY a.act_date DESC, a.id DESC
            """
        )

    def write_off_lines(self, act_id: int) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT l.norm_per_unit, l.calculated_quantity, l.actual_quantity, l.unit_price, l.notes,
                   m.code AS material_code, m.name AS material_name, m.unit
            FROM write_off_lines l
            JOIN materials m ON m.id = l.material_id
            WHERE l.act_id = ?
            ORDER BY m.code
            """,
            (act_id,),
        )

    def create_ppe_issuance(self, user: dict[str, Any], form: dict[str, str]) -> int:
        with self.connect() as conn:
            number = form.get("number") or self.generate_number(conn, "PPE")
            issue_date = form.get("issue_date") or today_str()
            status = form.get("status") or "draft"
            site_name = form.get("site_name") or user.get("site_name") or "Участок"
            cur = conn.execute(
                """
                INSERT INTO ppe_issuances (number, issue_date, site_name, season, issued_by, status, notes, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (number, issue_date, site_name, form.get("season") or "", user["id"], status, form.get("notes", ""), now_str()),
            )
            issuance_id = int(cur.lastrowid or 0)
            for raw_line in form.get("items", "").splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                parts = [part.strip() for part in line.split("|")]
                if len(parts) < 4:
                    raise ValueError("Строки спецодежды должны иметь вид: табельный номер | код материала | количество | срок службы")
                employee_number, material_code = parts[0], parts[1]
                quantity, service_life = parse_float(parts[2]), int(parse_float(parts[3], 0))
                worker = conn.execute("SELECT id, full_name FROM workers WHERE employee_number = ?", (employee_number,)).fetchone()
                material = conn.execute("SELECT id, price FROM materials WHERE code = ? AND is_ppe = TRUE", (material_code,)).fetchone()
                if not worker:
                    raise ValueError(f"Работник с табельным номером {employee_number} не найден.")
                if not material:
                    raise ValueError(f"Материал {material_code} не найден в перечне СИЗ.")
                conn.execute(
                    """
                    INSERT INTO ppe_issuance_lines (issuance_id, worker_id, material_id, quantity, service_life_months, issue_start_date, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (issuance_id, worker["id"], material["id"], quantity, service_life, issue_date, ""),
                )
                conn.execute(
                    """
                    INSERT INTO stock_movements (movement_date, material_id, quantity_delta, location_name, source_type, source_id, unit_price, created_by, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (issue_date, material["id"], -quantity, CONFIG.warehouse_name, "ppe_issuance", issuance_id, material["price"], user["id"], f"Выдача {worker['full_name']}"),
                )
            self.register_document(
                conn,
                entity_type="ppe_issuance",
                entity_id=issuance_id,
                doc_type="Ведомость спецодежды",
                doc_number=number,
                doc_date=issue_date,
                status=status,
                title="Ведомость учета выдачи спецодежды",
                counterparty=site_name,
                object_name=site_name,
                created_by=user["id"],
                search_text=form.get("notes", ""),
            )
            self.audit(conn, user["id"], "create", "ppe_issuance", issuance_id, f"Создана ведомость {number}")
            conn.commit()
            return issuance_id

    def list_ppe_issuances(self) -> list[dict[str, Any]]:
        return self.fetch_all("SELECT * FROM ppe_issuances ORDER BY issue_date DESC, id DESC")

    def ppe_lines(self, issuance_id: int) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT l.quantity, l.service_life_months, l.issue_start_date, w.full_name, w.employee_number,
                   m.code AS material_code, m.name AS material_name, m.unit
            FROM ppe_issuance_lines l
            JOIN workers w ON w.id = l.worker_id
            JOIN materials m ON m.id = l.material_id
            WHERE l.issuance_id = ?
            ORDER BY w.full_name, m.code
            """,
            (issuance_id,),
        )

    def stock_balances(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT m.id, m.code, m.name, m.unit, m.min_stock, COALESCE(SUM(CASE WHEN sm.location_name = ? THEN sm.quantity_delta END), 0) AS warehouse_balance
            FROM materials m
            LEFT JOIN stock_movements sm ON sm.material_id = m.id
            GROUP BY m.id, m.code, m.name, m.unit, m.min_stock
            ORDER BY m.code
            """,
            (CONFIG.warehouse_name,),
        )

    def site_balances(self) -> list[dict[str, Any]]:
        return self.fetch_all(
            """
            SELECT sm.location_name, m.code, m.name, m.unit, SUM(sm.quantity_delta) AS quantity
            FROM stock_movements sm
            JOIN materials m ON m.id = sm.material_id
            WHERE sm.location_name <> ?
            GROUP BY sm.location_name, m.code, m.name, m.unit
            HAVING SUM(sm.quantity_delta) <> 0
            ORDER BY sm.location_name, m.code
            """,
            (CONFIG.warehouse_name,),
        )

    def low_stock_alerts(self) -> list[dict[str, Any]]:
        return [row for row in self.stock_balances() if float(row["warehouse_balance"]) <= float(row["min_stock"])]

    def dashboard_metrics(self) -> dict[str, Any]:
        with self.connect() as conn:
            total_contracts = conn.execute("SELECT COUNT(*) FROM smr_contracts").fetchone()[0]
            pending_docs = conn.execute("SELECT COUNT(*) FROM documents WHERE status IN ('draft', 'approval', 'rework', 'uploaded')").fetchone()[0]
            supplier_docs = conn.execute("SELECT COUNT(*) FROM supplier_documents").fetchone()[0]
            site_tasks = conn.execute("SELECT COUNT(*) FROM work_logs WHERE status IN ('planned', 'delayed')").fetchone()[0]
        return {
            "contracts": total_contracts,
            "pending": pending_docs,
            "supplier_docs": supplier_docs,
            "site_tasks": site_tasks,
            "alerts": len(self.low_stock_alerts()),
        }

    def search_documents(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        conditions = ["1=1"]
        params: list[Any] = []
        if filters.get("doc_type"):
            conditions.append("doc_type = ?")
            params.append(filters["doc_type"])
        if filters.get("status"):
            conditions.append("status = ?")
            params.append(filters["status"])
        if filters.get("date_from"):
            conditions.append("doc_date >= ?")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            conditions.append("doc_date <= ?")
            params.append(filters["date_to"])
        if filters.get("query"):
            conditions.append("search_text LIKE ?")
            params.append(f"%{filters['query']}%")
        where = " AND ".join(conditions)
        return self.fetch_all(
            f"""
            SELECT d.*, u.full_name AS author_name
            FROM documents d
            LEFT JOIN users u ON u.id = d.created_by
            WHERE {where}
            ORDER BY d.doc_date DESC, d.id DESC
            """,
            tuple(params),
        )

    def transition_document(self, user: dict[str, Any], entity_type: str, entity_id: int, new_status: str) -> None:
        with self.connect() as conn:
            doc = conn.execute("SELECT * FROM documents WHERE entity_type = ? AND entity_id = ?", (entity_type, entity_id)).fetchone()
            if not doc:
                raise ValueError("Документ не найден.")
            table_map = {
                "smr_contract": "smr_contracts",
                "supply_contract": "supply_contracts",
                "procurement_request": "procurement_requests",
                "supplier_document": "supplier_documents",
                "stock_receipt": "stock_receipts",
                "stock_issue": "stock_issues",
                "write_off": "write_off_acts",
                "ppe_issuance": "ppe_issuances",
            }
            table_name = table_map.get(entity_type)
            if not table_name:
                raise ValueError("Для этого документа переход недоступен.")
            conn.execute(f"UPDATE {table_name} SET status = ? WHERE id = ?", (new_status, entity_id))
            self.register_document(
                conn,
                entity_type=entity_type,
                entity_id=entity_id,
                doc_type=doc["doc_type"],
                doc_number=doc["doc_number"],
                doc_date=doc["doc_date"],
                status=new_status,
                title=doc["title"],
                counterparty=doc["counterparty"] or "",
                object_name=doc["object_name"] or "",
                created_by=doc["created_by"],
                file_path=doc["file_path"] or "",
                metadata=json.loads(doc["metadata_json"] or "{}"),
                search_text=doc["search_text"] or "",
            )
            self.audit(conn, user["id"], "status_change", entity_type, entity_id, f"{doc['status']} -> {new_status}")
            conn.commit()

    def update_document_file(self, entity_type: str, entity_id: int, file_path: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE documents SET file_path = ?, updated_at = ? WHERE entity_type = ? AND entity_id = ?",
                (file_path, now_str(), entity_type, entity_id),
            )
            conn.commit()

    def report_period(self, filters: dict[str, str]) -> tuple[str, str]:
        date_from = filters.get("date_from") or datetime.now().replace(day=1).date().isoformat()
        date_to = filters.get("date_to") or today_str()
        return date_from, date_to

    def report_stock(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        return self.stock_balances()

    def report_purchases(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        date_from, date_to = self.report_period(filters)
        return self.fetch_all(
            """
            SELECT d.doc_date, d.doc_type, d.doc_number, s.name AS supplier_name, d.amount, d.vat_amount,
                   COALESCE(r.number, '-') AS request_number
            FROM supplier_documents d
            JOIN suppliers s ON s.id = d.supplier_id
            LEFT JOIN procurement_requests r ON r.id = d.request_id
            WHERE d.doc_date BETWEEN ? AND ?
            ORDER BY d.doc_date DESC
            """,
            (date_from, date_to),
        )

    def report_write_offs(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        date_from, date_to = self.report_period(filters)
        return self.fetch_all(
            """
            SELECT a.act_date, a.number, c.number AS contract_number, o.name AS object_name, a.work_type,
                   m.code AS material_code, m.name AS material_name, l.actual_quantity, m.unit
            FROM write_off_acts a
            JOIN write_off_lines l ON l.act_id = a.id
            JOIN materials m ON m.id = l.material_id
            JOIN smr_contracts c ON c.id = a.contract_id
            LEFT JOIN construction_objects o ON o.id = c.object_id
            WHERE a.act_date BETWEEN ? AND ?
            ORDER BY a.act_date DESC, a.number
            """,
            (date_from, date_to),
        )

    def report_work_logs(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        date_from, date_to = self.report_period(filters)
        return self.fetch_all(
            """
            SELECT w.site_name, c.number AS contract_number, w.work_type, w.planned_volume, w.actual_volume,
                   w.volume_unit, w.plan_date, w.actual_date, w.status
            FROM work_logs w
            LEFT JOIN smr_contracts c ON c.id = w.contract_id
            WHERE COALESCE(w.actual_date, w.plan_date) BETWEEN ? AND ?
            ORDER BY COALESCE(w.actual_date, w.plan_date) DESC
            """,
            (date_from, date_to),
        )

    def report_summary(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        date_from, date_to = self.report_period(filters)
        return self.fetch_all(
            """
            SELECT 'Договор СМР' AS section, number AS item, amount AS amount, status, contract_date AS event_date
            FROM smr_contracts
            WHERE contract_date BETWEEN ? AND ?
            UNION ALL
            SELECT 'Документы поставщиков', doc_number, amount, status, doc_date
            FROM supplier_documents
            WHERE doc_date BETWEEN ? AND ?
            UNION ALL
            SELECT 'Акты списания', number, work_volume, status, act_date
            FROM write_off_acts
            WHERE act_date BETWEEN ? AND ?
            ORDER BY event_date DESC
            """,
            (date_from, date_to, date_from, date_to, date_from, date_to),
        )

    def report_ppe(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        date_from, date_to = self.report_period(filters)
        return self.fetch_all(
            """
            SELECT p.issue_date, p.number, p.site_name, w.full_name, w.employee_number, m.name AS material_name, l.quantity, l.service_life_months
            FROM ppe_issuances p
            JOIN ppe_issuance_lines l ON l.issuance_id = p.id
            JOIN workers w ON w.id = l.worker_id
            JOIN materials m ON m.id = l.material_id
            WHERE p.issue_date BETWEEN ? AND ?
            ORDER BY p.issue_date DESC, w.full_name
            """,
            (date_from, date_to),
        )

    def create_backup(self, user_id: int) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        target = CONFIG.backups_dir / f"ais_backup_{timestamp}.json"
        with self.connect() as conn:
            payload = {
                "created_at": now_str(),
                "database": CONFIG.postgres_db,
                "tables": {table: [dict(row) for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1").fetchall()] for table in BACKUP_TABLES},
            }
            target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            self.audit(conn, user_id, "backup", "database", None, f"Создана резервная копия {target.name}")
            conn.commit()
        return target

    def list_backups(self) -> list[dict[str, Any]]:
        backups = []
        for path in sorted(CONFIG.backups_dir.glob("*.json"), reverse=True):
            stat = path.stat()
            backups.append(
                {
                    "name": path.name,
                    "path": str(path),
                    "size": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                }
            )
        return backups

    def restore_backup(self, backup_name: str, user_id: int) -> None:
        source = CONFIG.backups_dir / backup_name
        if not source.exists():
            raise ValueError("Файл резервной копии не найден.")
        payload = json.loads(source.read_text(encoding="utf-8"))
        with self.connect() as conn:
            conn.execute("TRUNCATE TABLE " + ", ".join(BACKUP_TABLES) + " RESTART IDENTITY CASCADE")
            for table in BACKUP_TABLES:
                rows = payload.get("tables", {}).get(table, [])
                if not rows:
                    continue
                columns = list(rows[0].keys())
                column_sql = ", ".join(columns)
                placeholder_sql = ", ".join("?" for _ in columns)
                values = [tuple(row.get(column) for column in columns) for row in rows]
                conn.executemany(f"INSERT INTO {table} ({column_sql}) VALUES ({placeholder_sql})", values)
            self.reset_sequences(conn)
            self.audit(conn, user_id, "restore", "database", None, f"Восстановление из {backup_name}")
            conn.commit()

    def reset_sequences(self, conn: ConnectionWrapper) -> None:
        for table in SERIAL_TABLES:
            seq_row = conn.execute("SELECT pg_get_serial_sequence(?, 'id') AS seq_name", (table,)).fetchone()
            if not seq_row or not seq_row["seq_name"]:
                continue
            stats = conn.execute(f"SELECT COALESCE(MAX(id), 1) AS max_id, COUNT(*) AS row_count FROM {table}").fetchone()
            conn.execute("SELECT setval(?, ?, ?)", (seq_row["seq_name"], int(stats["max_id"]), bool(stats["row_count"])))
