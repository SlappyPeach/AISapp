from __future__ import annotations

import traceback
from dataclasses import dataclass
from html import escape
from http.cookies import SimpleCookie
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode

from .config import CONFIG, ROLE_LABELS
from .database import Database, now_str
from .documents import Exporter, money
from .ui import action_form, layout, metric_card, panel, status_badge, table, url


@dataclass
class UploadedFile:
    filename: str
    content: bytes
    content_type: str


def parse_multipart(body: bytes, content_type: str) -> tuple[dict[str, str], dict[str, UploadedFile]]:
    form: dict[str, str] = {}
    files: dict[str, UploadedFile] = {}
    boundary_marker = "boundary="
    if boundary_marker not in content_type:
        return form, files
    boundary = content_type.split(boundary_marker, 1)[1].encode("utf-8")
    delimiter = b"--" + boundary
    for chunk in body.split(delimiter):
        part = chunk.strip(b"\r\n")
        if not part or part == b"--":
            continue
        header_blob, _, content = part.partition(b"\r\n\r\n")
        if not header_blob:
            continue
        headers: dict[str, str] = {}
        for raw_header in header_blob.decode("utf-8", "ignore").split("\r\n"):
            if ":" in raw_header:
                key, value = raw_header.split(":", 1)
                headers[key.strip().lower()] = value.strip()
        disposition = headers.get("content-disposition", "")
        if "name=" not in disposition:
            continue
        name = disposition.split('name="', 1)[1].split('"', 1)[0]
        payload = content.rstrip(b"\r\n")
        if 'filename="' in disposition:
            filename = disposition.split('filename="', 1)[1].split('"', 1)[0]
            files[name] = UploadedFile(
                filename=filename,
                content=payload,
                content_type=headers.get("content-type", "application/octet-stream"),
            )
        else:
            form[name] = payload.decode("utf-8", "ignore")
    return form, files


class Request:
    def __init__(self, environ: dict[str, Any]) -> None:
        self.environ = environ
        self.method = environ.get("REQUEST_METHOD", "GET").upper()
        self.path = environ.get("PATH_INFO", "/") or "/"
        self.query = {key: values[0] for key, values in parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=True).items()}
        cookie = SimpleCookie()
        cookie.load(environ.get("HTTP_COOKIE", ""))
        self.cookies = {key: morsel.value for key, morsel in cookie.items()}
        self.form: dict[str, str] = {}
        self.files: dict[str, UploadedFile] = {}
        if self.method in {"POST", "PUT"}:
            self._parse_body()

    def _parse_body(self) -> None:
        length = int(self.environ.get("CONTENT_LENGTH") or 0)
        if not length:
            return
        body = self.environ["wsgi.input"].read(length)
        content_type = self.environ.get("CONTENT_TYPE", "")
        if "application/x-www-form-urlencoded" in content_type:
            self.form = {key: values[0] for key, values in parse_qs(body.decode("utf-8", "ignore"), keep_blank_values=True).items()}
        elif "multipart/form-data" in content_type:
            self.form, self.files = parse_multipart(body, content_type)


CATALOG_CONFIGS: dict[str, dict[str, Any]] = {
    "materials": {
        "title": "Материалы",
        "roles": {"warehouse", "procurement", "director", "admin"},
        "fields": [
            {"name": "code", "label": "Код"},
            {"name": "name", "label": "Наименование"},
            {"name": "unit", "label": "Ед. изм."},
            {"name": "price", "label": "Цена", "type": "number"},
            {"name": "min_stock", "label": "Мин. остаток", "type": "number"},
            {"name": "category", "label": "Категория"},
            {"name": "is_ppe", "label": "СИЗ", "type": "checkbox"},
        ],
    },
    "suppliers": {
        "title": "Поставщики",
        "roles": {"procurement", "director", "admin"},
        "fields": [
            {"name": "name", "label": "Наименование"},
            {"name": "tax_id", "label": "ИНН/БИН"},
            {"name": "contact_person", "label": "Контактное лицо"},
            {"name": "phone", "label": "Телефон"},
            {"name": "email", "label": "Email"},
            {"name": "address", "label": "Адрес"},
        ],
    },
    "objects": {
        "title": "Объекты строительства",
        "roles": {"director", "admin"},
        "fields": [
            {"name": "name", "label": "Наименование объекта"},
            {"name": "address", "label": "Адрес"},
            {"name": "customer_name", "label": "Заказчик"},
            {"name": "description", "label": "Описание"},
            {"name": "start_date", "label": "Начало", "type": "date"},
            {"name": "end_date", "label": "Окончание", "type": "date"},
        ],
    },
    "workers": {
        "title": "Рабочие",
        "roles": {"site_manager", "director", "admin"},
        "fields": [
            {"name": "full_name", "label": "ФИО"},
            {"name": "employee_number", "label": "Табельный номер"},
            {"name": "site_name", "label": "Участок"},
            {"name": "position", "label": "Должность"},
            {"name": "hire_date", "label": "Дата приема", "type": "date"},
        ],
    },
    "norms": {
        "title": "Нормы расхода материалов",
        "roles": {"site_manager", "director", "admin"},
        "fields": [
            {"name": "work_type", "label": "Вид работ"},
            {"name": "material_id", "label": "Материал", "type": "select", "source": "materials"},
            {"name": "norm_per_unit", "label": "Норма на единицу", "type": "number"},
            {"name": "unit", "label": "Ед. измерения"},
            {"name": "notes", "label": "Примечание"},
        ],
    },
    "contracts": {
        "title": "Договоры СМР",
        "roles": {"director", "admin"},
        "fields": [
            {"name": "number", "label": "Номер"},
            {"name": "contract_date", "label": "Дата", "type": "date"},
            {"name": "object_id", "label": "Объект", "type": "select", "source": "objects"},
            {"name": "customer_name", "label": "Заказчик"},
            {"name": "subject", "label": "Предмет"},
            {"name": "work_type", "label": "Вид работ"},
            {"name": "planned_volume", "label": "Плановый объем", "type": "number"},
            {"name": "volume_unit", "label": "Ед. объема"},
            {"name": "amount", "label": "Сумма", "type": "number"},
            {"name": "vat_rate", "label": "НДС %", "type": "number"},
            {"name": "start_date", "label": "Начало", "type": "date"},
            {"name": "end_date", "label": "Окончание", "type": "date"},
            {"name": "status", "label": "Статус", "type": "select", "options": [("draft", "Черновик"), ("approved", "Утвержден")]},
            {"name": "created_by", "label": "ID автора", "type": "hidden", "default": "0"},
        ],
    },
    "supply_contracts": {
        "title": "Договоры поставки",
        "roles": {"procurement", "director", "admin"},
        "fields": [
            {"name": "number", "label": "Номер"},
            {"name": "contract_date", "label": "Дата", "type": "date"},
            {"name": "supplier_id", "label": "Поставщик", "type": "select", "source": "suppliers"},
            {"name": "related_smr_contract_id", "label": "Договор СМР", "type": "select", "source": "contracts"},
            {"name": "amount", "label": "Сумма", "type": "number"},
            {"name": "status", "label": "Статус", "type": "select", "options": [("draft", "Черновик"), ("approved", "Утвержден")]},
            {"name": "terms", "label": "Условия"},
        ],
    },
    "users": {
        "title": "Пользователи",
        "roles": {"admin"},
        "fields": [
            {"name": "username", "label": "Логин"},
            {"name": "full_name", "label": "ФИО"},
            {"name": "role", "label": "Роль", "type": "select", "options": [(key, value) for key, value in ROLE_LABELS.items()]},
            {"name": "site_name", "label": "Участок"},
            {"name": "supplier_id", "label": "Связанный поставщик", "type": "select", "source": "suppliers", "blank": True},
            {"name": "password", "label": "Пароль"},
            {"name": "is_active", "label": "Активен", "type": "checkbox"},
        ],
    },
}


class Application:
    def __init__(self, db: Database) -> None:
        self.db = db
        self.exporter = Exporter(db)

    def __call__(self, environ: dict[str, Any], start_response):
        request = Request(environ)
        user = self.db.get_user_by_session(request.cookies.get("session_id"))
        try:
            return self.dispatch(request, user, start_response)
        except Exception as exc:
            trace = traceback.format_exc()
            body = layout(
                "Ошибка приложения",
                f"<div class='panel'><h2>{escape(str(exc))}</h2><pre>{escape(trace)}</pre></div>",
                user=user,
                flash="Во время обработки запроса возникла ошибка.",
                error=True,
                active_path=request.path,
            )
            start_response("500 Internal Server Error", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(body)))])
            return [body]

    def dispatch(self, request: Request, user: dict[str, Any] | None, start_response):
        if request.path == "/login":
            return self.handle_login(request, start_response)
        if request.path == "/logout":
            return self.handle_logout(request, start_response)
        if request.path == "/":
            return self.redirect(start_response, "/dashboard" if user else "/login")
        if user is None:
            return self.redirect(start_response, "/login")
        if request.path == "/dashboard":
            return self.render_dashboard(user, request, start_response)
        if request.path == "/catalogs":
            return self.render_catalogs(user, request, start_response)
        if request.path == "/procurement":
            return self.render_procurement(user, request, start_response)
        if request.path == "/supplier":
            return self.render_supplier(user, request, start_response)
        if request.path == "/warehouse":
            return self.render_warehouse(user, request, start_response)
        if request.path == "/work":
            return self.render_work(user, request, start_response)
        if request.path == "/writeoffs":
            return self.render_writeoffs(user, request, start_response)
        if request.path == "/ppe":
            return self.render_ppe(user, request, start_response)
        if request.path == "/reports":
            return self.render_reports(user, request, start_response)
        if request.path == "/archive":
            return self.render_archive(user, request, start_response)
        if request.path == "/admin":
            return self.render_admin(user, request, start_response)
        if request.path == "/export":
            return self.handle_export(user, request, start_response)
        if request.path == "/download":
            return self.handle_download(user, request, start_response)
        return self.not_found(start_response)

    def html_response(self, start_response, content: bytes) -> list[bytes]:
        start_response("200 OK", [("Content-Type", "text/html; charset=utf-8"), ("Content-Length", str(len(content)))])
        return [content]

    def redirect(self, start_response, location: str, session_id: str | None = None, clear_session: bool = False):
        headers = [("Location", location)]
        if session_id is not None:
            headers.append(("Set-Cookie", f"session_id={session_id}; Path=/; HttpOnly; SameSite=Lax"))
        if clear_session:
            headers.append(("Set-Cookie", "session_id=; Max-Age=0; Path=/; HttpOnly; SameSite=Lax"))
        start_response("302 Found", headers)
        return [b""]

    def with_message(self, path: str, message: str, error: bool = False, **extra: str) -> str:
        params = {"message": message}
        if error:
            params["error"] = "1"
        params.update({key: value for key, value in extra.items() if value is not None})
        return f"{path}?{urlencode(params)}"

    def forbidden(self, start_response) -> list[bytes]:
        body = b"Access denied"
        start_response("403 Forbidden", [("Content-Type", "text/plain; charset=utf-8"), ("Content-Length", str(len(body)))])
        return [body]

    def not_found(self, start_response) -> list[bytes]:
        body = b"Not found"
        start_response("404 Not Found", [("Content-Type", "text/plain; charset=utf-8"), ("Content-Length", str(len(body)))])
        return [body]

    def require_role(self, user: dict[str, Any], roles: set[str], start_response) -> bool:
        if user["role"] not in roles:
            self.forbidden(start_response)
            return False
        return True

    def render(self, title: str, content: str, user: dict[str, Any] | None, request: Request, start_response) -> list[bytes]:
        page = layout(
            title,
            content,
            user=user,
            flash=request.query.get("message", ""),
            error=request.query.get("error") == "1",
            active_path=request.path + (f"?entity={request.query.get('entity')}" if request.path == "/catalogs" else ""),
        )
        return self.html_response(start_response, page)

    def handle_login(self, request: Request, start_response):
        if request.method == "POST":
            user = self.db.authenticate(request.form.get("username", "").strip(), request.form.get("password", "").strip())
            if user:
                session_id = self.db.create_session(user["id"])
                return self.redirect(start_response, "/dashboard", session_id=session_id)
            page = layout(
                "Вход в систему",
                self.login_form(),
                user=None,
                flash="Неверный логин или пароль.",
                error=True,
            )
            return self.html_response(start_response, page)
        page = layout("Вход в систему", self.login_form(), user=None)
        return self.html_response(start_response, page)

    def login_form(self) -> str:
        demo_rows = table(
            ["Логин", "Роль"],
            [
                ["director", "Начальник монтажного объекта"],
                ["procurement", "Снабженец"],
                ["warehouse", "Кладовщик"],
                ["site12", "Начальник участка"],
                ["accounting", "Бухгалтерия"],
                ["supplier1", "Поставщик"],
                ["admin", "Администратор"],
            ],
        )
        return (
            "<div class='stack'>"
            "<p class='muted'>Локальная АИС для учета материалов, складских операций и документооборота филиала АО «СТ-1».</p>"
            "<form method='post'>"
            "<div class='form-grid'>"
            "<div><label>Логин</label><input name='username' required></div>"
            "<div><label>Пароль</label><input type='password' name='password' required></div>"
            "</div><div class='actions'><button type='submit'>Войти</button></div></form>"
            "<div class='panel'><h2>Демо-пользователи</h2>"
            f"{demo_rows}"
            "<p class='hint'>Пароли указаны в README проекта.</p></div></div>"
        )

    def handle_logout(self, request: Request, start_response):
        self.db.delete_session(request.cookies.get("session_id"))
        return self.redirect(start_response, "/login", clear_session=True)

    def render_dashboard(self, user: dict[str, Any], request: Request, start_response):
        metrics = self.db.dashboard_metrics()
        cards = "".join(
            [
                metric_card("Договоры СМР", str(metrics["contracts"]), "Активные и зарегистрированные договоры"),
                metric_card("Документы в работе", str(metrics["pending"]), "Черновики, утверждение, возвраты и загрузки"),
                metric_card("Документы поставщиков", str(metrics["supplier_docs"]), "Счет, счет-фактура, накладная"),
                metric_card("Низкие остатки", str(metrics["alerts"]), "Материалы ниже минимального порога"),
            ]
        )
        low_stock = self.db.low_stock_alerts()
        low_stock_panel = panel(
            "Контроль минимального остатка",
            table(
                ["Код", "Материал", "Ед.", "Остаток", "Минимум"],
                [[item["code"], item["name"], item["unit"], str(round(float(item["warehouse_balance"]), 3)), str(item["min_stock"])] for item in low_stock[:12]],
            ),
        )
        recent_docs = self.db.search_documents({})
        recent_panel = panel(
            "Последние документы",
            table(
                ["Дата", "Тип", "Номер", "Статус", "Контрагент / объект"],
                [
                    [
                        doc["doc_date"],
                        doc["doc_type"],
                        doc["doc_number"],
                        status_badge(doc["status"]),
                        escape((doc.get("counterparty") or "") + (" / " + doc["object_name"] if doc.get("object_name") else "")),
                    ]
                    for doc in recent_docs[:10]
                ],
            ),
            toolbar=f"<a class='button secondary' href='/archive'>Открыть архив</a>",
        )
        site_balances = self.db.site_balances()
        site_panel = panel(
            "Материалы на участках",
            table(
                ["Локация", "Код", "Материал", "Количество", "Ед."],
                [[row["location_name"], row["code"], row["name"], str(round(float(row["quantity"]), 3)), row["unit"]] for row in site_balances[:12]],
            ),
        )
        content = f"<div class='grid'>{cards}</div><div class='stack'>{low_stock_panel}{recent_panel}{site_panel}</div>"
        return self.render("Главная панель", content, user, request, start_response)

    def render_catalogs(self, user: dict[str, Any], request: Request, start_response):
        entity = request.query.get("entity", "materials")
        config = CATALOG_CONFIGS.get(entity)
        if not config or not self.require_role(user, config["roles"], start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                data = self.extract_form_data(config["fields"], request, user)
                if entity == "contracts":
                    data["created_by"] = str(user["id"])
                self.db.save_simple_record(entity, data, user["id"])
                return self.redirect(start_response, self.with_message(f"/catalogs?entity={entity}", "Запись сохранена."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message(f"/catalogs?entity={entity}", str(exc), error=True))

        lookup = self.db.get_lookup_data()
        entity_links = " ".join(
            f"<a class='button {'secondary' if key != entity else ''}' href='/catalogs?entity={key}'>{escape(item['title'])}</a>"
            for key, item in CATALOG_CONFIGS.items()
            if user["role"] in item["roles"]
        )
        form_html = self.render_catalog_form(entity, config, lookup)
        rows = self.db.list_entity(entity)
        content = (
            f"<div class='actions'>{entity_links}</div>"
            + panel(config["title"], form_html)
            + panel("Реестр", self.catalog_table(entity, rows))
        )
        return self.render(config["title"], content, user, request, start_response)

    def render_catalog_form(self, entity: str, config: dict[str, Any], lookup: dict[str, list[dict[str, Any]]]) -> str:
        fields_html = []
        for field in config["fields"]:
            field_type = field.get("type", "text")
            if field_type == "hidden":
                continue
            fields_html.append(self.field_html(field, lookup))
        return (
            f"<form method='post'><input type='hidden' name='entity' value='{escape(entity)}'>"
            f"<div class='form-grid'>{''.join(fields_html)}</div>"
            "<div class='actions'><button type='submit'>Сохранить</button></div></form>"
        )

    def field_html(self, field: dict[str, Any], lookup: dict[str, list[dict[str, Any]]]) -> str:
        field_type = field.get("type", "text")
        name = field["name"]
        label = escape(field["label"])
        if field_type == "checkbox":
            return f"<div><label><input type='checkbox' name='{escape(name)}' value='1'> {label}</label></div>"
        if field_type == "select":
            if "options" in field:
                options = field["options"]
            else:
                source = lookup.get(field["source"], [])
                if field["source"] == "materials":
                    options = [(str(row["id"]), f"{row['code']} — {row['name']}") for row in source]
                elif field["source"] == "contracts":
                    options = [(str(row["id"]), f"{row['number']} — {row.get('subject', '')}") for row in source]
                else:
                    options = [(str(row["id"]), row.get("name") or row.get("full_name") or row.get("number") or str(row["id"])) for row in source]
            option_tags = ["<option value=''>Выбрать...</option>"] if field.get("blank", False) or "options" not in field else []
            option_tags.extend(f"<option value='{escape(str(value))}'>{escape(str(title))}</option>" for value, title in options)
            return f"<div><label>{label}</label><select name='{escape(name)}'>{''.join(option_tags)}</select></div>"
        input_type = "date" if field_type == "date" else "number" if field_type == "number" else "text"
        return f"<div><label>{label}</label><input type='{input_type}' name='{escape(name)}'></div>"

    def extract_form_data(self, fields: list[dict[str, Any]], request: Request, user: dict[str, Any]) -> dict[str, str]:
        data: dict[str, str] = {}
        for field in fields:
            name = field["name"]
            field_type = field.get("type", "text")
            if field_type == "checkbox":
                data[name] = "1" if request.form.get(name) == "1" else "0"
            else:
                data[name] = request.form.get(name, field.get("default", ""))
        if "is_active" in data and not data["is_active"]:
            data["is_active"] = "0"
        return data

    def catalog_table(self, entity: str, rows: list[dict[str, Any]]) -> str:
        if entity == "materials":
            return table(
                ["Код", "Наименование", "Ед.", "Цена", "Мин. остаток", "Категория", "СИЗ"],
                [[row["code"], row["name"], row["unit"], money(row["price"]), str(row["min_stock"]), row.get("category") or "", "Да" if row["is_ppe"] else "Нет"] for row in rows],
            )
        if entity == "suppliers":
            return table(
                ["Наименование", "ИНН/БИН", "Контакт", "Телефон", "Email"],
                [[row["name"], row.get("tax_id") or "", row.get("contact_person") or "", row.get("phone") or "", row.get("email") or ""] for row in rows],
            )
        if entity == "objects":
            return table(
                ["Объект", "Адрес", "Заказчик", "Период"],
                [[row["name"], row.get("address") or "", row.get("customer_name") or "", f"{row.get('start_date') or '-'} — {row.get('end_date') or '-'}"] for row in rows],
            )
        if entity == "workers":
            return table(
                ["ФИО", "Таб. №", "Участок", "Должность", "Дата приема"],
                [[row["full_name"], row["employee_number"], row.get("site_name") or "", row.get("position") or "", row.get("hire_date") or ""] for row in rows],
            )
        if entity == "norms":
            return table(
                ["Вид работ", "Код", "Материал", "Норма", "Ед.", "Примечание"],
                [[row["work_type"], row["material_code"], row["material_name"], str(row["norm_per_unit"]), row.get("unit") or "", row.get("notes") or ""] for row in rows],
            )
        if entity == "contracts":
            return table(
                ["Номер", "Дата", "Объект", "Заказчик", "Сумма", "Статус", "Экспорт"],
                [
                    [
                        row["number"],
                        row["contract_date"],
                        row.get("object_name") or "",
                        row["customer_name"],
                        money(row["amount"]),
                        status_badge(row["status"]),
                        f"<a class='button secondary' href='/export?kind=document&entity_type=smr_contract&id={row['id']}'>DOCX</a>",
                    ]
                    for row in rows
                ],
            )
        if entity == "supply_contracts":
            return table(
                ["Номер", "Дата", "Поставщик", "Договор СМР", "Сумма", "Статус", "Экспорт"],
                [
                    [
                        row["number"],
                        row["contract_date"],
                        row["supplier_name"],
                        row.get("smr_number") or "",
                        money(row["amount"]),
                        status_badge(row["status"]),
                        f"<a class='button secondary' href='/export?kind=document&entity_type=supply_contract&id={row['id']}'>DOCX</a>",
                    ]
                    for row in rows
                ],
            )
        if entity == "users":
            return table(
                ["Логин", "ФИО", "Роль", "Участок", "Поставщик", "Активен"],
                [[row["username"], row["full_name"], ROLE_LABELS.get(row["role"], row["role"]), row.get("site_name") or "", row.get("supplier_name") or "", "Да" if row["is_active"] else "Нет"] for row in rows],
            )
        return "<p class='muted'>Нет данных.</p>"

    def render_procurement(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"site_manager", "procurement", "director", "admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                self.db.create_procurement_request(user, request.form)
                return self.redirect(start_response, self.with_message("/procurement", "Заявка поставщику создана."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/procurement", str(exc), error=True))
        lookup = self.db.get_lookup_data()
        contract_options = "".join(
            f"<option value='{row['id']}'>{escape(row['number'])} — {escape(row.get('object_name') or '')}</option>" for row in lookup["contracts"]
        )
        supplier_options = "".join(f"<option value='{row['id']}'>{escape(row['name'])}</option>" for row in lookup["suppliers"])
        form_html = (
            "<form method='post'><div class='form-grid'>"
            f"<div><label>Дата заявки</label><input type='date' name='request_date' value='{escape(now_str()[:10])}'></div>"
            f"<div><label>Участок</label><input name='site_name' value='{escape(user.get('site_name') or 'Участок №12')}'></div>"
            f"<div><label>Договор СМР</label><select name='contract_id'><option value=''>Выбрать...</option>{contract_options}</select></div>"
            f"<div><label>Поставщик</label><select name='supplier_id'><option value=''>Выбрать...</option>{supplier_options}</select></div>"
            "</div>"
            "<div class='form-grid'>"
            "<div><label>Статус</label><select name='status'><option value='draft'>Черновик</option><option value='approval'>На утверждении</option></select></div>"
            "<div><label>Примечание</label><input name='notes' placeholder='Цель закупки, сроки, комментарии'></div>"
            "</div>"
            "<div><label>Позиции</label><textarea name='items' placeholder='КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ'></textarea></div>"
            "<div class='hint'>Пример: БН00178 | 12 | 550 | Аргон для сварки</div>"
            "<div class='actions'><button type='submit'>Создать заявку</button></div></form>"
        )
        requests = self.db.list_procurement_requests()
        rows = []
        for item in requests:
            rows.append(
                [
                    item["number"],
                    item["request_date"],
                    item["site_name"],
                    item.get("contract_number") or "",
                    item.get("supplier_name") or "",
                    status_badge(item["status"]),
                    f"<a class='button secondary' href='/export?kind=document&entity_type=procurement_request&id={item['id']}'>DOCX</a>",
                ]
            )
        content = panel("Новая заявка поставщику", form_html) + panel("Реестр заявок", table(["Номер", "Дата", "Участок", "Договор", "Поставщик", "Статус", "Экспорт"], rows))
        return self.render("Закупки и заявки", content, user, request, start_response)

    def render_supplier(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"supplier", "procurement", "director", "admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                file_payload = self.request_file(request, "attachment")
                self.db.create_supplier_document(user, request.form, file_payload)
                return self.redirect(start_response, self.with_message("/supplier", "Документ поставщика загружен."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/supplier", str(exc), error=True))
        supplier_scope = user.get("supplier_id") if user["role"] == "supplier" else None
        requests = self.db.list_procurement_requests(supplier_scope=supplier_scope)
        documents = self.db.list_supplier_documents(supplier_scope=supplier_scope)
        lookup = self.db.get_lookup_data()
        request_options = "".join(f"<option value='{item['id']}'>{escape(item['number'])} — {escape(item.get('supplier_name') or '')}</option>" for item in requests)
        supply_contract_options = "".join(f"<option value='{row['id']}'>{escape(row['number'])}</option>" for row in lookup["supply_contracts"])
        supplier_hidden = f"<input type='hidden' name='supplier_id' value='{user.get('supplier_id') or ''}'>" if user["role"] == "supplier" else ""
        upload_form = (
            f"<form method='post' enctype='multipart/form-data'>{supplier_hidden}<div class='form-grid'>"
            "<div><label>Тип документа</label><select name='doc_type'><option>Счет</option><option>Счет-фактура</option><option>Товарная накладная</option></select></div>"
            "<div><label>Номер документа</label><input name='doc_number'></div>"
            f"<div><label>Дата</label><input type='date' name='doc_date' value='{escape(now_str()[:10])}'></div>"
            "<div><label>Сумма</label><input type='number' step='0.01' name='amount'></div>"
            "<div><label>Сумма НДС</label><input type='number' step='0.01' name='vat_amount'></div>"
            f"<div><label>Заявка поставщику</label><select name='request_id'><option value=''>Выбрать...</option>{request_options}</select></div>"
            f"<div><label>Договор поставки</label><select name='supply_contract_id'><option value=''>Выбрать...</option>{supply_contract_options}</select></div>"
            "<div><label>Файл</label><input type='file' name='attachment'></div>"
            "</div><div><label>Примечание</label><input name='notes'></div>"
            "<div class='actions'><button type='submit'>Загрузить документ</button></div></form>"
        )
        doc_rows = []
        for item in documents:
            download = f"<a class='button secondary' href='/download?document_id={self.find_document_id('supplier_document', item['id'])}'>Файл</a>" if item.get("stored_path") else ""
            doc_rows.append(
                [
                    item["doc_type"],
                    item["doc_number"],
                    item["doc_date"],
                    item.get("request_number") or "",
                    item["supplier_name"],
                    money(item["amount"]),
                    status_badge(item["status"]),
                    download or f"<a class='button secondary' href='/export?kind=document&entity_type=supplier_document&id={item['id']}'>DOCX</a>",
                ]
            )
        content = panel("Загрузка документов поставщиком", upload_form) + panel(
            "Загруженные документы",
            table(["Тип", "Номер", "Дата", "Заявка", "Поставщик", "Сумма", "Статус", "Файл"], doc_rows),
        )
        return self.render("Кабинет поставщика", content, user, request, start_response)

    def request_file(self, request: Request, name: str) -> dict[str, Any] | None:
        uploaded = request.files.get(name)
        if not uploaded or not uploaded.filename:
            return None
        return {"filename": uploaded.filename, "content": uploaded.content, "content_type": uploaded.content_type}

    def render_warehouse(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"warehouse", "procurement", "director", "admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                action = request.form.get("action", "receipt")
                if action == "receipt":
                    self.db.create_stock_receipt(user, request.form)
                    target_message = "Приходный ордер создан."
                else:
                    self.db.create_stock_issue(user, request.form)
                    target_message = "Требование-накладная создана."
                return self.redirect(start_response, self.with_message("/warehouse", target_message))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/warehouse", str(exc), error=True))
        lookup = self.db.get_lookup_data()
        supplier_options = "".join(f"<option value='{row['id']}'>{escape(row['name'])}</option>" for row in lookup["suppliers"])
        contract_options = "".join(f"<option value='{row['id']}'>{escape(row['number'])}</option>" for row in lookup["contracts"])
        supplier_docs = self.db.list_supplier_documents()
        supplier_doc_options = "".join(
            f"<option value='{row['id']}'>{escape(row['doc_type'])} {escape(row['doc_number'])} — {escape(row['supplier_name'])}</option>" for row in supplier_docs
        )
        receipt_form = (
            "<form method='post'><input type='hidden' name='action' value='receipt'>"
            "<div class='form-grid'>"
            f"<div><label>Дата прихода</label><input type='date' name='receipt_date' value='{escape(now_str()[:10])}'></div>"
            f"<div><label>Поставщик</label><select name='supplier_id'>{supplier_options}</select></div>"
            f"<div><label>Документ поставщика</label><select name='supplier_document_id'><option value=''>Выбрать...</option>{supplier_doc_options}</select></div>"
            "<div><label>Статус</label><select name='status'><option value='draft'>Черновик</option><option value='approved'>Утвержден</option></select></div>"
            "</div>"
            "<div><label>Позиции</label><textarea name='items' placeholder='КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ'></textarea></div>"
            "<div><label>Примечание</label><input name='notes'></div>"
            "<div class='actions'><button type='submit'>Оформить приход</button></div></form>"
        )
        issue_form = (
            "<form method='post'><input type='hidden' name='action' value='issue'>"
            "<div class='form-grid'>"
            f"<div><label>Дата отпуска</label><input type='date' name='issue_date' value='{escape(now_str()[:10])}'></div>"
            "<div><label>Участок</label><input name='site_name' value='Участок №12'></div>"
            f"<div><label>Договор СМР</label><select name='contract_id'><option value=''>Выбрать...</option>{contract_options}</select></div>"
            "<div><label>Получатель</label><input name='received_by_name' value='Начальник участка'></div>"
            "<div><label>Статус</label><select name='status'><option value='draft'>Черновик</option><option value='approved'>Утвержден</option></select></div>"
            "</div>"
            "<div><label>Позиции</label><textarea name='items' placeholder='КОД | КОЛИЧЕСТВО | ЦЕНА | ПРИМЕЧАНИЕ'></textarea></div>"
            "<div><label>Примечание</label><input name='notes'></div>"
            "<div class='actions'><button type='submit'>Оформить отпуск</button></div></form>"
        )
        balances = self.db.stock_balances()
        balance_rows = [[row["code"], row["name"], row["unit"], str(round(float(row["warehouse_balance"]), 3)), str(row["min_stock"])] for row in balances]
        receipts = self.db.list_stock_receipts()
        receipt_rows = [[item["number"], item["receipt_date"], item["supplier_name"], status_badge(item["status"]), f"<a class='button secondary' href='/export?kind=document&entity_type=stock_receipt&id={item['id']}'>DOCX</a>"] for item in receipts]
        issues = self.db.list_stock_issues()
        issue_rows = [[item["number"], item["issue_date"], item["site_name"], item.get("contract_number") or "", status_badge(item["status"]), f"<a class='button secondary' href='/export?kind=document&entity_type=stock_issue&id={item['id']}'>DOCX</a>"] for item in issues]
        content = (
            panel("Приход материалов", receipt_form)
            + panel("Отпуск материалов на участок", issue_form)
            + panel("Остатки на центральном складе", table(["Код", "Материал", "Ед.", "Остаток", "Мин. остаток"], balance_rows))
            + panel("Приходные ордера", table(["Номер", "Дата", "Поставщик", "Статус", "Экспорт"], receipt_rows))
            + panel("Требования-накладные", table(["Номер", "Дата", "Участок", "Договор", "Статус", "Экспорт"], issue_rows))
        )
        return self.render("Складской учет", content, user, request, start_response)

    def render_work(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"site_manager", "director", "admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                self.db.create_work_log(user, request.form)
                return self.redirect(start_response, self.with_message("/work", "Отчет по участку сохранен."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/work", str(exc), error=True))
        lookup = self.db.get_lookup_data()
        contract_options = "".join(f"<option value='{row['id']}'>{escape(row['number'])}</option>" for row in lookup["contracts"])
        form_html = (
            "<form method='post'><div class='form-grid'>"
            f"<div><label>Участок</label><input name='site_name' value='{escape(user.get('site_name') or 'Участок №12')}'></div>"
            f"<div><label>Договор</label><select name='contract_id'><option value=''>Выбрать...</option>{contract_options}</select></div>"
            "<div><label>Вид работ</label><input name='work_type' value='Покраска'></div>"
            "<div><label>Плановый объем</label><input type='number' step='0.01' name='planned_volume'></div>"
            "<div><label>Фактический объем</label><input type='number' step='0.01' name='actual_volume'></div>"
            "<div><label>Ед. объема</label><input name='volume_unit' value='м2'></div>"
            f"<div><label>Плановая дата</label><input type='date' name='plan_date' value='{escape(now_str()[:10])}'></div>"
            f"<div><label>Фактическая дата</label><input type='date' name='actual_date' value='{escape(now_str()[:10])}'></div>"
            "<div><label>Статус</label><select name='status'><option value='planned'>План</option><option value='in_progress'>В работе</option><option value='completed'>Выполнено</option><option value='delayed'>С опозданием</option></select></div>"
            "</div><div><label>Примечание</label><input name='notes'></div>"
            "<div class='actions'><button type='submit'>Сохранить запись</button></div></form>"
        )
        rows = self.db.list_work_logs()
        content = panel("Отчет по работе участка", form_html) + panel(
            "История работ",
            table(
                ["Участок", "Договор", "Вид работ", "План", "Факт", "Сроки", "Статус"],
                [
                    [
                        row["site_name"],
                        row.get("contract_number") or "",
                        row["work_type"],
                        f"{row['planned_volume']} {row.get('volume_unit') or ''}",
                        f"{row['actual_volume']} {row.get('volume_unit') or ''}",
                        f"{row.get('plan_date') or '-'} / {row.get('actual_date') or '-'}",
                        row["status"],
                    ]
                    for row in rows
                ],
            ),
        )
        return self.render("Работа участков", content, user, request, start_response)

    def render_writeoffs(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"site_manager", "director", "accounting", "admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                self.db.create_write_off(user, request.form)
                return self.redirect(start_response, self.with_message("/writeoffs", "Акт списания создан и рассчитан по нормам."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/writeoffs", str(exc), error=True))
        lookup = self.db.get_lookup_data()
        contract_options = "".join(f"<option value='{row['id']}'>{escape(row['number'])} — {escape(row.get('subject') or '')}</option>" for row in lookup["contracts"])
        norms = self.db.list_entity("norms")
        norms_hint = table(["Вид работ", "Код", "Материал", "Норма"], [[row["work_type"], row["material_code"], row["material_name"], str(row["norm_per_unit"])] for row in norms])
        form_html = (
            "<form method='post'><div class='form-grid'>"
            f"<div><label>Дата акта</label><input type='date' name='act_date' value='{escape(now_str()[:10])}'></div>"
            f"<div><label>Договор СМР</label><select name='contract_id'>{contract_options}</select></div>"
            f"<div><label>Участок</label><input name='site_name' value='{escape(user.get('site_name') or 'Участок №12')}'></div>"
            "<div><label>Вид работ</label><input name='work_type' value='Покраска'></div>"
            "<div><label>Объем работ</label><input type='number' step='0.001' name='work_volume'></div>"
            "<div><label>Ед. объема</label><input name='volume_unit' value='м2'></div>"
            "<div><label>Статус</label><select name='status'><option value='draft'>Черновик</option><option value='approval'>На утверждении</option></select></div>"
            "</div><div><label>Примечание</label><input name='notes'></div>"
            "<div class='actions'><button type='submit'>Рассчитать и создать акт</button></div></form>"
        )
        acts = self.db.list_write_offs()
        act_rows = [
            [
                item["number"],
                item["act_date"],
                item["contract_number"],
                item.get("object_name") or "",
                item["work_type"],
                f"{item['work_volume']} {item.get('volume_unit') or ''}",
                status_badge(item["status"]),
                f"<a class='button secondary' href='/export?kind=document&entity_type=write_off&id={item['id']}'>DOCX</a>",
            ]
            for item in acts
        ]
        content = panel("Нормативное списание материалов", form_html) + panel("Настроенные нормы", norms_hint) + panel(
            "Акты списания",
            table(["Номер", "Дата", "Договор", "Объект", "Вид работ", "Объем", "Статус", "Экспорт"], act_rows),
        )
        return self.render("Списание материалов", content, user, request, start_response)

    def render_ppe(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"site_manager", "warehouse", "director", "admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                self.db.create_ppe_issuance(user, request.form)
                return self.redirect(start_response, self.with_message("/ppe", "Ведомость выдачи спецодежды создана."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/ppe", str(exc), error=True))
        form_html = (
            "<form method='post'><div class='form-grid'>"
            f"<div><label>Дата</label><input type='date' name='issue_date' value='{escape(now_str()[:10])}'></div>"
            f"<div><label>Участок</label><input name='site_name' value='{escape(user.get('site_name') or 'Участок №12')}'></div>"
            "<div><label>Сезон</label><select name='season'><option value='летняя'>летняя</option><option value='зимняя'>зимняя</option></select></div>"
            "<div><label>Статус</label><select name='status'><option value='draft'>Черновик</option><option value='approved'>Утвержден</option></select></div>"
            "</div>"
            "<div><label>Позиции</label><textarea name='items' placeholder='ТАБЕЛЬНЫЙ_НОМЕР | КОД_МАТЕРИАЛА | КОЛИЧЕСТВО | СРОК_СЛУЖБЫ_МЕС'></textarea></div>"
            "<div><label>Примечание</label><input name='notes'></div>"
            "<div class='actions'><button type='submit'>Создать ведомость</button></div></form>"
        )
        issuances = self.db.list_ppe_issuances()
        rows = [[item["number"], item["issue_date"], item["site_name"], item.get("season") or "", status_badge(item["status"]), f"<a class='button secondary' href='/export?kind=document&entity_type=ppe_issuance&id={item['id']}'>DOCX</a>"] for item in issuances]
        content = panel("Выдача спецодежды и СИЗ", form_html) + panel("Реестр ведомостей", table(["Номер", "Дата", "Участок", "Сезон", "Статус", "Экспорт"], rows))
        return self.render("Спецодежда", content, user, request, start_response)

    def render_reports(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"director", "procurement", "warehouse", "site_manager", "accounting", "admin"}, start_response):
            return self.forbidden(start_response)
        report_name = request.query.get("report", "stock")
        date_from = request.query.get("date_from", now_str()[:7] + "-01")
        date_to = request.query.get("date_to", now_str()[:10])
        filters = {"date_from": date_from, "date_to": date_to}
        providers = {
            "stock": self.db.report_stock,
            "purchases": self.db.report_purchases,
            "writeoffs": self.db.report_write_offs,
            "work": self.db.report_work_logs,
            "summary": self.db.report_summary,
            "ppe": self.db.report_ppe,
        }
        rows = providers[report_name](filters)
        headers = list(rows[0].keys()) if rows else ["Результат"]
        body_rows = [[escape(str(row.get(header, ""))) for header in headers] for row in rows[:100]] if rows else [["Нет данных"]]
        buttons = " ".join(
            f"<a class='button {'secondary' if name != report_name else ''}' href='{url('/reports', report=name, date_from=date_from, date_to=date_to)}'>{label}</a>"
            for name, label in [
                ("stock", "Остатки"),
                ("purchases", "Закупки"),
                ("writeoffs", "Списание"),
                ("work", "Работа участков"),
                ("summary", "Сводный"),
                ("ppe", "Спецодежда"),
            ]
        )
        filter_form = (
            "<form method='get'>"
            f"<input type='hidden' name='report' value='{escape(report_name)}'>"
            "<div class='form-grid'>"
            f"<div><label>Дата с</label><input type='date' name='date_from' value='{escape(date_from)}'></div>"
            f"<div><label>Дата по</label><input type='date' name='date_to' value='{escape(date_to)}'></div>"
            "</div><div class='actions'><button type='submit'>Показать</button>"
            f"<a class='button secondary' href='/export?kind=report&report_name={escape(report_name)}&date_from={escape(date_from)}&date_to={escape(date_to)}'>Excel</a>"
            "</div></form>"
        )
        content = f"<div class='actions'>{buttons}</div>" + panel("Фильтр периода", filter_form) + panel(
            "Данные отчета",
            table(headers, body_rows),
            toolbar="<span class='small muted'>Показаны первые 100 строк.</span>",
        )
        return self.render("Отчеты", content, user, request, start_response)

    def render_archive(self, user: dict[str, Any], request: Request, start_response):
        if request.method == "POST":
            if not self.require_role(user, {"director", "procurement", "warehouse", "site_manager", "accounting", "admin"}, start_response):
                return self.forbidden(start_response)
            try:
                self.db.transition_document(user, request.form["entity_type"], int(request.form["entity_id"]), request.form["new_status"])
                return self.redirect(start_response, self.with_message("/archive", "Статус документа обновлен."))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/archive", str(exc), error=True))
        filters = {
            "doc_type": request.query.get("doc_type", ""),
            "status": request.query.get("status", ""),
            "date_from": request.query.get("date_from", ""),
            "date_to": request.query.get("date_to", ""),
            "query": request.query.get("query", ""),
        }
        docs = self.db.search_documents(filters)
        if user["role"] == "supplier":
            supplier_name = self.db.fetch_one("SELECT name FROM suppliers WHERE id = ?", (user.get("supplier_id"),)) if user.get("supplier_id") else None
            docs = [doc for doc in docs if doc.get("counterparty") == (supplier_name["name"] if supplier_name else "") or doc.get("created_by") == user["id"]]
        filter_form = (
            "<form method='get'><div class='form-grid'>"
            f"<div><label>Тип</label><input name='doc_type' value='{escape(filters['doc_type'])}' placeholder='Например: Акт списания'></div>"
            f"<div><label>Статус</label><select name='status'><option value=''>Все</option><option value='draft'>Черновик</option><option value='approval'>На утверждении</option><option value='approved'>Утвержден</option><option value='sent_accounting'>В бухгалтерию</option><option value='accepted'>Принят</option><option value='rework'>Доработка</option><option value='uploaded'>Загружен</option></select></div>"
            f"<div><label>Дата с</label><input type='date' name='date_from' value='{escape(filters['date_from'])}'></div>"
            f"<div><label>Дата по</label><input type='date' name='date_to' value='{escape(filters['date_to'])}'></div>"
            "</div>"
            f"<div><label>Поиск</label><input name='query' value='{escape(filters['query'])}' placeholder='Номер, контрагент, объект'></div>"
            "<div class='actions'><button type='submit'>Найти</button></div></form>"
        )
        rows = []
        for doc in docs[:120]:
            download = f"<a class='button secondary' href='/download?document_id={doc['id']}'>Файл</a>" if doc.get("file_path") else ""
            export_link = f"<a class='button secondary' href='/export?kind=document&entity_type={escape(doc['entity_type'])}&id={doc['entity_id']}'>Экспорт</a>"
            actions = self.workflow_actions(user, doc)
            rows.append(
                [
                    doc["doc_date"],
                    doc["doc_type"],
                    doc["doc_number"],
                    status_badge(doc["status"]),
                    escape(doc.get("counterparty") or ""),
                    escape(doc.get("object_name") or ""),
                    escape(doc.get("author_name") or ""),
                    (download + " " + export_link).strip(),
                    actions or "<span class='small muted'>Нет</span>",
                ]
            )
        content = panel("Поиск по архиву документов", filter_form) + panel(
            "Архив",
            table(["Дата", "Тип", "Номер", "Статус", "Контрагент", "Объект", "Автор", "Файл", "Действия"], rows),
            toolbar="<span class='small muted'>Жизненный цикл документов управляется отсюда.</span>",
        )
        return self.render("Архив документов", content, user, request, start_response)

    def workflow_actions(self, user: dict[str, Any], doc: dict[str, Any]) -> str:
        buttons: list[str] = []
        if doc["status"] in {"draft", "rework"} and user["role"] in {"director", "procurement", "warehouse", "site_manager", "admin"}:
            buttons.append(
                action_form(
                    "transition",
                    {"entity_type": doc["entity_type"], "entity_id": str(doc["entity_id"]), "new_status": "approval"},
                    "На утверждение",
                )
            )
        if doc["status"] in {"approval", "uploaded"} and user["role"] in {"director", "procurement", "admin"}:
            buttons.append(
                action_form(
                    "transition",
                    {"entity_type": doc["entity_type"], "entity_id": str(doc["entity_id"]), "new_status": "approved"},
                    "Утвердить",
                )
            )
        if doc["status"] == "approved" and user["role"] in {"director", "admin"}:
            buttons.append(
                action_form(
                    "transition",
                    {"entity_type": doc["entity_type"], "entity_id": str(doc["entity_id"]), "new_status": "sent_accounting"},
                    "В бухгалтерию",
                )
            )
        if doc["status"] == "sent_accounting" and user["role"] in {"accounting", "admin"}:
            buttons.append(
                action_form(
                    "transition",
                    {"entity_type": doc["entity_type"], "entity_id": str(doc["entity_id"]), "new_status": "accepted"},
                    "Принять",
                )
            )
            buttons.append(
                action_form(
                    "transition",
                    {"entity_type": doc["entity_type"], "entity_id": str(doc["entity_id"]), "new_status": "rework"},
                    "На доработку",
                    css_class="warn",
                )
            )
        return " ".join(buttons)

    def render_admin(self, user: dict[str, Any], request: Request, start_response):
        if not self.require_role(user, {"admin"}, start_response):
            return self.forbidden(start_response)
        if request.method == "POST":
            try:
                action = request.form.get("action")
                if action == "backup":
                    self.db.create_backup(user["id"])
                    message = "Резервная копия создана."
                elif action == "restore":
                    self.db.restore_backup(request.form.get("backup_name", ""), user["id"])
                    message = "База восстановлена из резервной копии."
                else:
                    message = "Действие не распознано."
                return self.redirect(start_response, self.with_message("/admin", message))
            except Exception as exc:
                return self.redirect(start_response, self.with_message("/admin", str(exc), error=True))
        backups = self.db.list_backups()
        backup_options = "".join(f"<option value='{escape(item['name'])}'>{escape(item['name'])}</option>" for item in backups)
        audit_rows = self.db.fetch_all(
            """
            SELECT a.created_at, a.action, a.entity_type, a.entity_id, a.details, u.full_name
            FROM audit_log a
            LEFT JOIN users u ON u.id = a.user_id
            ORDER BY a.id DESC
            LIMIT 40
            """
        )
        content = (
            panel(
                "Резервное копирование",
                "<form method='post'><input type='hidden' name='action' value='backup'><div class='actions'><button type='submit'>Создать резервную копию</button></div></form>",
            )
            + panel(
                "Восстановление",
                "<form method='post'><input type='hidden' name='action' value='restore'>"
                f"<div class='form-grid'><div><label>Резервная копия</label><select name='backup_name'>{backup_options}</select></div></div>"
                "<div class='actions'><button class='warn' type='submit'>Восстановить</button></div></form>",
            )
            + panel(
                "Журнал действий",
                table(
                    ["Дата", "Пользователь", "Действие", "Сущность", "ID", "Описание"],
                    [[row["created_at"], row.get("full_name") or "", row["action"], row["entity_type"], str(row.get("entity_id") or ""), row.get("details") or ""] for row in audit_rows],
                ),
            )
        )
        return self.render("Администрирование", content, user, request, start_response)

    def handle_export(self, user: dict[str, Any], request: Request, start_response):
        kind = request.query.get("kind", "document")
        if kind == "document":
            path = self.exporter.export_document(request.query["entity_type"], int(request.query["id"]))
        else:
            filters = {"date_from": request.query.get("date_from", ""), "date_to": request.query.get("date_to", "")}
            path = self.exporter.export_report(request.query["report_name"], filters)
        return self.send_file(start_response, path)

    def handle_download(self, user: dict[str, Any], request: Request, start_response):
        document_id = int(request.query.get("document_id", "0"))
        record = self.db.fetch_one("SELECT file_path FROM documents WHERE id = ?", (document_id,))
        if not record or not record.get("file_path"):
            return self.not_found(start_response)
        path = Path(record["file_path"]).resolve()
        if not self.is_allowed_file(path):
            return self.forbidden(start_response)
        return self.send_file(start_response, path)

    def is_allowed_file(self, path: Path) -> bool:
        path_str = str(path)
        allowed_roots = [CONFIG.exports_dir.resolve(), CONFIG.uploads_dir.resolve(), CONFIG.backups_dir.resolve(), CONFIG.root_dir.resolve()]
        return any(path.exists() and path_str.startswith(str(root)) for root in allowed_roots)

    def send_file(self, start_response, path: Path):
        content = path.read_bytes()
        headers = [
            ("Content-Type", self.exporter.content_type(path)),
            ("Content-Length", str(len(content))),
            ("Content-Disposition", f'attachment; filename="{path.name}"'),
        ]
        start_response("200 OK", headers)
        return [content]

    def find_document_id(self, entity_type: str, entity_id: int) -> int:
        record = self.db.fetch_one("SELECT id FROM documents WHERE entity_type = ? AND entity_id = ?", (entity_type, entity_id))
        return int(record["id"]) if record else 0


def create_app() -> Application:
    database = Database()
    database.init()
    return Application(database)
