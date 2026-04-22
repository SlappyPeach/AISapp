from __future__ import annotations

from html import escape
from urllib.parse import urlencode

from .config import ROLE_LABELS, ROLE_NAV


BASE_CSS = """
body {
  margin: 0;
  font-family: "Segoe UI", Tahoma, sans-serif;
  background: linear-gradient(135deg, #eef4f7 0%, #f8fafb 50%, #e3ecef 100%);
  color: #1f2933;
}
.layout {
  display: grid;
  grid-template-columns: 280px 1fr;
  min-height: 100vh;
}
.sidebar {
  background: #12343b;
  color: #f5fbfc;
  padding: 28px 22px;
  box-shadow: inset -1px 0 0 rgba(255, 255, 255, 0.08);
}
.brand {
  font-size: 1.25rem;
  font-weight: 700;
  margin-bottom: 8px;
}
.subtitle {
  color: #b9d5da;
  font-size: 0.92rem;
  margin-bottom: 22px;
}
.sidebar a {
  display: block;
  color: #e9f4f5;
  text-decoration: none;
  padding: 10px 12px;
  border-radius: 12px;
  margin-bottom: 8px;
}
.sidebar a:hover, .sidebar a.active {
  background: rgba(255, 255, 255, 0.12);
}
.content {
  padding: 28px 32px 48px;
}
.header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  gap: 16px;
  margin-bottom: 22px;
}
.header h1 {
  margin: 0;
  font-size: 1.8rem;
}
.muted {
  color: #52606d;
}
.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
  gap: 16px;
}
.card, .panel {
  background: rgba(255, 255, 255, 0.88);
  border: 1px solid rgba(18, 52, 59, 0.08);
  box-shadow: 0 16px 40px rgba(18, 52, 59, 0.08);
  border-radius: 20px;
  padding: 18px 20px;
  backdrop-filter: blur(6px);
}
.metric {
  font-size: 1.9rem;
  font-weight: 700;
  margin: 6px 0;
}
.panel h2 {
  margin-top: 0;
  font-size: 1.2rem;
}
form {
  margin: 0;
}
.form-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 12px;
}
label {
  display: block;
  font-size: 0.92rem;
  color: #334e68;
  margin-bottom: 6px;
}
input, select, textarea {
  width: 100%;
  padding: 10px 12px;
  border-radius: 12px;
  border: 1px solid #cbd2d9;
  box-sizing: border-box;
  font: inherit;
  background: #fff;
}
textarea {
  min-height: 120px;
  resize: vertical;
}
.actions, .inline-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 14px;
}
button, .button {
  border: none;
  border-radius: 999px;
  padding: 10px 16px;
  font: inherit;
  font-weight: 600;
  background: #1f6f78;
  color: #fff;
  cursor: pointer;
  text-decoration: none;
}
button.secondary, .button.secondary {
  background: #d9e7ea;
  color: #12343b;
}
button.warn {
  background: #b45309;
}
table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.94rem;
}
th, td {
  padding: 10px 8px;
  border-bottom: 1px solid #e6edf0;
  text-align: left;
  vertical-align: top;
}
th {
  color: #486581;
  font-size: 0.88rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
}
.badge {
  display: inline-block;
  border-radius: 999px;
  padding: 4px 10px;
  font-size: 0.78rem;
  font-weight: 700;
}
.badge.draft { background: #e7eef3; color: #334e68; }
.badge.approval { background: #fff2cc; color: #8a5d00; }
.badge.approved { background: #dff6dd; color: #1d6f42; }
.badge.sent_accounting { background: #d7ecff; color: #0b5ea8; }
.badge.accepted { background: #d1fae5; color: #065f46; }
.badge.rework { background: #fde2e2; color: #9b1c1c; }
.badge.uploaded { background: #efe8ff; color: #5b21b6; }
.flash {
  padding: 14px 16px;
  border-radius: 14px;
  background: #e8f7f1;
  color: #0f5132;
  margin-bottom: 16px;
}
.error {
  background: #fde8e8;
  color: #842029;
}
.login-shell {
  min-height: 100vh;
  display: grid;
  place-items: center;
  padding: 24px;
}
.login-card {
  width: min(560px, 100%);
  background: rgba(255, 255, 255, 0.92);
  border-radius: 28px;
  padding: 28px;
  box-shadow: 0 24px 64px rgba(18, 52, 59, 0.14);
}
.hint {
  font-size: 0.88rem;
  color: #617d98;
}
.stack > * + * {
  margin-top: 16px;
}
.toolbar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
  flex-wrap: wrap;
}
.small {
  font-size: 0.88rem;
}
@media (max-width: 980px) {
  .layout {
    grid-template-columns: 1fr;
  }
}
"""


STATUS_LABELS = {
    "draft": "Черновик",
    "approval": "На утверждении",
    "approved": "Утвержден",
    "sent_accounting": "Отправлен в бухгалтерию",
    "accepted": "Принят",
    "rework": "Возвращен на доработку",
    "uploaded": "Загружен поставщиком",
}


def status_badge(status: str) -> str:
    label = STATUS_LABELS.get(status, status)
    return f'<span class="badge {escape(status)}">{escape(label)}</span>'


def metric_card(title: str, value: str, subtitle: str) -> str:
    return (
        '<div class="card">'
        f"<div class='muted'>{escape(title)}</div>"
        f"<div class='metric'>{escape(value)}</div>"
        f"<div class='small muted'>{escape(subtitle)}</div>"
        "</div>"
    )


def panel(title: str, inner: str, toolbar: str = "") -> str:
    return (
        '<section class="panel">'
        f"<div class='toolbar'><h2>{escape(title)}</h2>{toolbar}</div>"
        f"{inner}"
        "</section>"
    )


def table(headers: list[str], rows: list[list[str]]) -> str:
    thead = "".join(f"<th>{escape(header)}</th>" for header in headers)
    body_rows = []
    for row in rows:
        cols = "".join(f"<td>{cell}</td>" for cell in row)
        body_rows.append(f"<tr>{cols}</tr>")
    tbody = "".join(body_rows) or f"<tr><td colspan='{len(headers)}' class='muted'>Нет данных.</td></tr>"
    return f"<table><thead><tr>{thead}</tr></thead><tbody>{tbody}</tbody></table>"


def nav(active_path: str, role: str) -> str:
    links = []
    labels = {
        "/dashboard": "Главная",
        "/catalogs?entity=contracts": "Договоры",
        "/catalogs?entity=suppliers": "Поставщики",
        "/catalogs?entity=materials": "Материалы",
        "/catalogs?entity=workers": "Рабочие",
        "/catalogs?entity=users": "Пользователи",
        "/procurement": "Закупки",
        "/warehouse": "Склад",
        "/work": "Работы участков",
        "/writeoffs": "Списание",
        "/ppe": "Спецодежда",
        "/reports": "Отчеты",
        "/archive": "Архив",
        "/supplier": "Кабинет поставщика",
        "/admin": "Администрирование",
    }
    for href in ROLE_NAV.get(role, ["/dashboard"]):
        active = "active" if active_path in href or href in active_path else ""
        links.append(f"<a class='{active}' href='{href}'>{escape(labels.get(href, href))}</a>")
    return "".join(links)


def layout(title: str, content: str, user: dict | None = None, flash: str = "", error: bool = False, active_path: str = "") -> bytes:
    if user is None:
        shell = (
            '<div class="login-shell">'
            '<div class="login-card">'
            f"<h1>{escape(title)}</h1>"
            f"{flash_message(flash, error) if flash else ''}"
            f"{content}"
            "</div></div>"
        )
        return render_document(title, shell)
    sidebar = (
        '<aside class="sidebar">'
        '<div class="brand">АИС учета материалов</div>'
        f"<div class='subtitle'>{escape(ROLE_LABELS.get(user['role'], user['role']))}<br>{escape(user['full_name'])}</div>"
        f"{nav(active_path, user['role'])}"
        '<div style="margin-top:16px;"><a href="/logout">Выйти</a></div>'
        "</aside>"
    )
    header = (
        '<div class="header">'
        f"<div><h1>{escape(title)}</h1><div class='muted'>Рабочее место филиала АО «СТ-1»</div></div>"
        f"<div class='card'><div class='small muted'>Пользователь</div><strong>{escape(user['full_name'])}</strong><br><span class='small'>{escape(ROLE_LABELS.get(user['role'], user['role']))}</span></div>"
        "</div>"
    )
    shell = (
        '<div class="layout">'
        f"{sidebar}"
        '<main class="content">'
        f"{flash_message(flash, error) if flash else ''}"
        f"{header}"
        f"{content}"
        "</main></div>"
    )
    return render_document(title, shell)


def render_document(title: str, body: str) -> bytes:
    html = (
        "<!doctype html><html lang='ru'><head><meta charset='utf-8'>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        f"<title>{escape(title)}</title><style>{BASE_CSS}</style></head><body>{body}</body></html>"
    )
    return html.encode("utf-8")


def flash_message(message: str, error: bool = False) -> str:
    css_class = "flash error" if error else "flash"
    return f"<div class='{css_class}'>{escape(message)}</div>"


def action_form(action: str, hidden_fields: dict[str, str], label: str, css_class: str = "secondary", method: str = "post") -> str:
    hidden_html = [f"<input type='hidden' name='action' value='{escape(action)}'>"]
    for key, value in hidden_fields.items():
        hidden_html.append(f"<input type='hidden' name='{escape(key)}' value='{escape(str(value))}'>")
    hidden_block = "".join(hidden_html)
    return (
        f"<form method='{method}' style='display:inline-block'>"
        f"{hidden_block}"
        f"<button class='{escape(css_class)}' type='submit'>{escape(label)}</button>"
        "</form>"
    )


def url(path: str, **params: str) -> str:
    query = urlencode({k: v for k, v in params.items() if v not in (None, "")})
    return f"{path}?{query}" if query else path
