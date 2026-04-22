from __future__ import annotations

import os
from pathlib import Path

from .env import load_project_env


BASE_DIR = Path(__file__).resolve().parent.parent
load_project_env(BASE_DIR / ".env")

SECRET_KEY = os.environ.get("DJANGO_SECRET_KEY", "ais-2026-dev-secret-key")
DEBUG = os.environ.get("DJANGO_DEBUG", "0") == "1"
ALLOWED_HOSTS = [host.strip() for host in os.environ.get("DJANGO_ALLOWED_HOSTS", "127.0.0.1,localhost,testserver").split(",") if host.strip()]
CSRF_TRUSTED_ORIGINS = [origin.strip() for origin in os.environ.get("DJANGO_CSRF_TRUSTED_ORIGINS", "").split(",") if origin.strip()]

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5433")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "ais_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "root")
POSTGRES_SSLMODE = os.environ.get("POSTGRES_SSLMODE", "prefer")
POSTGRES_CONNECT_TIMEOUT = int(os.environ.get("POSTGRES_CONNECT_TIMEOUT", "10"))

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "apps.core",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "config.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [BASE_DIR / "templates"],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "config.wsgi.application"
ASGI_APPLICATION = "config.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": POSTGRES_DB,
        "USER": POSTGRES_USER,
        "PASSWORD": POSTGRES_PASSWORD,
        "HOST": POSTGRES_HOST,
        "PORT": POSTGRES_PORT,
        "OPTIONS": {
            "sslmode": POSTGRES_SSLMODE,
            "connect_timeout": POSTGRES_CONNECT_TIMEOUT,
        },
    }
}

AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {"NAME": "django.contrib.auth.password_validation.MinimumLengthValidator"},
]

LANGUAGE_CODE = "ru-ru"
TIME_ZONE = "Europe/Moscow"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
STATIC_ROOT = BASE_DIR / "staticfiles"

MEDIA_URL = "/media/"
MEDIA_ROOT = BASE_DIR / "media"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
AUTH_USER_MODEL = "core.User"
LOGIN_URL = "login"
LOGIN_REDIRECT_URL = "dashboard"
LOGOUT_REDIRECT_URL = "login"

SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SECURE = not DEBUG
CSRF_COOKIE_SECURE = not DEBUG
SECURE_BROWSER_XSS_FILTER = True
X_FRAME_OPTIONS = "DENY"

EXPORTS_DIR = BASE_DIR / "exports"
UPLOADS_DIR = BASE_DIR / "uploads"
BACKUPS_DIR = BASE_DIR / "backups"
LEGACY_DIR = BASE_DIR / "legacy"

for path in [EXPORTS_DIR, UPLOADS_DIR, BACKUPS_DIR, LEGACY_DIR, MEDIA_ROOT, BASE_DIR / "static", BASE_DIR / "templates"]:
    path.mkdir(parents=True, exist_ok=True)

REST_FRAMEWORK = {
    "DEFAULT_PERMISSION_CLASSES": ["rest_framework.permissions.IsAuthenticated"],
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.SessionAuthentication",
        "rest_framework.authentication.BasicAuthentication",
    ],
}

CELERY_BROKER_URL = REDIS_URL
CELERY_RESULT_BACKEND = REDIS_URL
CELERY_TASK_ALWAYS_EAGER = os.environ.get("CELERY_TASK_ALWAYS_EAGER", "0") == "1"
CELERY_TASK_TIME_LIMIT = 60 * 5

ORGANIZATION_PROFILE = {
    "name": os.environ.get("AIS_ORGANIZATION_NAME", "АО «СТ-1»"),
    "tax_id": os.environ.get("AIS_ORGANIZATION_TAX_ID", ""),
    "kpp": os.environ.get("AIS_ORGANIZATION_KPP", ""),
    "ogrn": os.environ.get("AIS_ORGANIZATION_OGRN", ""),
    "address": os.environ.get("AIS_ORGANIZATION_ADDRESS", ""),
    "bank_details": os.environ.get("AIS_ORGANIZATION_BANK_DETAILS", ""),
    "requisites": os.environ.get("AIS_ORGANIZATION_REQUISITES", ""),
}
WAREHOUSE_NAME = "Центральный склад"
ROLE_LABELS = {
    "director": "Начальник монтажного объекта",
    "procurement": "Снабженец",
    "warehouse": "Кладовщик",
    "site_manager": "Начальник участка",
    "accounting": "Бухгалтерия",
    "supplier": "Поставщик",
    "admin": "Администратор",
}

