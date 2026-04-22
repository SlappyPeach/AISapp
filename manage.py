#!/usr/bin/env python
from __future__ import annotations

import os
import sys
import ctypes.util
from pathlib import Path

from config.env import load_project_env


BASE_DIR = Path(__file__).resolve().parent
VENV_SITE_PACKAGES = BASE_DIR / ".venv" / "Lib" / "site-packages"
load_project_env(BASE_DIR / ".env")


def _add_project_site_packages() -> None:
    if VENV_SITE_PACKAGES.exists():
        site_packages = str(VENV_SITE_PACKAGES)
        if site_packages not in sys.path:
            sys.path.insert(0, site_packages)


def _postgres_bin_candidates() -> list[Path]:
    candidates: list[Path] = []

    env_bin = os.environ.get("POSTGRES_BIN", "").strip()
    if env_bin:
        candidates.append(Path(env_bin))

    env_home = os.environ.get("POSTGRES_HOME", "").strip()
    if env_home:
        candidates.append(Path(env_home) / "bin")

    program_files = Path(os.environ.get("ProgramFiles", r"C:\Program Files"))
    postgres_root = program_files / "PostgreSQL"
    if postgres_root.exists():
        for child in sorted(postgres_root.iterdir(), reverse=True):
            candidate = child / "bin"
            if candidate.exists():
                candidates.append(candidate)

    return candidates


def _patch_windows_libpq_lookup() -> None:
    if os.name != "nt":
        return

    libpq_path = None
    for candidate in _postgres_bin_candidates():
        maybe_libpq = candidate / "libpq.dll"
        if maybe_libpq.exists():
            libpq_path = maybe_libpq.resolve()
            try:
                os.add_dll_directory(str(libpq_path.parent))
            except (AttributeError, FileNotFoundError):
                pass
            break

    if not libpq_path:
        return

    original_find_library = ctypes.util.find_library

    def patched_find_library(name: str):
        if name in {"libpq.dll", "libpq", "pq"}:
            return str(libpq_path)
        return original_find_library(name)

    ctypes.util.find_library = patched_find_library


_add_project_site_packages()
_patch_windows_libpq_lookup()

import psycopg
from psycopg import sql


def _build_admin_dsn() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5433")
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "root")
    sslmode = os.environ.get("POSTGRES_SSLMODE", "prefer")
    timeout = os.environ.get("POSTGRES_CONNECT_TIMEOUT", "10")
    return f"host={host} port={port} dbname=postgres user={user} password={password} sslmode={sslmode} connect_timeout={timeout}"


def ensure_database() -> None:
    database = os.environ.get("POSTGRES_DB", "ais_db")
    with psycopg.connect(_build_admin_dsn(), autocommit=True) as conn:
        exists = conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (database,)).fetchone()
        if not exists:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))


def main() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    if len(sys.argv) > 1 and sys.argv[1] not in {"help", "--help", "version", "--version"}:
        ensure_database()
    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)


if __name__ == "__main__":
    main()
