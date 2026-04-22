from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from config.env import load_project_env


BASE_DIR = Path(__file__).resolve().parent
load_project_env(BASE_DIR / ".env")


def _runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")
    # `run.py` is the local developer launcher, so we explicitly enable DEBUG
    # to keep static assets and local cookies working over plain HTTP.
    env.setdefault("DJANGO_DEBUG", "1")
    return env


def run_command(*args: str) -> None:
    subprocess.run([sys.executable, "manage.py", *args], check=True, cwd=BASE_DIR, env=_runtime_env())


def maybe_bootstrap_admin() -> None:
    username = os.environ.get("AIS_INITIAL_ADMIN_USERNAME", "").strip()
    password = os.environ.get("AIS_INITIAL_ADMIN_PASSWORD", "").strip()
    if username and password:
        run_command("bootstrap_product", "--from-env", "--noinput")


def main() -> None:
    os.chdir(BASE_DIR)
    run_command("migrate", "--noinput")
    maybe_bootstrap_admin()
    run_command("runserver", "127.0.0.1:8010", "--insecure")


if __name__ == "__main__":
    main()
