from __future__ import annotations

import os
from getpass import getpass

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError


class Command(BaseCommand):
    help = "Создает или обновляет стартового администратора продуктовой версии АИС."

    def add_arguments(self, parser) -> None:
        parser.add_argument("--from-env", action="store_true", help="Взять параметры стартового администратора из переменных окружения.")
        parser.add_argument("--username", help="Логин администратора.")
        parser.add_argument("--password", help="Пароль администратора.")
        parser.add_argument("--email", default="", help="Email администратора.")
        parser.add_argument("--first-name", default="", help="Имя администратора.")
        parser.add_argument("--last-name", default="", help="Фамилия администратора.")
        parser.add_argument("--site-name", default="", help="Подразделение или площадка администратора.")
        parser.add_argument("--reset-password", action="store_true", help="Обновить пароль, если пользователь уже существует.")
        parser.add_argument("--noinput", action="store_true", help="Не задавать интерактивных вопросов.")

    def handle(self, *args, **options) -> None:
        payload = self._build_payload(options)
        User = get_user_model()

        user, created = User.objects.get_or_create(username=payload["username"])
        user.email = payload["email"]
        user.first_name = payload["first_name"]
        user.last_name = payload["last_name"]
        user.role = "admin"
        user.site_name = payload["site_name"]
        user.is_staff = True
        user.is_superuser = True
        user.is_active = True

        password = payload["password"]
        if created or payload["reset_password"]:
            user.set_password(password)
        elif password and not user.has_usable_password():
            user.set_password(password)

        user.save()

        if created:
            self.stdout.write(self.style.SUCCESS(f"Стартовый администратор {user.username} создан."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Стартовый администратор {user.username} обновлен."))

    def _build_payload(self, options: dict) -> dict[str, str | bool]:
        use_env = options["from_env"]
        noinput = options["noinput"]

        username = options["username"] or (os.environ.get("AIS_INITIAL_ADMIN_USERNAME") if use_env else "")
        password = options["password"] or (os.environ.get("AIS_INITIAL_ADMIN_PASSWORD") if use_env else "")
        email = options["email"] or (os.environ.get("AIS_INITIAL_ADMIN_EMAIL") if use_env else "")
        first_name = options["first_name"] or (os.environ.get("AIS_INITIAL_ADMIN_FIRST_NAME") if use_env else "")
        last_name = options["last_name"] or (os.environ.get("AIS_INITIAL_ADMIN_LAST_NAME") if use_env else "")
        site_name = options["site_name"] or (os.environ.get("AIS_INITIAL_ADMIN_SITE_NAME") if use_env else "")

        if not username and not noinput:
            username = input("Логин администратора: ").strip()
        if not password and not noinput:
            password = getpass("Пароль администратора: ").strip()

        if not username:
            raise CommandError("Не указан логин администратора. Передайте --username или AIS_INITIAL_ADMIN_USERNAME.")
        if not password:
            raise CommandError("Не указан пароль администратора. Передайте --password или AIS_INITIAL_ADMIN_PASSWORD.")

        return {
            "username": username,
            "password": password,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "site_name": site_name,
            "reset_password": options["reset_password"],
        }
