from __future__ import annotations

from dataclasses import dataclass

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand

from apps.core.models import Supplier


@dataclass(frozen=True)
class RoleAccount:
    username: str
    password: str
    role: str
    first_name: str
    last_name: str
    email: str
    site_name: str = ""
    needs_supplier: bool = False
    is_staff: bool = False
    is_superuser: bool = False


ROLE_ACCOUNTS = [
    RoleAccount(
        username="admin",
        password="Admin@AIS2026",
        role="admin",
        first_name="Системный",
        last_name="Администратор",
        email="admin@ais.local",
        site_name="Главный офис",
        is_staff=True,
        is_superuser=True,
    ),
    RoleAccount(
        username="director",
        password="Director@AIS2026",
        role="director",
        first_name="Алексей",
        last_name="Белов",
        email="director@ais.local",
        site_name="Главный офис",
        is_staff=True,
    ),
    RoleAccount(
        username="procurement",
        password="Procure@AIS2026",
        role="procurement",
        first_name="Марина",
        last_name="Соколова",
        email="procurement@ais.local",
        site_name="Отдел снабжения",
        is_staff=True,
    ),
    RoleAccount(
        username="warehouse",
        password="Warehouse@AIS2026",
        role="warehouse",
        first_name="Игорь",
        last_name="Пахомов",
        email="warehouse@ais.local",
        site_name="Центральный склад",
        is_staff=True,
    ),
    RoleAccount(
        username="site_manager",
        password="SiteMgr@AIS2026",
        role="site_manager",
        first_name="Владимир",
        last_name="Орлов",
        email="site_manager@ais.local",
        site_name="Участок №1",
    ),
    RoleAccount(
        username="accounting",
        password="Accounting@AIS2026",
        role="accounting",
        first_name="Наталья",
        last_name="Крылова",
        email="accounting@ais.local",
        site_name="Бухгалтерия",
        is_staff=True,
    ),
    RoleAccount(
        username="supplier",
        password="Supplier@AIS2026",
        role="supplier",
        first_name="Павел",
        last_name="Громов",
        email="supplier@ais.local",
        site_name="Кабинет поставщика",
        needs_supplier=True,
    ),
]


class Command(BaseCommand):
    help = "Создает стартовые учетные записи авторизации по всем ролям системы."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--reset-passwords",
            action="store_true",
            help="Перезаписать пароли, если стартовые пользователи уже существуют.",
        )

    def handle(self, *args, **options) -> None:
        User = get_user_model()
        reset_passwords = options["reset_passwords"]
        supplier = Supplier.objects.get_or_create(
            name='ООО "Базовый поставщик"',
            defaults={
                "tax_id": "0000000000",
                "contact_person": "Ответственный менеджер",
                "phone": "+7 000 000 00 00",
                "email": "supplier@ais.local",
                "address": "Не заполнено",
            },
        )[0]

        created_count = 0
        updated_count = 0
        lines: list[str] = []

        for item in ROLE_ACCOUNTS:
            user, created = User.objects.get_or_create(username=item.username)
            user.role = item.role
            user.first_name = item.first_name
            user.last_name = item.last_name
            user.email = item.email
            user.site_name = item.site_name
            user.is_active = True
            user.is_staff = item.is_staff or item.is_superuser
            user.is_superuser = item.is_superuser
            user.supplier = supplier if item.needs_supplier else None

            if created or reset_passwords or not user.has_usable_password():
                user.set_password(item.password)

            user.save()

            if created:
                created_count += 1
            else:
                updated_count += 1

            lines.append(f"{item.role}: {item.username} / {item.password}")

        self.stdout.write(self.style.SUCCESS(f"Стартовые учетные записи готовы. Создано: {created_count}, обновлено: {updated_count}."))
        self.stdout.write("Реквизиты авторизации:")
        for line in lines:
            self.stdout.write(f"  - {line}")
