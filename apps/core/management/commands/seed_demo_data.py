from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any

from django.conf import settings
from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from apps.core.models import (
    AuditLog,
    ConstructionObject,
    DocumentRecord,
    DocumentStatus,
    DocumentType,
    Material,
    MaterialNorm,
    PPEIssuance,
    PPEIssuanceLine,
    PrimaryDocument,
    ProcurementRequest,
    RoleChoices,
    SMRContract,
    StockIssue,
    StockReceipt,
    Supplier,
    SupplierDocument,
    SupplyContract,
    WorkLog,
    Worker,
    WriteOffAct,
)
from apps.core.services import (
    create_ppe_issuance,
    create_primary_document,
    create_procurement_request,
    create_stock_issue,
    create_stock_receipt,
    create_supplier_document,
    create_work_log,
    create_writeoff,
    transition_document,
)


DEFAULT_TOP_RECORDS = 72
DEFAULT_PASSWORD = "Demo@AIS2026"
DEFAULT_PREFIX = "DEMO"


@dataclass(frozen=True)
class SiteSeed:
    code: str
    site_name: str
    object_name: str
    customer_name: str
    address: str


@dataclass(frozen=True)
class SupplierSeed:
    name: str
    contact_person: str
    phone: str
    email: str


@dataclass(frozen=True)
class MaterialSeed:
    code_suffix: str
    name: str
    unit: str
    price: str
    min_stock: str
    category: str
    is_ppe: bool = False


@dataclass(frozen=True)
class WorkTypeRecipe:
    name: str
    unit: str
    norms: list[tuple[str, str]]


SITE_SEEDS = [
    SiteSeed(
        code="north",
        site_name="Участок Север-1",
        object_name="ЖК Северный квартал",
        customer_name='ООО "Север Девелопмент"',
        address="г. Москва, ул. Полярная, 18",
    ),
    SiteSeed(
        code="south",
        site_name="Участок Юг-2",
        object_name="Логистический парк Южный",
        customer_name='АО "ТранспортИнвест"',
        address="Московская обл., Каширское ш., 11",
    ),
    SiteSeed(
        code="east",
        site_name="Участок Восток-3",
        object_name="БЦ Восточный",
        customer_name='ООО "Восток Проект"',
        address="г. Москва, Щелковское ш., 42",
    ),
]

SUPPLIER_SEEDS = [
    SupplierSeed('ООО "СеверЭлектро Снаб"', "Анна Кириллова", "+7 495 111 00 01", "north@demo.ais.local"),
    SupplierSeed('ООО "КабельИмпорт"', "Игорь Смирнов", "+7 495 111 00 02", "cable@demo.ais.local"),
    SupplierSeed('ООО "ИнжСтрой Логистика"', "Марина Соколова", "+7 495 111 00 03", "log@demo.ais.local"),
    SupplierSeed('ООО "Безопасность Плюс"', "Павел Новиков", "+7 495 111 00 04", "safe@demo.ais.local"),
]

MATERIAL_SEEDS = [
    MaterialSeed("MAT-001", "Кабель ВВГнг-LS 3x2.5", "м", "185.00", "120", "Кабельная продукция"),
    MaterialSeed("MAT-002", "Кабель UTP cat.6", "м", "62.00", "200", "Слаботочные сети"),
    MaterialSeed("MAT-003", "Гофротруба ПНД 25", "м", "54.00", "80", "Кабеленесущие системы"),
    MaterialSeed("MAT-004", "Труба ПВХ 32", "м", "71.00", "60", "Кабеленесущие системы"),
    MaterialSeed("MAT-005", "Анкер-клин 6x40", "шт", "8.50", "500", "Крепеж"),
    MaterialSeed("MAT-006", "Саморез 4.2x32", "шт", "2.40", "1200", "Крепеж"),
    MaterialSeed("MAT-007", "Кабель-канал 40x25", "м", "120.00", "40", "Кабеленесущие системы"),
    MaterialSeed("MAT-008", "Распаячная коробка IP54", "шт", "165.00", "30", "Электромонтаж"),
    MaterialSeed("MAT-009", "Шкаф управления ШУ-24", "шт", "24500.00", "3", "Щитовое оборудование"),
    MaterialSeed("MAT-010", "Автоматический выключатель 16A", "шт", "480.00", "40", "Щитовое оборудование"),
    MaterialSeed("MAT-011", "Светильник LED 36W", "шт", "2150.00", "20", "Освещение"),
    MaterialSeed("MAT-012", "Датчик дыма адресный", "шт", "1750.00", "25", "Пожарная сигнализация"),
    MaterialSeed("MAT-013", "IP камера уличная", "шт", "8600.00", "10", "Видеонаблюдение"),
    MaterialSeed("MAT-014", "Коммутатор PoE 8 портов", "шт", "11200.00", "6", "Видеонаблюдение"),
    MaterialSeed("PPE-001", "Каска защитная", "шт", "950.00", "10", "СИЗ", True),
    MaterialSeed("PPE-002", "Костюм сигнальный", "компл", "3650.00", "8", "СИЗ", True),
    MaterialSeed("PPE-003", "Ботинки защитные", "пар", "4200.00", "8", "СИЗ", True),
    MaterialSeed("PPE-004", "Перчатки монтажные", "пар", "180.00", "40", "СИЗ", True),
]

WORK_TYPE_RECIPES = [
    WorkTypeRecipe(
        name="Монтаж кабельной трассы",
        unit="этап",
        norms=[("MAT-001", "18.0"), ("MAT-003", "10.0"), ("MAT-005", "24.0")],
    ),
    WorkTypeRecipe(
        name="Монтаж электрощита",
        unit="щит",
        norms=[("MAT-009", "1.0"), ("MAT-010", "6.0"), ("MAT-001", "12.0")],
    ),
    WorkTypeRecipe(
        name="Система видеонаблюдения",
        unit="пост",
        norms=[("MAT-002", "28.0"), ("MAT-013", "1.0"), ("MAT-014", "0.2")],
    ),
    WorkTypeRecipe(
        name="Монтаж освещения",
        unit="линия",
        norms=[("MAT-011", "4.0"), ("MAT-001", "16.0"), ("MAT-008", "2.0")],
    ),
]


class Command(BaseCommand):
    help = "Создает демонстрационный набор данных АИС для показов и безопасного рефакторинга."

    def add_arguments(self, parser) -> None:
        parser.add_argument(
            "--top-records",
            type=int,
            default=DEFAULT_TOP_RECORDS,
            help="Количество верхнеуровневых операционных записей (по умолчанию 72, рекомендуемый диапазон 50-100).",
        )
        parser.add_argument(
            "--prefix",
            default=DEFAULT_PREFIX,
            help="Префикс демо-данных для номеров документов, кодов материалов и логинов.",
        )
        parser.add_argument(
            "--password",
            default=DEFAULT_PASSWORD,
            help="Общий пароль для всех созданных демо-пользователей.",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Удалить ранее созданный набор с тем же префиксом и заполнить заново.",
        )

    def handle(self, *args, **options) -> None:
        top_records = int(options["top_records"])
        if top_records < 12:
            raise CommandError("Для связного демо-набора укажите минимум 12 верхнеуровневых записей.")
        if top_records > 100:
            raise CommandError("Команда рассчитана максимум на 100 верхнеуровневых записей в одном наборе.")

        self.prefix_upper = str(options["prefix"]).strip().upper() or DEFAULT_PREFIX
        self.prefix_lower = self.prefix_upper.lower()
        self.password = str(options["password"]).strip() or DEFAULT_PASSWORD
        self.demo_tag = f"[DEMO-SEED {self.prefix_upper}]"
        self.rng = random.Random(f"{self.prefix_upper}:{top_records}")
        self.today = timezone.localdate()

        if self._demo_exists():
            if not options["replace"]:
                raise CommandError(
                    f"Демо-данные с префиксом {self.prefix_upper} уже существуют. "
                    f"Запустите команду с --replace, если хотите пересоздать набор."
                )
            self._purge_existing_demo()

        counts = self._build_business_counts(top_records)

        with transaction.atomic():
            refs = self._ensure_reference_data()
            contracts = self._create_contracts(counts["smr_contracts"], refs)
            supply_contracts = self._create_supply_contracts(counts["supply_contracts"], refs, contracts)
            requests = self._create_procurement_requests(counts["procurement_requests"], refs, contracts, supply_contracts)
            supplier_documents = self._create_supplier_documents(counts["supplier_documents"], refs, requests, supply_contracts)
            receipts = self._create_stock_receipts(counts["stock_receipts"], refs, requests, supplier_documents)
            primary_documents = self._create_primary_documents(counts["primary_documents"], refs, requests, receipts, supply_contracts)
            issues = self._create_stock_issues(counts["stock_issues"], refs, contracts)
            work_logs = self._create_work_logs(counts["work_logs"], refs, contracts)
            writeoffs = self._create_writeoffs(counts["writeoffs"], refs, contracts)
            ppe_issuances = self._create_ppe_issuances(counts["ppe_issuances"], refs)

        self._print_summary(
            refs=refs,
            counts=counts,
            created={
                "contracts": len(contracts),
                "supply_contracts": len(supply_contracts),
                "requests": len(requests),
                "supplier_documents": len(supplier_documents),
                "receipts": len(receipts),
                "primary_documents": len(primary_documents),
                "issues": len(issues),
                "work_logs": len(work_logs),
                "writeoffs": len(writeoffs),
                "ppe_issuances": len(ppe_issuances),
            },
        )

    def _build_business_counts(self, top_records: int) -> dict[str, int]:
        weights = [
            ("smr_contracts", 0.08),
            ("supply_contracts", 0.07),
            ("procurement_requests", 0.18),
            ("supplier_documents", 0.11),
            ("primary_documents", 0.11),
            ("stock_receipts", 0.13),
            ("stock_issues", 0.11),
            ("work_logs", 0.11),
            ("writeoffs", 0.06),
            ("ppe_issuances", 0.04),
        ]
        counts: dict[str, int] = {}
        remainders: list[tuple[float, str]] = []
        current_total = 0
        for key, weight in weights:
            raw = top_records * weight
            base = max(1, int(raw))
            counts[key] = base
            current_total += base
            remainders.append((raw - int(raw), key))

        if current_total < top_records:
            for _remainder, key in sorted(remainders, reverse=True):
                if current_total >= top_records:
                    break
                counts[key] += 1
                current_total += 1

        while current_total > top_records:
            for _remainder, key in sorted(remainders):
                if current_total <= top_records:
                    break
                if counts[key] > 1:
                    counts[key] -= 1
                    current_total -= 1
        return counts

    def _demo_exists(self) -> bool:
        return (
            SMRContract.objects.filter(number__startswith=f"{self.prefix_upper}-SMR-").exists()
            or SupplyContract.objects.filter(number__startswith=f"{self.prefix_upper}-SUP-").exists()
            or ProcurementRequest.objects.filter(notes__icontains=self.demo_tag).exists()
            or SupplierDocument.objects.filter(notes__icontains=self.demo_tag).exists()
            or PrimaryDocument.objects.filter(notes__icontains=self.demo_tag).exists()
            or StockReceipt.objects.filter(notes__icontains=self.demo_tag).exists()
            or StockIssue.objects.filter(notes__icontains=self.demo_tag).exists()
            or WriteOffAct.objects.filter(notes__icontains=self.demo_tag).exists()
            or PPEIssuance.objects.filter(notes__icontains=self.demo_tag).exists()
            or WorkLog.objects.filter(notes__icontains=self.demo_tag).exists()
        )

    def _purge_existing_demo(self) -> None:
        demo_users = get_user_model().objects.filter(username__startswith=f"{self.prefix_lower}_")
        AuditLog.objects.filter(Q(user__in=demo_users) | Q(details__icontains=self.demo_tag)).delete()
        WorkLog.objects.filter(notes__icontains=self.demo_tag).delete()
        PPEIssuance.objects.filter(notes__icontains=self.demo_tag).delete()
        WriteOffAct.objects.filter(notes__icontains=self.demo_tag).delete()
        StockIssue.objects.filter(notes__icontains=self.demo_tag).delete()
        PrimaryDocument.objects.filter(notes__icontains=self.demo_tag).delete()
        StockReceipt.objects.filter(notes__icontains=self.demo_tag).delete()
        SupplierDocument.objects.filter(notes__icontains=self.demo_tag).delete()
        ProcurementRequest.objects.filter(notes__icontains=self.demo_tag).delete()
        SupplyContract.objects.filter(number__startswith=f"{self.prefix_upper}-SUP-").delete()
        SMRContract.objects.filter(number__startswith=f"{self.prefix_upper}-SMR-").delete()

    def _ensure_reference_data(self) -> dict[str, Any]:
        User = get_user_model()
        suppliers = self._ensure_suppliers()
        document_types = self._ensure_document_types()
        users = self._ensure_users(User=User, suppliers=suppliers)
        site_contexts = self._ensure_sites(users)
        materials = self._ensure_materials()
        recipes = self._ensure_norms(materials)
        workers_by_site = self._ensure_workers(site_contexts)
        return {
            "suppliers": suppliers,
            "document_types": document_types,
            "users": users,
            "sites": site_contexts,
            "materials": materials,
            "recipes": recipes,
            "workers_by_site": workers_by_site,
        }

    def _ensure_suppliers(self) -> list[Supplier]:
        suppliers: list[Supplier] = []
        for index, seed in enumerate(SUPPLIER_SEEDS, start=1):
            supplier, _created = Supplier.objects.update_or_create(
                name=seed.name,
                defaults={
                    "tax_id": f"77{index:08d}",
                    "contact_person": seed.contact_person,
                    "phone": seed.phone,
                    "email": seed.email,
                    "address": f"Демо-адрес поставщика {index}. {self.demo_tag}",
                    "requisites": f"ИНН 77{index:08d}; договорные условия для демо-набора. {self.demo_tag}",
                },
            )
            suppliers.append(supplier)
        return suppliers

    def _ensure_document_types(self) -> dict[str, DocumentType]:
        result: dict[str, DocumentType] = {}
        definitions = [
            {
                "code": "invoice",
                "name": "Счет",
                "prefix": "INV",
                "available_for_upload": True,
                "available_for_generation": True,
                "requires_items": True,
            },
            {
                "code": "invoice_facture",
                "name": "Счет-фактура",
                "prefix": "SF",
                "available_for_upload": True,
                "available_for_generation": True,
                "requires_items": True,
            },
            {
                "code": "upd",
                "name": "УПД",
                "prefix": "UPD",
                "available_for_upload": True,
                "available_for_generation": True,
                "requires_items": True,
            },
        ]
        for definition in definitions:
            defaults = {
                "code": definition["code"],
                "name": definition["name"],
                "prefix": definition["prefix"],
                "is_active": True,
                "available_for_upload": definition["available_for_upload"],
                "available_for_generation": definition["available_for_generation"],
                "requires_items": definition["requires_items"],
                "description": f"Тип документа для демонстрационного контура. {self.demo_tag}",
            }
            document_type = DocumentType.objects.filter(
                Q(code=definition["code"]) | Q(name=definition["name"]) | Q(prefix=definition["prefix"])
            ).first()
            if document_type is None:
                document_type = DocumentType.objects.create(**defaults)
            else:
                for field_name, field_value in defaults.items():
                    setattr(document_type, field_name, field_value)
                document_type.save()
            result[definition["code"]] = document_type
        return result

    def _ensure_users(self, *, User, suppliers: list[Supplier]) -> dict[str, Any]:
        office_users = [
            {
                "username": f"{self.prefix_lower}_admin",
                "first_name": "Демо",
                "last_name": "Администратор",
                "email": "demo_admin@ais.local",
                "role": RoleChoices.ADMIN,
                "site_name": "Главный офис",
                "is_staff": True,
                "is_superuser": True,
                "supplier": None,
            },
            {
                "username": f"{self.prefix_lower}_director",
                "first_name": "Алексей",
                "last_name": "Родионов",
                "email": "demo_director@ais.local",
                "role": RoleChoices.DIRECTOR,
                "site_name": "Главный офис",
                "is_staff": True,
                "is_superuser": False,
                "supplier": None,
            },
            {
                "username": f"{self.prefix_lower}_procurement",
                "first_name": "Марина",
                "last_name": "Соколова",
                "email": "demo_procurement@ais.local",
                "role": RoleChoices.PROCUREMENT,
                "site_name": "Отдел снабжения",
                "is_staff": True,
                "is_superuser": False,
                "supplier": None,
            },
            {
                "username": f"{self.prefix_lower}_warehouse",
                "first_name": "Игорь",
                "last_name": "Пахомов",
                "email": "demo_warehouse@ais.local",
                "role": RoleChoices.WAREHOUSE,
                "site_name": settings.WAREHOUSE_NAME,
                "is_staff": True,
                "is_superuser": False,
                "supplier": None,
            },
            {
                "username": f"{self.prefix_lower}_accounting",
                "first_name": "Наталья",
                "last_name": "Крылова",
                "email": "demo_accounting@ais.local",
                "role": RoleChoices.ACCOUNTING,
                "site_name": "Бухгалтерия",
                "is_staff": True,
                "is_superuser": False,
                "supplier": None,
            },
        ]
        users: dict[str, Any] = {}
        for payload in office_users:
            users[payload["role"]] = self._upsert_user(User=User, **payload)

        site_managers: dict[str, Any] = {}
        for seed in SITE_SEEDS:
            site_managers[seed.code] = self._upsert_user(
                User=User,
                username=f"{self.prefix_lower}_site_{seed.code}",
                first_name="Начальник",
                last_name=seed.site_name.replace("Участок ", ""),
                email=f"demo_site_{seed.code}@ais.local",
                role=RoleChoices.SITE_MANAGER,
                site_name=seed.site_name,
                is_staff=False,
                is_superuser=False,
                supplier=None,
            )
        users["site_managers"] = site_managers

        supplier_users: dict[int, Any] = {}
        for index, supplier in enumerate(suppliers, start=1):
            supplier_users[supplier.id] = self._upsert_user(
                User=User,
                username=f"{self.prefix_lower}_supplier_{index:02d}",
                first_name="Поставщик",
                last_name=str(index),
                email=f"demo_supplier_{index:02d}@ais.local",
                role=RoleChoices.SUPPLIER,
                site_name="Кабинет поставщика",
                is_staff=False,
                is_superuser=False,
                supplier=supplier,
            )
        users["supplier_users"] = supplier_users
        return users

    def _upsert_user(
        self,
        *,
        User,
        username: str,
        first_name: str,
        last_name: str,
        email: str,
        role: str,
        site_name: str,
        is_staff: bool,
        is_superuser: bool,
        supplier: Supplier | None,
    ):
        user, _created = User.objects.update_or_create(
            username=username,
            defaults={
                "first_name": first_name,
                "last_name": last_name,
                "email": email,
                "role": role,
                "site_name": site_name,
                "is_active": True,
                "is_staff": is_staff or is_superuser,
                "is_superuser": is_superuser,
                "supplier": supplier,
            },
        )
        user.set_password(self.password)
        user.save()
        return user

    def _ensure_sites(self, users: dict[str, Any]) -> list[dict[str, Any]]:
        sites: list[dict[str, Any]] = []
        for seed in SITE_SEEDS:
            construction_object, _created = ConstructionObject.objects.update_or_create(
                name=f"{self.prefix_upper} {seed.object_name}",
                defaults={
                    "address": seed.address,
                    "customer_name": seed.customer_name,
                    "customer_requisites": f"Реквизиты заказчика для демо-набора. {self.demo_tag}",
                    "description": f"Строительный объект для демонстрационной базы. {self.demo_tag}",
                    "start_date": self.today - timedelta(days=180),
                    "end_date": self.today + timedelta(days=240),
                },
            )
            sites.append(
                {
                    "seed": seed,
                    "object": construction_object,
                    "site_name": seed.site_name,
                    "manager": users["site_managers"][seed.code],
                }
            )
        return sites

    def _ensure_materials(self) -> dict[str, Material]:
        materials: dict[str, Material] = {}
        for seed in MATERIAL_SEEDS:
            code = f"{self.prefix_upper}-{seed.code_suffix}"
            material, _created = Material.objects.update_or_create(
                code=code,
                defaults={
                    "name": seed.name,
                    "unit": seed.unit,
                    "price": Decimal(seed.price),
                    "min_stock": Decimal(seed.min_stock),
                    "category": seed.category,
                    "is_ppe": seed.is_ppe,
                },
            )
            materials[seed.code_suffix] = material
        return materials

    def _ensure_norms(self, materials: dict[str, Material]) -> dict[str, WorkTypeRecipe]:
        recipes: dict[str, WorkTypeRecipe] = {}
        for recipe in WORK_TYPE_RECIPES:
            for material_suffix, norm_value in recipe.norms:
                MaterialNorm.objects.update_or_create(
                    work_type=recipe.name,
                    material=materials[material_suffix],
                    defaults={
                        "norm_per_unit": Decimal(norm_value),
                        "unit": recipe.unit,
                        "notes": f"Норма расхода для демонстрационного набора. {self.demo_tag}",
                    },
                )
            recipes[recipe.name] = recipe
        return recipes

    def _ensure_workers(self, sites: list[dict[str, Any]]) -> dict[str, list[Worker]]:
        last_names = ["Иванов", "Петров", "Сидоров", "Кузнецов", "Орлов", "Зайцев"]
        first_names = ["Иван", "Павел", "Денис", "Олег", "Артем", "Егор"]
        positions = ["Монтажник", "Электромонтажник", "Бригадир", "Наладчик"]
        workers_by_site: dict[str, list[Worker]] = {site["site_name"]: [] for site in sites}
        for index in range(18):
            site = sites[index % len(sites)]
            full_name = f"{last_names[index % len(last_names)]} {first_names[index % len(first_names)]}"
            worker, _created = Worker.objects.update_or_create(
                employee_number=f"{self.prefix_upper}-EMP-{index + 1:03d}",
                defaults={
                    "full_name": full_name,
                    "site_name": site["site_name"],
                    "position": positions[index % len(positions)],
                    "hire_date": self.today - timedelta(days=240 - index * 5),
                },
            )
            workers_by_site[site["site_name"]].append(worker)
        return workers_by_site

    def _create_contracts(self, count: int, refs: dict[str, Any]) -> list[dict[str, Any]]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
                DocumentStatus.REWORK,
            ],
        )
        contracts: list[dict[str, Any]] = []
        for index in range(count):
            site = refs["sites"][index % len(refs["sites"])]
            recipe = WORK_TYPE_RECIPES[index % len(WORK_TYPE_RECIPES)]
            creator = refs["users"][RoleChoices.DIRECTOR if index % 3 else RoleChoices.ADMIN]
            contract = SMRContract.objects.create(
                number=f"{self.prefix_upper}-SMR-{index + 1:03d}",
                contract_date=self.today - timedelta(days=150 - index * 6),
                object=site["object"],
                customer_name=site["seed"].customer_name,
                customer_requisites=f"Типовой договор заказчика. {self.demo_tag}",
                contractor_name=str(settings.ORGANIZATION_PROFILE.get("name", "")).strip() or "АО «СТ-1»",
                contractor_requisites=f"Реквизиты подрядчика для демонстрации. {self.demo_tag}",
                subject=f"{recipe.name} на объекте {site['object'].name}",
                work_type=recipe.name,
                planned_volume=Decimal(str(8 + index)),
                volume_unit=recipe.unit,
                amount=Decimal(str(250000 + index * 35000)),
                vat_rate=Decimal("20.00"),
                start_date=self.today - timedelta(days=120 - index * 5),
                end_date=self.today + timedelta(days=90 + index * 3),
                status=DocumentStatus.DRAFT,
                created_by=creator,
            )
            self._transition_default_entity(
                entity_type="smr_contract",
                instance=contract,
                target_status=statuses[index],
                creator=creator,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            contracts.append({"instance": contract, "site": site, "recipe": recipe})
        return contracts

    def _create_supply_contracts(self, count: int, refs: dict[str, Any], contracts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
            ],
        )
        supply_contracts: list[dict[str, Any]] = []
        for index in range(count):
            supplier = refs["suppliers"][index % len(refs["suppliers"])]
            related_contract = contracts[index % len(contracts)]["instance"]
            creator = refs["users"][RoleChoices.DIRECTOR if index % 2 else RoleChoices.ADMIN]
            supply_contract = SupplyContract.objects.create(
                number=f"{self.prefix_upper}-SUP-{index + 1:03d}",
                contract_date=self.today - timedelta(days=120 - index * 4),
                supplier=supplier,
                related_smr_contract=related_contract,
                amount=Decimal(str(180000 + index * 27000)),
                status=DocumentStatus.DRAFT,
                terms=f"Поставка материалов по графику объекта. {self.demo_tag}",
            )
            self._transition_default_entity(
                entity_type="supply_contract",
                instance=supply_contract,
                target_status=statuses[index],
                creator=creator,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            supply_contracts.append({"instance": supply_contract, "supplier": supplier})
        return supply_contracts

    def _create_procurement_requests(
        self,
        count: int,
        refs: dict[str, Any],
        contracts: list[dict[str, Any]],
        supply_contracts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
                DocumentStatus.REWORK,
            ],
        )
        requests: list[dict[str, Any]] = []
        for index in range(count):
            contract_payload = contracts[index % len(contracts)]
            supplier = refs["suppliers"][index % len(refs["suppliers"])]
            creator = contract_payload["site"]["manager"] if index % 2 == 0 else refs["users"][RoleChoices.PROCUREMENT]
            item_lines = self._recipe_item_lines(contract_payload["recipe"], base_multiplier=Decimal("1.2"), extra_items=index % 2)
            request = create_procurement_request(
                user=creator,
                cleaned_data={
                    "request_date": self.today - timedelta(days=90 - index * 2),
                    "site_name": contract_payload["site"]["site_name"],
                    "contract": contract_payload["instance"],
                    "supplier": supplier,
                    "status": DocumentStatus.DRAFT,
                    "notes": f"Плановая заявка на закупку для демонстрационного сценария. {self.demo_tag}",
                    "items": self._format_line_items(item_lines),
                },
                ip_address="10.10.0.10",
            )
            self._transition_default_entity(
                entity_type="procurement_request",
                instance=request,
                target_status=statuses[index],
                creator=creator,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            requests.append(
                {
                    "instance": request,
                    "contract": contract_payload["instance"],
                    "supplier": supplier,
                    "site": contract_payload["site"],
                    "recipe": contract_payload["recipe"],
                }
            )
        return requests

    def _create_supplier_documents(
        self,
        count: int,
        refs: dict[str, Any],
        requests: list[dict[str, Any]],
        supply_contracts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.SUPPLY_CONFIRMED,
                DocumentStatus.UPLOADED,
                DocumentStatus.REWORK,
                DocumentStatus.APPROVAL,
            ],
        )
        document_types = list(refs["document_types"].values())
        documents: list[dict[str, Any]] = []
        for index in range(count):
            linked_request = requests[index % len(requests)] if requests else None
            linked_contract = supply_contracts[index % len(supply_contracts)] if supply_contracts else None
            supplier = linked_request["supplier"] if linked_request and index % 2 == 0 else linked_contract["supplier"]
            request_instance = linked_request["instance"] if linked_request and index % 2 == 0 else None
            supply_contract_instance = None
            if request_instance is None and linked_contract and linked_contract["supplier"].id == supplier.id:
                supply_contract_instance = linked_contract["instance"]
            supplier_user = refs["users"]["supplier_users"][supplier.id]
            doc_type = document_types[index % len(document_types)]
            amount = Decimal(str(48000 + index * 5300))
            document = create_supplier_document(
                user=supplier_user,
                cleaned_data={
                    "supplier": supplier,
                    "request": request_instance,
                    "supply_contract": supply_contract_instance,
                    "doc_type": doc_type.name,
                    "doc_number": f"{self.prefix_upper}-SD-{index + 1:03d}",
                    "doc_date": self.today - timedelta(days=60 - index * 2),
                    "amount": amount,
                    "vat_amount": (amount * Decimal("0.20")).quantize(Decimal("0.01")),
                    "attachment": None,
                    "notes": f"Документ поставщика для демонстрационного сценария. {self.demo_tag}",
                },
                ip_address="10.20.0.10",
            )
            self._transition_supplier_document(
                instance=document,
                target_status=statuses[index],
                supplier_user=supplier_user,
                reviewer=refs["users"][RoleChoices.PROCUREMENT],
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            documents.append(
                {
                    "instance": document,
                    "supplier": supplier,
                    "request": request_instance,
                }
            )
        return documents

    def _create_stock_receipts(
        self,
        count: int,
        refs: dict[str, Any],
        requests: list[dict[str, Any]],
        supplier_documents: list[dict[str, Any]],
    ) -> list[StockReceipt]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
            ],
        )
        receipts: list[StockReceipt] = []
        regular_materials = [material for material in refs["materials"].values() if not material.is_ppe]
        ppe_materials = [material for material in refs["materials"].values() if material.is_ppe]
        warehouse_user = refs["users"][RoleChoices.WAREHOUSE]
        for index in range(count):
            linked_request = requests[index % len(requests)] if requests else None
            supplier = linked_request["supplier"] if linked_request else refs["suppliers"][index % len(refs["suppliers"])]
            related_document = next(
                (
                    item["instance"]
                    for item in supplier_documents
                    if item["supplier"].id == supplier.id
                ),
                None,
            )
            item_lines: list[dict[str, Any]]
            if linked_request:
                item_lines = []
                for line in linked_request["instance"].lines.select_related("material")[:3]:
                    item_lines.append(
                        {
                            "material": line.material,
                            "quantity": max(line.quantity * Decimal("2.5"), Decimal("12.000")),
                            "unit_price": line.unit_price or line.material.price,
                            "notes": f"Пополнение склада по заявке. {self.demo_tag}",
                        }
                    )
            else:
                item_lines = self._stock_replenishment_lines(regular_materials, start=index, count=3, base_quantity=Decimal("40"))
            item_lines.extend(
                self._stock_replenishment_lines(
                    regular_materials,
                    start=index * 3,
                    count=3,
                    base_quantity=Decimal("60"),
                )
            )
            for ppe_material in ppe_materials:
                item_lines.append(
                    {
                        "material": ppe_material,
                        "quantity": Decimal("6"),
                        "unit_price": ppe_material.price,
                        "notes": f"Пополнение склада СИЗ. {self.demo_tag}",
                    }
                )
            receipt = create_stock_receipt(
                user=warehouse_user,
                cleaned_data={
                    "receipt_date": self.today - timedelta(days=55 - index * 2),
                    "supplier": supplier,
                    "supplier_document": related_document,
                    "status": DocumentStatus.DRAFT,
                    "notes": f"Приход на центральный склад. {self.demo_tag}",
                    "items": self._format_line_items(item_lines),
                },
                ip_address="10.30.0.10",
            )
            self._transition_default_entity(
                entity_type="stock_receipt",
                instance=receipt,
                target_status=statuses[index],
                creator=warehouse_user,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            receipts.append(receipt)
        return receipts

    def _create_primary_documents(
        self,
        count: int,
        refs: dict[str, Any],
        requests: list[dict[str, Any]],
        receipts: list[StockReceipt],
        supply_contracts: list[dict[str, Any]],
    ) -> list[PrimaryDocument]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
            ],
        )
        document_types = list(refs["document_types"].values())
        procurement_user = refs["users"][RoleChoices.PROCUREMENT]
        warehouse_user = refs["users"][RoleChoices.WAREHOUSE]
        documents: list[PrimaryDocument] = []
        for index in range(count):
            document_type = document_types[index % len(document_types)]
            linked_request = requests[index % len(requests)]["instance"] if requests and index % 2 == 0 else None
            linked_receipt = receipts[index % len(receipts)] if receipts and linked_request is None else None
            linked_supply_contract = None
            if linked_request is None and linked_receipt is None and supply_contracts:
                linked_supply_contract = supply_contracts[index % len(supply_contracts)]["instance"]
            creator = procurement_user if index % 2 == 0 else warehouse_user
            document = create_primary_document(
                user=creator,
                cleaned_data={
                    "document_type": document_type,
                    "doc_date": self.today - timedelta(days=40 - index * 2),
                    "supplier": None,
                    "request": linked_request,
                    "supply_contract": linked_supply_contract,
                    "stock_receipt": linked_receipt,
                    "status": DocumentStatus.DRAFT,
                    "amount": Decimal("0"),
                    "vat_amount": Decimal("0"),
                    "notes": f"Первичный документ для демонстрационного набора. {self.demo_tag}",
                    "items": "",
                },
                ip_address="10.40.0.10",
            )
            self._transition_default_entity(
                entity_type="primary_document",
                instance=document,
                target_status=statuses[index],
                creator=creator,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            documents.append(document)
        return documents

    def _create_stock_issues(self, count: int, refs: dict[str, Any], contracts: list[dict[str, Any]]) -> list[StockIssue]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
            ],
        )
        warehouse_user = refs["users"][RoleChoices.WAREHOUSE]
        issues: list[StockIssue] = []
        for index in range(count):
            contract_payload = contracts[index % len(contracts)]
            item_lines = self._recipe_item_lines(contract_payload["recipe"], base_multiplier=Decimal("3.0"), extra_items=0)
            issue = create_stock_issue(
                user=warehouse_user,
                cleaned_data={
                    "issue_date": self.today - timedelta(days=30 - index),
                    "site_name": contract_payload["site"]["site_name"],
                    "contract": contract_payload["instance"],
                    "received_by_name": contract_payload["site"]["manager"].full_name_or_username,
                    "status": DocumentStatus.DRAFT,
                    "notes": f"Отпуск материалов на участок для демонстрации. {self.demo_tag}",
                    "items": self._format_line_items(item_lines),
                },
                ip_address="10.50.0.10",
            )
            self._transition_default_entity(
                entity_type="stock_issue",
                instance=issue,
                target_status=statuses[index],
                creator=warehouse_user,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            issues.append(issue)
        return issues

    def _create_work_logs(self, count: int, refs: dict[str, Any], contracts: list[dict[str, Any]]) -> list[WorkLog]:
        statuses = ["planned", "planned", "delayed", "Завершено"]
        logs: list[WorkLog] = []
        for index in range(count):
            contract_payload = contracts[index % len(contracts)]
            manager = contract_payload["site"]["manager"]
            planned_volume = Decimal(str(2 + index % 4))
            actual_volume = planned_volume - Decimal("0.5") if index % 3 == 0 else planned_volume
            log = create_work_log(
                user=manager,
                cleaned_data={
                    "site_name": contract_payload["site"]["site_name"],
                    "contract": contract_payload["instance"],
                    "work_type": contract_payload["recipe"].name,
                    "planned_volume": planned_volume,
                    "actual_volume": actual_volume,
                    "volume_unit": contract_payload["recipe"].unit,
                    "plan_date": self.today - timedelta(days=25 - index),
                    "actual_date": self.today - timedelta(days=24 - index),
                    "status": statuses[index % len(statuses)],
                    "notes": f"Журнал работ для демонстрационного сценария. {self.demo_tag}",
                },
                ip_address="10.60.0.10",
            )
            logs.append(log)
        return logs

    def _create_writeoffs(self, count: int, refs: dict[str, Any], contracts: list[dict[str, Any]]) -> list[WriteOffAct]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVED,
                DocumentStatus.DRAFT,
                DocumentStatus.REWORK,
            ],
        )
        acts: list[WriteOffAct] = []
        for index in range(count):
            contract_payload = contracts[index % len(contracts)]
            manager = contract_payload["site"]["manager"]
            act = create_writeoff(
                user=manager,
                cleaned_data={
                    "act_date": self.today - timedelta(days=18 - index),
                    "contract": contract_payload["instance"],
                    "site_name": contract_payload["site"]["site_name"],
                    "work_type": contract_payload["recipe"].name,
                    "work_volume": Decimal(str(1 + (index % 3))),
                    "volume_unit": contract_payload["recipe"].unit,
                    "status": DocumentStatus.DRAFT,
                    "notes": f"Списание материалов по нормативу. {self.demo_tag}",
                },
                ip_address="10.70.0.10",
            )
            self._transition_default_entity(
                entity_type="write_off",
                instance=act,
                target_status=statuses[index],
                creator=manager,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            acts.append(act)
        return acts

    def _create_ppe_issuances(self, count: int, refs: dict[str, Any]) -> list[PPEIssuance]:
        statuses = self._status_cycle(
            count,
            [
                DocumentStatus.ACCEPTED,
                DocumentStatus.APPROVAL,
                DocumentStatus.DRAFT,
            ],
        )
        ppe_materials = [material for material in refs["materials"].values() if material.is_ppe]
        issuances: list[PPEIssuance] = []
        for index in range(count):
            site = refs["sites"][index % len(refs["sites"])]
            manager = site["manager"]
            workers = refs["workers_by_site"][site["site_name"]]
            ppe_lines = []
            for offset in range(2):
                worker = workers[(index + offset) % len(workers)]
                material = ppe_materials[(index + offset) % len(ppe_materials)]
                ppe_lines.append(
                    {
                        "worker": worker,
                        "material": material,
                        "quantity": Decimal("1"),
                        "service_life_months": 6 if (index + offset) % 2 == 0 else 12,
                    }
                )
            issuance = create_ppe_issuance(
                user=manager,
                cleaned_data={
                    "issue_date": self.today - timedelta(days=90 - index * 7),
                    "site_name": site["site_name"],
                    "season": "Лето" if index % 2 == 0 else "Зима",
                    "status": DocumentStatus.DRAFT,
                    "notes": f"Выдача СИЗ для демонстрационного сценария. {self.demo_tag}",
                    "items": self._format_ppe_items(ppe_lines),
                },
                ip_address="10.80.0.10",
            )
            self._transition_default_entity(
                entity_type="ppe_issuance",
                instance=issuance,
                target_status=statuses[index],
                creator=manager,
                approver=refs["users"][RoleChoices.DIRECTOR],
                accounting=refs["users"][RoleChoices.ACCOUNTING],
            )
            issuances.append(issuance)
        return issuances

    def _recipe_item_lines(
        self,
        recipe: WorkTypeRecipe,
        *,
        base_multiplier: Decimal,
        extra_items: int = 0,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        materials_map = self._materials_by_suffix()
        for material_suffix, norm_value in recipe.norms:
            material = materials_map[material_suffix]
            quantity = (Decimal(norm_value) * base_multiplier).quantize(Decimal("0.001"))
            items.append(
                {
                    "material": material,
                    "quantity": quantity,
                    "unit_price": material.price,
                    "notes": f"Позиция по рецепту работ. {self.demo_tag}",
                }
            )
        regular_materials = [material for material in materials_map.values() if not material.is_ppe]
        for offset in range(extra_items):
            material = regular_materials[(self.rng.randint(0, len(regular_materials) - 1) + offset) % len(regular_materials)]
            items.append(
                {
                    "material": material,
                    "quantity": Decimal(str(3 + offset)).quantize(Decimal("0.001")),
                    "unit_price": material.price,
                    "notes": f"Сопутствующая позиция. {self.demo_tag}",
                }
            )
        return items

    def _stock_replenishment_lines(
        self,
        materials: list[Material],
        *,
        start: int,
        count: int,
        base_quantity: Decimal,
    ) -> list[dict[str, Any]]:
        lines: list[dict[str, Any]] = []
        for index in range(count):
            material = materials[(start + index) % len(materials)]
            quantity = (base_quantity + Decimal(str(index * 12))).quantize(Decimal("0.001"))
            lines.append(
                {
                    "material": material,
                    "quantity": quantity,
                    "unit_price": material.price,
                    "notes": f"Плановое пополнение склада. {self.demo_tag}",
                }
            )
        return lines

    def _materials_by_suffix(self) -> dict[str, Material]:
        return {seed.code_suffix: Material.objects.get(code=f"{self.prefix_upper}-{seed.code_suffix}") for seed in MATERIAL_SEEDS}

    def _format_line_items(self, items: list[dict[str, Any]]) -> str:
        rows = []
        for item in items:
            material = item["material"]
            rows.append(
                f"{material.code}|{self._decimal_to_str(item['quantity'])}|{self._decimal_to_str(item['unit_price'])}|{item['notes']}"
            )
        return "\n".join(rows)

    def _format_ppe_items(self, items: list[dict[str, Any]]) -> str:
        rows = []
        for item in items:
            rows.append(
                f"{item['worker'].employee_number}|{item['material'].code}|{self._decimal_to_str(item['quantity'])}|{item['service_life_months']}"
            )
        return "\n".join(rows)

    def _decimal_to_str(self, value: Decimal | str | int | float) -> str:
        if isinstance(value, Decimal):
            normalized = value.quantize(Decimal("0.001")).normalize()
            return format(normalized, "f")
        return str(value)

    def _status_cycle(self, count: int, pattern: list[str]) -> list[str]:
        return [pattern[index % len(pattern)] for index in range(count)]

    def _transition_default_entity(
        self,
        *,
        entity_type: str,
        instance,
        target_status: str,
        creator,
        approver,
        accounting,
    ) -> None:
        if target_status == DocumentStatus.DRAFT:
            return

        record = DocumentRecord.objects.get(entity_type=entity_type, entity_id=instance.id)
        if record.status == DocumentStatus.DRAFT:
            record = transition_document(user=creator, record=record, new_status=DocumentStatus.APPROVAL, ip_address="10.0.0.1")
        if target_status == DocumentStatus.APPROVAL:
            return
        if target_status == DocumentStatus.REWORK:
            transition_document(user=approver, record=record, new_status=DocumentStatus.REWORK, ip_address="10.0.0.2")
            return

        record = transition_document(user=approver, record=record, new_status=DocumentStatus.APPROVED, ip_address="10.0.0.3")
        if target_status == DocumentStatus.APPROVED:
            return

        record = transition_document(user=approver, record=record, new_status=DocumentStatus.SENT_ACCOUNTING, ip_address="10.0.0.4")
        if target_status == DocumentStatus.SENT_ACCOUNTING:
            return

        transition_document(user=accounting, record=record, new_status=DocumentStatus.ACCEPTED, ip_address="10.0.0.5")

    def _transition_supplier_document(
        self,
        *,
        instance: SupplierDocument,
        target_status: str,
        supplier_user,
        reviewer,
        approver,
        accounting,
    ) -> None:
        if target_status == DocumentStatus.UPLOADED:
            return

        record = DocumentRecord.objects.get(entity_type="supplier_document", entity_id=instance.id)
        if target_status == DocumentStatus.REWORK:
            transition_document(user=reviewer, record=record, new_status=DocumentStatus.REWORK, ip_address="10.0.1.1")
            return

        record = transition_document(user=supplier_user, record=record, new_status=DocumentStatus.SUPPLY_CONFIRMED, ip_address="10.0.1.2")
        if target_status == DocumentStatus.SUPPLY_CONFIRMED:
            return

        record = transition_document(user=reviewer, record=record, new_status=DocumentStatus.APPROVAL, ip_address="10.0.1.3")
        if target_status == DocumentStatus.APPROVAL:
            return

        record = transition_document(user=approver, record=record, new_status=DocumentStatus.APPROVED, ip_address="10.0.1.4")
        if target_status == DocumentStatus.APPROVED:
            return

        record = transition_document(user=approver, record=record, new_status=DocumentStatus.SENT_ACCOUNTING, ip_address="10.0.1.5")
        if target_status == DocumentStatus.SENT_ACCOUNTING:
            return

        transition_document(user=accounting, record=record, new_status=DocumentStatus.ACCEPTED, ip_address="10.0.1.6")

    def _print_summary(self, *, refs: dict[str, Any], counts: dict[str, int], created: dict[str, int]) -> None:
        demo_document_records = DocumentRecord.objects.filter(
            Q(search_text__icontains=self.demo_tag)
            | Q(doc_number__startswith=f"{self.prefix_upper}-SMR-")
            | Q(doc_number__startswith=f"{self.prefix_upper}-SUP-")
        ).count()
        demo_audit_logs = AuditLog.objects.filter(user__username__startswith=f"{self.prefix_lower}_").count()

        self.stdout.write(self.style.SUCCESS(f"Демонстрационный набор {self.prefix_upper} успешно создан."))
        self.stdout.write("")
        self.stdout.write("Верхнеуровневые записи:")
        self.stdout.write(f"  - Договоры СМР: {created['contracts']}")
        self.stdout.write(f"  - Договоры поставки: {created['supply_contracts']}")
        self.stdout.write(f"  - Заявки поставщику: {created['requests']}")
        self.stdout.write(f"  - Документы поставщиков: {created['supplier_documents']}")
        self.stdout.write(f"  - Первичные документы: {created['primary_documents']}")
        self.stdout.write(f"  - Приходы на склад: {created['receipts']}")
        self.stdout.write(f"  - Отпуски материалов: {created['issues']}")
        self.stdout.write(f"  - Журналы работ: {created['work_logs']}")
        self.stdout.write(f"  - Акты списания: {created['writeoffs']}")
        self.stdout.write(f"  - Выдачи СИЗ: {created['ppe_issuances']}")
        self.stdout.write(f"  - Итого: {sum(created.values())}")
        self.stdout.write("")
        self.stdout.write("Справочники и сервисные записи:")
        self.stdout.write(f"  - Поставщики: {len(refs['suppliers'])}")
        self.stdout.write(f"  - Пользователи: {5 + len(refs['sites']) + len(refs['users']['supplier_users'])}")
        self.stdout.write(f"  - Материалы: {len(refs['materials'])}")
        self.stdout.write(f"  - Работники: {sum(len(items) for items in refs['workers_by_site'].values())}")
        self.stdout.write(f"  - Архивные записи: {demo_document_records}")
        self.stdout.write(f"  - Записи журнала действий: {demo_audit_logs}")
        self.stdout.write("")
        self.stdout.write("Демо-доступ:")
        self.stdout.write(f"  - Общий пароль: {self.password}")
        self.stdout.write(f"  - Администратор: {self.prefix_lower}_admin")
        self.stdout.write(f"  - Руководитель: {self.prefix_lower}_director")
        self.stdout.write(f"  - Снабжение: {self.prefix_lower}_procurement")
        self.stdout.write(f"  - Склад: {self.prefix_lower}_warehouse")
        self.stdout.write(f"  - Бухгалтерия: {self.prefix_lower}_accounting")
        for site in refs["sites"]:
            self.stdout.write(f"  - Начальник участка {site['seed'].site_name}: {self.prefix_lower}_site_{site['seed'].code}")
        for index, _supplier in enumerate(refs["suppliers"], start=1):
            self.stdout.write(f"  - Поставщик #{index}: {self.prefix_lower}_supplier_{index:02d}")
