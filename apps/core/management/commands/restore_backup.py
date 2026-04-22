from __future__ import annotations

from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from ...services import restore_backup_file


class Command(BaseCommand):
    help = "Восстанавливает данные АИС из JSON backup-файла."

    def add_arguments(self, parser) -> None:
        parser.add_argument("backup_path", help="Путь к JSON-файлу резервной копии")

    def handle(self, *args, **options):
        backup_path = Path(options["backup_path"]).expanduser().resolve()
        if not backup_path.exists():
            raise CommandError(f"Файл не найден: {backup_path}")

        try:
            restored_counts = restore_backup_file(backup_path)
        except Exception as exc:
            raise CommandError(str(exc)) from exc

        self.stdout.write(self.style.SUCCESS(f"Восстановление завершено из {backup_path.name}"))
        self.stdout.write(f"Всего восстановлено записей: {sum(restored_counts.values())}")
