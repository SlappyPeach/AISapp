from django.apps import AppConfig


class CoreConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "apps.core"
    verbose_name = "АИС: ядро"

    def ready(self) -> None:
        from . import signals  # noqa: F401
