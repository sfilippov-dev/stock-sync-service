"""Миграции обязаны знать обо всех таблицах моделей.

Самая частая ошибка в проектах с alembic: модель добавили, миграцию
сгенерировать забыли. На своей машине всё работает, потому что схема
создавалась из моделей, а на сервере таблицы нет.

Тест статический и дешёвый: он читает файлы миграций и сверяет список
созданных таблиц со списком в метаданных.
"""

import re
from pathlib import Path

from app.models import Base

VERSIONS = Path(__file__).resolve().parents[1] / "alembic" / "versions"


def tables_in_migrations() -> set[str]:
    created: set[str] = set()
    for path in VERSIONS.glob("*.py"):
        created |= set(re.findall(r"op\.create_table\(\s*[\"']([^\"']+)[\"']",
                                  path.read_text(encoding="utf-8")))
    return created


def test_every_model_table_has_a_migration():
    missing = set(Base.metadata.tables) - tables_in_migrations()
    assert not missing, f"нет миграции для таблиц: {', '.join(sorted(missing))}"


def test_no_migration_creates_a_table_that_no_model_describes():
    extra = tables_in_migrations() - set(Base.metadata.tables)
    assert not extra, f"миграция создаёт таблицы без моделей: {', '.join(sorted(extra))}"


def test_every_migration_can_be_rolled_back():
    """Миграция без downgrade это дорога в один конец: неудачный выкат
    придётся откатывать восстановлением базы, а не одной командой."""
    for path in VERSIONS.glob("*.py"):
        text = path.read_text(encoding="utf-8")
        body = text.split("def downgrade()", 1)[1]
        assert "op." in body, f"{path.name}: downgrade ничего не делает"
