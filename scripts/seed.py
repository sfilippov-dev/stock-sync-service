"""Завести склады и показать сервис в работе.

Склады не создаются на лету при первой правке намеренно: опечатка в коде
склада превратилась бы в новый склад с нулевым остатком, и найти это потом
крайне тяжело. Поэтому справочник заполняется отдельно и осознанно.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select

from app.db import SessionMaker
from app.models import Warehouse

WAREHOUSES = [
    ("spb-1", "Санкт-Петербург, Шушары"),
    ("msk-1", "Москва, Коледино"),
    ("kzn-1", "Казань"),
    ("ekb-1", "Екатеринбург"),
]


async def main() -> None:
    async with SessionMaker() as session:
        existing = set((await session.execute(select(Warehouse.code))).scalars().all())
        added = 0
        for code, name in WAREHOUSES:
            if code in existing:
                continue
            session.add(Warehouse(code=code, name=name))
            added += 1
        await session.commit()
    print(f"складов добавлено: {added}, всего: {len(set(existing) | {c for c, _ in WAREHOUSES})}")


if __name__ == "__main__":
    asyncio.run(main())
