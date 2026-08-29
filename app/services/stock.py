"""Изменение остатков.

Три свойства, ради которых сервис вообще существует.

Атомарность. Изменение остатка, запись в журнал и событие для внешних
систем происходят в одной транзакции. Иначе бывает «остаток изменён, но
никто не узнал» и «все узнали об изменении, которого не было». Второе
хуже: расхождение находят через неделю по жалобе.

Обратимость. В журнал пишется прежнее значение, поэтому откат это одна
команда, а не восстановление из резервной копии.

Отсутствие холостых записей. Если новое значение совпадает с текущим,
не пишется ни строка журнала, ни событие. Площадки шлют одно и то же
состояние по расписанию, и без этой проверки журнал за неделю
разрастается до миллионов строк, в которых ничего не менялось.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ChangeLog, DeliveryStatus, OutboxEvent, Stock, Warehouse

TARGETS = ("wb", "ozon")


class UnknownWarehouse(Exception):
    """Склад не заведён. Создавать его на лету нельзя: опечатка в коде
    склада превратилась бы в новый склад с нулевым остатком."""


@dataclass(frozen=True, slots=True)
class ApplyResult:
    batch_id: str
    applied: int
    unchanged: int
    events: int
    rows: list[tuple[Stock, str]]


async def _warehouse_map(session: AsyncSession, codes: set[str]) -> dict[str, Warehouse]:
    found = (await session.execute(
        select(Warehouse).where(Warehouse.code.in_(codes))
    )).scalars().all()
    mapping = {warehouse.code: warehouse for warehouse in found}
    missing = codes - set(mapping)
    if missing:
        raise UnknownWarehouse(f"неизвестные склады: {', '.join(sorted(missing))}")
    return mapping


async def apply_updates(session: AsyncSession, updates: list, actor: str = "api") -> ApplyResult:
    """Применить партию правок в одной транзакции."""
    batch_id = uuid.uuid4().hex
    warehouses = await _warehouse_map(session, {update.warehouse for update in updates})

    applied = unchanged = events = 0
    rows: list[tuple[Stock, str]] = []

    for update in updates:
        warehouse = warehouses[update.warehouse]
        stock = (await session.execute(
            select(Stock)
            .where(Stock.sku == update.sku, Stock.warehouse_id == warehouse.id)
            .with_for_update()      # две параллельные правки одной строки не должны гоняться
        )).scalar_one_or_none()

        previous = stock.quantity if stock else None
        if stock is not None and stock.quantity == update.quantity:
            unchanged += 1
            rows.append((stock, warehouse.code))
            continue

        if stock is None:
            stock = Stock(sku=update.sku, warehouse_id=warehouse.id,
                          quantity=update.quantity, version=1)
            session.add(stock)
            await session.flush()
        else:
            stock.quantity = update.quantity
            stock.version += 1
            await session.flush()

        session.add(ChangeLog(
            stock_id=stock.id,
            previous_quantity=previous,
            new_quantity=update.quantity,
            reason=update.reason,
            actor=actor,
            batch_id=batch_id,
        ))
        for target in TARGETS:
            session.add(OutboxEvent(
                target=target,
                payload=json.dumps({
                    "sku": update.sku,
                    "warehouse": warehouse.code,
                    "quantity": update.quantity,
                    "batch_id": batch_id,
                }, ensure_ascii=False),
                status=DeliveryStatus.PENDING,
            ))
            events += 1
        applied += 1
        rows.append((stock, warehouse.code))

    await session.flush()
    return ApplyResult(batch_id, applied, unchanged, events, rows)


async def revert_batch(
    session: AsyncSession, batch_id: str, actor: str = "revert"
) -> tuple[int, int]:
    """Откатить партию: вернуть каждой строке прежнее значение.

    Правки применяются в обратном порядке. Если в партии одна строка
    менялась дважды, откат в прямом порядке вернул бы промежуточное
    значение вместо исходного.
    """
    changes = (await session.execute(
        select(ChangeLog)
        .where(ChangeLog.batch_id == batch_id, ChangeLog.reverted_by.is_(None))
        .order_by(ChangeLog.id.desc())
    )).scalars().all()

    reverted = skipped = 0
    revert_batch_id = f"revert-{batch_id[:12]}-{uuid.uuid4().hex[:8]}"

    for change in changes:
        if change.previous_quantity is None:
            # Строки до правки не существовало. Удалять её опасно: на неё
            # уже могли сослаться. Обнуляем остаток и говорим об этом.
            target_quantity = 0
        else:
            target_quantity = change.previous_quantity

        stock = await session.get(Stock, change.stock_id, with_for_update=True)
        if stock is None:
            skipped += 1
            continue

        entry = ChangeLog(
            stock_id=stock.id,
            previous_quantity=stock.quantity,
            new_quantity=target_quantity,
            reason=f"откат партии {batch_id[:12]}",
            actor=actor,
            batch_id=revert_batch_id,
        )
        session.add(entry)
        await session.flush()

        stock.quantity = target_quantity
        stock.version += 1
        change.reverted_by = entry.id
        reverted += 1

        for target in TARGETS:
            session.add(OutboxEvent(
                target=target,
                payload=json.dumps({
                    "sku": stock.sku,
                    "quantity": target_quantity,
                    "batch_id": revert_batch_id,
                    "reverts": batch_id,
                }, ensure_ascii=False),
                status=DeliveryStatus.PENDING,
            ))

    await session.flush()
    return reverted, skipped
