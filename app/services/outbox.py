"""Доставка событий во внешние системы.

Отправка вынесена из транзакции намеренно: держать транзакцию базы
открытой на время сетевого вызова значит блокировать строку остатка на
секунды, а при зависании приёмника — на минуты.

Событие живёт в трёх состояниях. Ждёт отправки, доставлено, или исчерпало
попытки и ушло в мёртвую очередь. Последнее состояние отдельное, а не
«просто не доставлено»: мёртвые события требуют человека, а не очередного
повтора, и смешивать их с обычными неудачами означает никогда их не
заметить.
"""

from __future__ import annotations

import logging
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import Settings, get_settings
from app.models import DeliveryStatus, OutboxEvent, utcnow

log = logging.getLogger("stocksync.outbox")

#: Отправитель: получает цель, тело и ключ события; бросает исключение при неудаче.
#: Ключ это идентификатор строки в outbox. Он передаётся отправителю, потому
#: что транспорт с доставкой «не реже одного раза» обязан дать приёмнику
#: возможность отличить повтор от нового события, а стабильнее этого числа
#: у события ничего нет.
Sender = Callable[[str, str, str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class DispatchResult:
    delivered: int
    retried: int
    dead: int


def next_delay(attempt: int, settings: Settings) -> float:
    """Пауза перед следующей попыткой: растёт экспоненциально, с разбросом.

    Разброс тот же, что и в клиенте: без него после падения приёмника все
    накопленные события уходят одной волной ровно в тот момент, когда он
    поднялся, и роняют его повторно.
    """
    ceiling = min(settings.delivery_max_delay,
                  settings.delivery_base_delay * 2 ** max(attempt - 1, 0))
    return random.uniform(ceiling / 2, ceiling)


async def dispatch_once(session: AsyncSession, sender: Sender,
                        settings: Settings | None = None) -> DispatchResult:
    """Один проход по очереди."""
    settings = settings or get_settings()
    now = utcnow()

    events = (await session.execute(
        select(OutboxEvent)
        .where(
            OutboxEvent.status.in_([DeliveryStatus.PENDING, DeliveryStatus.FAILED]),
            OutboxEvent.next_attempt_at <= now,
        )
        .order_by(OutboxEvent.id)
        .limit(settings.delivery_batch)
        .with_for_update(skip_locked=True)   # два воркера не берут одно событие
    )).scalars().all()

    delivered = retried = dead = 0
    for event in events:
        event.attempts += 1
        try:
            await sender(event.target, event.payload, str(event.id))
        except Exception as error:                     # noqa: BLE001
            event.last_error = f"{type(error).__name__}: {error}"[:1000]
            if event.attempts >= settings.delivery_attempts:
                event.status = DeliveryStatus.DEAD
                dead += 1
                log.error("событие %s ушло в мёртвую очередь: %s", event.id, event.last_error)
            else:
                event.status = DeliveryStatus.FAILED
                event.next_attempt_at = utcnow() + timedelta(
                    seconds=next_delay(event.attempts, settings)
                )
                retried += 1
        else:
            event.status = DeliveryStatus.DELIVERED
            event.delivered_at = utcnow()
            event.last_error = None
            delivered += 1

    await session.flush()
    return DispatchResult(delivered, retried, dead)


async def summary(session: AsyncSession) -> dict:
    counts = dict((await session.execute(
        select(OutboxEvent.status, func.count()).group_by(OutboxEvent.status)
    )).all())
    oldest = (await session.execute(
        select(func.min(OutboxEvent.created_at)).where(
            OutboxEvent.status.in_([DeliveryStatus.PENDING, DeliveryStatus.FAILED])
        )
    )).scalar_one_or_none()
    age = (utcnow() - oldest).total_seconds() if oldest else None
    return {
        "pending": counts.get(DeliveryStatus.PENDING, 0),
        "delivered": counts.get(DeliveryStatus.DELIVERED, 0),
        "failed": counts.get(DeliveryStatus.FAILED, 0),
        "dead": counts.get(DeliveryStatus.DEAD, 0),
        "oldest_pending_age_seconds": age,
    }


async def revive_dead(session: AsyncSession, limit: int = 100) -> int:
    """Вернуть мёртвые события в очередь.

    Нужна после того, как человек починил причину. Счётчик попыток
    обнуляется, иначе событие уйдёт в мёртвые с первой же неудачи.
    """
    events = (await session.execute(
        select(OutboxEvent).where(OutboxEvent.status == DeliveryStatus.DEAD)
        .order_by(OutboxEvent.id).limit(limit).with_for_update(skip_locked=True)
    )).scalars().all()
    for event in events:
        event.status = DeliveryStatus.PENDING
        event.attempts = 0
        event.next_attempt_at = utcnow()
        event.last_error = None
    await session.flush()
    return len(events)
