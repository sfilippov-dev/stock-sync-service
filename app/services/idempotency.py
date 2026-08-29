"""Повторные запросы.

Площадка при таймауте повторяет вебхук. Без ключа идемпотентности остаток
уезжает дважды, и расхождение находят через неделю по жалобе.

Хранится не только факт обработки, но и сам ответ: повтор обязан вернуть
ровно то же, что и первый запрос. Иначе клиент, получивший на повтор другой
ответ, решит, что состояние изменилось, и начнёт разбираться.

Отдельно проверяется отпечаток тела. Один и тот же ключ с другим телом это
ошибка на стороне клиента, и молча вернуть ему старый ответ значит спрятать
эту ошибку.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import IdempotencyKey, utcnow


class KeyReused(Exception):
    """Тот же ключ, другое тело запроса."""


@dataclass(frozen=True, slots=True)
class StoredResponse:
    status: int
    body: dict


def fingerprint(payload: object) -> str:
    text = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:64]


async def lookup(session: AsyncSession, key: str, payload: object) -> StoredResponse | None:
    record = await session.get(IdempotencyKey, key)
    if record is None:
        return None
    if record.request_fingerprint != fingerprint(payload):
        raise KeyReused(
            f"ключ {key} уже использован с другим телом запроса"
        )
    return StoredResponse(record.response_status, json.loads(record.response_body))


async def remember(session: AsyncSession, key: str, payload: object,
                   status: int, body: dict) -> None:
    session.add(IdempotencyKey(
        key=key,
        request_fingerprint=fingerprint(payload),
        response_status=status,
        response_body=json.dumps(body, ensure_ascii=False, default=str),
    ))
    await session.flush()


async def purge_expired(session: AsyncSession, ttl_hours: int) -> int:
    """Ключи живут ограниченное время: иначе таблица растёт вечно.

    Срок берётся с запасом относительно того, сколько площадка повторяет
    доставку. Сутки покрывают все известные мне случаи.
    """
    cutoff = utcnow() - timedelta(hours=ttl_hours)
    result = await session.execute(
        delete(IdempotencyKey).where(IdempotencyKey.created_at < cutoff)
    )
    await session.flush()
    return result.rowcount or 0


async def count(session: AsyncSession) -> int:
    from sqlalchemy import func
    return (await session.execute(select(func.count()).select_from(IdempotencyKey))).scalar_one()
