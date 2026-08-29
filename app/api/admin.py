"""Служебные ручки: здоровье и очередь."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import get_session
from app.schemas import Health, OutboxSummary
from app.services import idempotency, outbox

router = APIRouter(tags=["service"])


@router.get("/health", response_model=Health, summary="Проверка живости")
async def health(session: AsyncSession = Depends(get_session)) -> Health:
    """Здоровье проверяется запросом в базу, а не строкой «ok».

    Сервис, который отвечает «ok» не касаясь базы, продолжает делать это и
    после того, как база отвалилась. Балансировщик такой узел не выключит,
    и запросы будут идти в него до тех пор, пока кто-то не посмотрит логи.
    """
    try:
        await session.execute(text("SELECT 1"))
    except Exception as error:                          # noqa: BLE001
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            f"база недоступна: {type(error).__name__}",
        ) from error

    counts = await outbox.summary(session)
    return Health(
        status="ok",
        database="ok",
        pending_events=counts["pending"] + counts["failed"],
        dead_events=counts["dead"],
        version=get_settings().app_name,
    )


@router.get("/v1/outbox", response_model=OutboxSummary, summary="Состояние очереди")
async def outbox_state(session: AsyncSession = Depends(get_session)) -> OutboxSummary:
    return OutboxSummary(**await outbox.summary(session))


@router.post("/v1/outbox/revive", summary="Вернуть мёртвые события в очередь")
async def revive(limit: int = 100, session: AsyncSession = Depends(get_session)) -> dict:
    return {"revived": await outbox.revive_dead(session, limit)}


@router.post("/v1/maintenance/purge-idempotency", summary="Убрать просроченные ключи")
async def purge(session: AsyncSession = Depends(get_session)) -> dict:
    removed = await idempotency.purge_expired(session, get_settings().idempotency_ttl_hours)
    return {"removed": removed, "left": await idempotency.count(session)}
