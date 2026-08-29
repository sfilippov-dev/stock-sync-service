"""Ручки остатков."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_session
from app.models import ChangeLog, Stock, Warehouse
from app.schemas import BatchResult, ChangeOut, RevertResult, StockBatch, StockOut
from app.services import idempotency
from app.services.stock import UnknownWarehouse, apply_updates, revert_batch

log = logging.getLogger("stocksync.api")
router = APIRouter(prefix="/v1", tags=["stock"])


def _to_out(stock: Stock, warehouse_code: str) -> StockOut:
    return StockOut(
        sku=stock.sku, warehouse=warehouse_code, quantity=stock.quantity,
        version=stock.version, updated_at=stock.updated_at,
    )


@router.post("/stock", response_model=BatchResult, status_code=status.HTTP_200_OK,
             summary="Записать остатки",
             description="Партия правок применяется в одной транзакции и откатывается "
                         "целиком по batch_id. Повторный запрос с тем же заголовком "
                         "Idempotency-Key возвращает прежний ответ и ничего не меняет.")
async def set_stock(
    batch: StockBatch,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> BatchResult:
    payload = batch.model_dump()

    if idempotency_key:
        try:
            stored = await idempotency.lookup(session, idempotency_key, payload)
        except idempotency.KeyReused as error:
            raise HTTPException(status.HTTP_409_CONFLICT, str(error)) from error
        if stored is not None:
            response.headers["Idempotent-Replay"] = "true"
            return BatchResult.model_validate(stored.body)

    try:
        result = await apply_updates(session, batch.updates, batch.actor)
    except UnknownWarehouse as error:
        # Код указан числом: имя константы в starlette переименовали,
        # и привязываться к нему значит ломаться на смене версии.
        raise HTTPException(422, str(error)) from error

    body = BatchResult(
        batch_id=result.batch_id,
        applied=result.applied,
        unchanged=result.unchanged,
        events_queued=result.events,
        items=[_to_out(stock, code) for stock, code in result.rows],
    )
    if idempotency_key:
        await idempotency.remember(session, idempotency_key, payload, 200,
                                   body.model_dump(mode="json"))
    log.info("партия %s: применено %s, без изменений %s",
             result.batch_id, result.applied, result.unchanged)
    return body


@router.get("/stock", response_model=list[StockOut], summary="Текущие остатки")
async def list_stock(
    sku: str | None = None,
    warehouse: str | None = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
) -> list[StockOut]:
    query = select(Stock, Warehouse.code).join(Warehouse, Warehouse.id == Stock.warehouse_id)
    if sku:
        query = query.where(Stock.sku == sku)
    if warehouse:
        query = query.where(Warehouse.code == warehouse)
    rows = (await session.execute(query.order_by(Stock.sku).limit(limit))).all()
    return [_to_out(stock, code) for stock, code in rows]


@router.get("/changes", response_model=list[ChangeOut], summary="Журнал изменений")
async def list_changes(
    batch_id: str | None = None,
    sku: str | None = None,
    limit: int = 50,
    session: AsyncSession = Depends(get_session),
) -> list[ChangeOut]:
    query = (
        select(ChangeLog, Stock.sku, Warehouse.code)
        .join(Stock, Stock.id == ChangeLog.stock_id)
        .join(Warehouse, Warehouse.id == Stock.warehouse_id)
    )
    if batch_id:
        query = query.where(ChangeLog.batch_id == batch_id)
    if sku:
        query = query.where(Stock.sku == sku)
    rows = (await session.execute(query.order_by(ChangeLog.id.desc()).limit(limit))).all()
    return [
        ChangeOut(
            id=change.id, sku=sku_value, warehouse=code,
            previous_quantity=change.previous_quantity, new_quantity=change.new_quantity,
            reason=change.reason, actor=change.actor, batch_id=change.batch_id,
            reverted_by=change.reverted_by, created_at=change.created_at,
        )
        for change, sku_value, code in rows
    ]


@router.post("/batches/{batch_id}/revert", response_model=RevertResult,
             summary="Откатить партию",
             description="Возвращает каждой строке значение, которое было до правки. "
                         "Правки откатываются в обратном порядке.")
async def revert(batch_id: str, session: AsyncSession = Depends(get_session)) -> RevertResult:
    reverted, skipped = await revert_batch(session, batch_id)
    if reverted == 0 and skipped == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            f"партия {batch_id} не найдена или уже откачена",
        )
    return RevertResult(
        batch_id=batch_id, reverted=reverted, skipped=skipped,
        detail=f"откачено строк: {reverted}, пропущено: {skipped}",
    )
