"""Формы запросов и ответов."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class StockUpdate(BaseModel):
    """Новое значение остатка."""

    sku: str = Field(min_length=1, max_length=64, examples=["ART-0042"])
    warehouse: str = Field(min_length=1, max_length=32, examples=["spb-1"])
    quantity: int = Field(ge=0, examples=[120])
    reason: str = Field(default="manual", max_length=64, examples=["поставка"])


class StockBatch(BaseModel):
    """Партия правок. Откатывается целиком, по одному batch_id."""

    updates: list[StockUpdate] = Field(min_length=1, max_length=500)
    actor: str = Field(default="api", max_length=64)


class StockOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str
    warehouse: str
    quantity: int
    version: int
    updated_at: datetime


class BatchResult(BaseModel):
    batch_id: str
    applied: int
    unchanged: int
    events_queued: int
    items: list[StockOut]


class RevertResult(BaseModel):
    batch_id: str
    reverted: int
    skipped: int
    detail: str


class ChangeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    sku: str
    warehouse: str
    previous_quantity: int | None
    new_quantity: int
    reason: str
    actor: str
    batch_id: str
    reverted_by: int | None
    created_at: datetime


class OutboxSummary(BaseModel):
    pending: int
    delivered: int
    failed: int
    dead: int
    oldest_pending_age_seconds: float | None


class Health(BaseModel):
    status: str
    database: str
    pending_events: int | None = None
    dead_events: int | None = None
    version: str
