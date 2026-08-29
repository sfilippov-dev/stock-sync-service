"""Таблицы.

Три идеи, вокруг которых собрана схема.

1. Остаток это состояние, и у него ровно одна строка на пару «товар и склад».
2. Любое изменение состояния пишется в журнал вместе с прежним значением.
   Тогда откат это одна команда, а не восстановление из резервной копии.
3. Событие для внешних систем кладётся в ту же транзакцию, что и само
   изменение. Иначе возможна пара «остаток изменён, но никто не узнал»
   или «все узнали об изменении, которого не было».
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD = "dead"


class Warehouse(Base):
    __tablename__ = "warehouses"

    id: Mapped[int] = mapped_column(primary_key=True)
    code: Mapped[str] = mapped_column(String(32), unique=True)
    name: Mapped[str] = mapped_column(String(128))


class Stock(Base):
    """Текущий остаток. Одна строка на пару «товар и склад»."""

    __tablename__ = "stock"
    __table_args__ = (
        UniqueConstraint("sku", "warehouse_id", name="uq_stock_sku_warehouse"),
        CheckConstraint("quantity >= 0", name="ck_stock_quantity_non_negative"),
        Index("ix_stock_updated_at", "updated_at"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    sku: Mapped[str] = mapped_column(String(64))
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    quantity: Mapped[int] = mapped_column(Integer)
    version: Mapped[int] = mapped_column(Integer, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow
    )


class ChangeLog(Base):
    """Журнал изменений. Хранит прежнее значение, поэтому откат тривиален."""

    __tablename__ = "change_log"
    __table_args__ = (
        Index("ix_change_log_stock_id", "stock_id", "id"),
        Index("ix_change_log_batch", "batch_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    stock_id: Mapped[int] = mapped_column(ForeignKey("stock.id", ondelete="CASCADE"))
    previous_quantity: Mapped[int | None] = mapped_column(Integer)
    new_quantity: Mapped[int] = mapped_column(Integer)
    reason: Mapped[str] = mapped_column(String(64))
    actor: Mapped[str] = mapped_column(String(64), default="api")
    #: одна партия правок откатывается целиком
    batch_id: Mapped[str] = mapped_column(String(64))
    reverted_by: Mapped[int | None] = mapped_column(BigInteger, default=None)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class OutboxEvent(Base):
    """Событие для внешних систем.

    Пишется в той же транзакции, что и изменение остатка. Отправляется
    отдельным воркером: сеть не должна держать транзакцию базы открытой.
    """

    __tablename__ = "outbox"
    __table_args__ = (
        Index("ix_outbox_pending", "status", "next_attempt_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    target: Mapped[str] = mapped_column(String(64))
    payload: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default=DeliveryStatus.PENDING)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, default=None)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    delivered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), default=None
    )


class IdempotencyKey(Base):
    """Ключ повторного запроса.

    Площадка при таймауте повторяет вебхук, и без этой таблицы остаток
    уезжает дважды. Хранится не только факт обработки, но и ответ: повтор
    обязан вернуть то же самое, что и первый запрос, иначе клиент решит,
    что состояние изменилось.
    """

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    request_fingerprint: Mapped[str] = mapped_column(String(64))
    response_status: Mapped[int] = mapped_column(Integer)
    response_body: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class InboxEvent(Base):
    """Отметка о принятом сообщении на стороне приёмника.

    RabbitMQ обещает доставку не реже одного раза. Приёмник, упавший
    между обработкой и подтверждением, получит то же сообщение снова, и
    это штатный ход событий, а не сбой. Уникальный ключ по message_id
    превращает повтор в пустую операцию: вставка не проходит, значит
    сообщение уже учтено, и остаётся только подтвердить его брокеру.

    Пара к таблице outbox: та отвечает за то, что событие не потеряется
    на отправке, эта за то, что оно не применится дважды на приёме.
    """

    __tablename__ = "inbox"
    __table_args__ = (
        Index("ix_inbox_received_at", "received_at"),
    )

    message_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    target: Mapped[str] = mapped_column(String(64))
    payload: Mapped[str] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
