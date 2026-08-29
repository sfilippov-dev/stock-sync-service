"""Приёмник событий из RabbitMQ.

Отдельный процесс и отдельная роль. Отправщик доводит событие до брокера,
приёмник забирает его оттуда и применяет у себя. Между ними нет общей
транзакции, и поэтому у приёмника своя защита от повтора.

Порядок обработки одного сообщения.

1. Вставить отметку в таблицу inbox. Уникальный ключ по message_id: если
   вставка не прошла, сообщение уже обработано и остаётся подтвердить его.
2. Выполнить полезную работу в той же транзакции, что и отметка. Иначе
   возможен разрыв, ради устранения которого таблица и заводилась.
3. Подтвердить сообщение брокеру. Только после фиксации транзакции.

Сообщение, которое не удалось обработать, отклоняется без возврата в
очередь: requeue крутит приёмник на полной скорости по одному и тому же
телу. Отклонённое уходит в мёртвую очередь через x-dead-letter-exchange,
где его видит человек.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

import aio_pika
from sqlalchemy.exc import IntegrityError

from app.config import get_settings
from app.db import SessionMaker
from app.models import InboxEvent
from app.services.rabbit import declare_consumer_topology

log = logging.getLogger("stocksync.consumer")


async def handle(message: aio_pika.abc.AbstractIncomingMessage, target: str) -> None:
    """Обработка одного сообщения с защитой от повторной доставки."""
    message_id = message.message_id
    if not message_id:
        # Без идентификатора повтор не отличить от нового события.
        # Такое сообщение отправлено не нашим издателем: в мёртвую очередь.
        log.error("сообщение без message_id отклонено")
        await message.reject(requeue=False)
        return

    body = message.body.decode("utf-8")
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as error:
        log.error("сообщение %s не разбирается: %s", message_id, error)
        await message.reject(requeue=False)
        return

    async with SessionMaker() as session:
        session.add(InboxEvent(message_id=message_id, target=target, payload=body))
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            log.info("сообщение %s уже обработано, повтор пропущен", message_id)
            await message.ack()
            return

    log.info("принято %s: sku=%s склад=%s остаток=%s", message_id,
             payload.get("sku"), payload.get("warehouse"), payload.get("quantity"))
    await message.ack()


async def run(target: str, limit: int | None = None) -> int:
    """Читать очередь до отмены. limit нужен тестам и разовым прогонам."""
    settings = get_settings()
    processed = 0
    connection = await aio_pika.connect_robust(settings.rabbit_url)
    try:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=settings.rabbit_prefetch)
        queue = await declare_consumer_topology(channel, target, settings)
        log.info("приёмник слушает очередь %s", queue.name)
        async with queue.iterator() as messages:
            async for message in messages:
                await handle(message, target)
                processed += 1
                if limit is not None and processed >= limit:
                    break
    finally:
        await connection.close()
    return processed


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="Приёмник событий из RabbitMQ")
    parser.add_argument("--target", default="wb", help="Цель, чью очередь читать.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Выйти после стольких сообщений.")
    args = parser.parse_args()
    asyncio.run(run(args.target, args.limit))


if __name__ == "__main__":
    main()
