"""Связка «очередь в базе → RabbitMQ → приёмник».

Тесты идут против настоящего брокера, а не против заглушки. Заглушка
проверила бы, что код вызывает publish, и промолчала бы ровно о том, ради
чего брокер и добавлен: о подтверждениях, о живучести сообщения при
перезапуске и о повторной доставке. Если брокера нет, модуль пропускается
целиком, чтобы `pytest` оставался запускаемым на голой машине.
"""

from __future__ import annotations

import asyncio
import json
import uuid

import pytest

aio_pika = pytest.importorskip("aio_pika")

from sqlalchemy import select  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.consumer import handle  # noqa: E402
from app.consumer import run as consume  # noqa: E402
from app.models import DeliveryStatus, InboxEvent, OutboxEvent  # noqa: E402
from app.services.outbox import dispatch_once  # noqa: E402
from app.services.rabbit import (  # noqa: E402
    DEAD_EXCHANGE_SUFFIX,
    RabbitPublisher,
    declare_consumer_topology,
    queue_name,
)
from app.worker import rabbit_sender  # noqa: E402


async def _broker_available() -> bool:
    try:
        connection = await asyncio.wait_for(
            aio_pika.connect_robust(get_settings().rabbit_url), timeout=3)
    except Exception:  # noqa: BLE001
        return False
    await connection.close()
    return True


@pytest.fixture(scope="module", autouse=True)
def require_broker():
    if not asyncio.run(_broker_available()):
        pytest.skip("RabbitMQ недоступен по STOCKSYNC_RABBIT_URL")


@pytest.fixture(autouse=True)
async def app_engine_per_loop():
    """Закрыть пул приложения после теста.

    Движок в app.db создаётся один раз на модуль, а событийный цикл у
    каждого теста свой. Соединение, открытое в прошлом цикле, во втором
    тесте падает с «attached to a different loop». Пул закрывается после
    каждого теста, и приёмник открывает соединение заново в своём цикле.
    """
    yield
    from app.db import engine
    await engine.dispose()


@pytest.fixture
def target():
    """Своя цель на каждый тест: очереди не пересекаются между прогонами."""
    return "t" + uuid.uuid4().hex[:10]


async def _drain(queue, limit=10, timeout=5.0):
    """Забрать сообщения из очереди, не подтверждая их логикой приёмника."""
    out = []
    async with queue.iterator(timeout=timeout) as messages:
        async for message in messages:
            async with message.process():
                out.append(message)
            if len(out) >= limit:
                break
    return out


@pytest.fixture
async def channel():
    connection = await aio_pika.connect_robust(get_settings().rabbit_url)
    channel = await connection.channel()
    yield channel
    await connection.close()


async def test_событие_из_очереди_доходит_до_брокера(session, target, channel):
    """Отправитель в брокер подставляется в ту же очередь без её правок."""
    queue = await declare_consumer_topology(channel, target)
    session.add(OutboxEvent(target=target, payload=json.dumps({"sku": "ART-1"})))
    await session.flush()

    async with RabbitPublisher() as publisher:
        result = await dispatch_once(session, rabbit_sender(publisher))
    await session.commit()

    assert result.delivered == 1
    messages = await _drain(queue, limit=1)
    assert len(messages) == 1
    assert json.loads(messages[0].body) == {"sku": "ART-1"}
    # ключ события уехал в message_id: по нему приёмник отсеет повтор
    assert messages[0].message_id.isdigit()
    assert messages[0].delivery_mode == 2          # persistent


async def test_сообщение_помечено_как_переживающее_перезапуск(session, target, channel):
    """Durable очередь и persistent сообщение нужны вместе, поодиночке они не спасают."""
    queue = await declare_consumer_topology(channel, target)
    assert queue.durable

    async with RabbitPublisher() as publisher:
        await publisher.publish(target, json.dumps({"sku": "ART-2"}), message_id="42")

    messages = await _drain(queue, limit=1)
    assert messages[0].delivery_mode == aio_pika.DeliveryMode.PERSISTENT


async def test_событие_без_подписчика_не_пропадает_молча(session, target):
    """Никто не привязан к ключу, значит publish обязан упасть, а не промолчать.

    Без флага mandatory обменник выбросил бы сообщение, а очередь в базе
    записала бы «доставлено». Это худший из возможных исходов: данные
    потеряны, а система считает, что всё в порядке.
    """
    session.add(OutboxEvent(target=target, payload=json.dumps({"sku": "ART-3"})))
    await session.flush()

    async with RabbitPublisher() as publisher:
        result = await dispatch_once(session, rabbit_sender(publisher))
    await session.commit()

    assert result.delivered == 0
    event = (await session.execute(select(OutboxEvent))).scalars().one()
    assert event.status == DeliveryStatus.FAILED
    assert event.attempts == 1
    assert event.last_error


async def test_приёмник_записывает_сообщение_в_inbox(session, target, channel):
    await declare_consumer_topology(channel, target)
    async with RabbitPublisher() as publisher:
        await publisher.publish(target, json.dumps({"sku": "ART-4", "quantity": 7}),
                                message_id="1001")

    processed = await consume(target, limit=1)
    assert processed == 1

    rows = (await session.execute(select(InboxEvent))).scalars().all()
    assert [row.message_id for row in rows] == ["1001"]


async def test_повторная_доставка_не_применяется_дважды(session, target, channel):
    """Брокер обещает доставку не реже одного раза, значит повтор это норма."""
    await declare_consumer_topology(channel, target)
    body = json.dumps({"sku": "ART-5", "quantity": 3})
    async with RabbitPublisher() as publisher:
        await publisher.publish(target, body, message_id="2002")
        await publisher.publish(target, body, message_id="2002")

    assert await consume(target, limit=2) == 2

    rows = (await session.execute(select(InboxEvent))).scalars().all()
    assert len(rows) == 1                      # вторая доставка отсеяна ключом


async def test_нечитаемое_сообщение_уходит_в_мёртвую_очередь(session, target, channel):
    """Отклонённое без возврата попадает в очередь мёртвых через x-dead-letter-exchange.

    Возврат в ту же очередь раскрутил бы приёмник на полной скорости по
    одному и тому же телу: сообщение, которое не разбирается сейчас, не
    станет разбираться через миллисекунду.
    """
    await declare_consumer_topology(channel, target)
    async with RabbitPublisher() as publisher:
        await publisher.publish(target, "не json", message_id="3003")

    assert await consume(target, limit=1) == 1

    settings = get_settings()
    dead = await channel.get_queue(queue_name(settings, target) + DEAD_EXCHANGE_SUFFIX)
    messages = await _drain(dead, limit=1)
    assert len(messages) == 1
    assert messages[0].message_id == "3003"
    assert (await session.execute(select(InboxEvent))).scalars().all() == []


async def test_сообщение_без_ключа_отклоняется(session, target):
    """Без message_id повтор не отличить от нового события, пропускать нельзя.

    Проверяется на самом обработчике, а не через брокер: aio-pika
    проставляет message_id сама, и сообщение без него приходит только от
    чужого издателя. Именно поэтому проверка в коде и нужна.
    """
    class Stub:
        message_id = None
        body = b'{"sku": "ART-6"}'

        def __init__(self):
            self.rejected = None

        async def reject(self, requeue):
            self.rejected = requeue

        async def ack(self):
            raise AssertionError("сообщение без ключа не должно подтверждаться")

    message = Stub()
    await handle(message, target)
    assert message.rejected is False
    assert (await session.execute(select(InboxEvent))).scalars().all() == []
