"""Отправка событий в RabbitMQ.

Зачем брокер, если очередь уже есть в базе. Очередь в базе решает задачу
согласованности: событие пишется в той же транзакции, что и остаток, и
потому не может разойтись с ним. Она не решает задачу доставки многим:
приёмников со временем становится больше одного, и каждый новый приёмник
это ещё один адрес в конфигурации отправщика и ещё один повод его
перевыкатить.

Поэтому роли разделены. База остаётся единственной точкой фиксации, брокер
берёт на себя транспорт и разветвление. Событие уходит в обменник по типу
topic, а кто на него подписан, отправителя не касается.

Три вещи, без которых такая связка врёт о доставке.

1. Подтверждения издателя. Без них publish возвращает управление сразу, и
   «доставлено» в базе означает лишь «отдано в сокет». Событие помечается
   доставленным только после того, как брокер подтвердил запись.
2. Persistent и durable. Сообщение переживает перезапуск брокера, только
   если durable очередь и persistent сообщение стоят одновременно.
3. Флаг mandatory. Обменник без подходящей привязки молча выбрасывает
   сообщение. С mandatory брокер возвращает его отправителю, publish
   падает, и событие уходит в повтор вместо тихой пропажи.
"""

from __future__ import annotations

import logging
from types import TracebackType

import aio_pika
import pamqp.commands
from aio_pika.abc import AbstractRobustChannel, AbstractRobustConnection

from app.config import Settings, get_settings

log = logging.getLogger("stocksync.rabbit")

#: очередь мёртвых сообщений на стороне брокера: сюда попадает то,
#: что приёмник отверг окончательно
DEAD_EXCHANGE_SUFFIX = ".dead"


class Unrouted(RuntimeError):
    """Брокер вернул сообщение: под ключ маршрутизации нет ни одной очереди."""


def routing_key(target: str) -> str:
    """Ключ маршрутизации события: stock.<цель>.

    Ключ из двух частей, а не одно слово, чтобы приёмник мог подписаться
    и на одну цель (stock.wb), и на все сразу (stock.*), не меняя издателя.
    """
    return f"stock.{target}"


def queue_name(settings: Settings, target: str) -> str:
    return f"{settings.rabbit_exchange}.{target}"


class RabbitPublisher:
    """Издатель с подтверждениями.

    Соединение robust: при обрыве aio-pika переподключается сам и заново
    объявляет топологию. Для отправщика, который работает сутками, это
    разница между «пережил перезагрузку брокера» и «висит до перезапуска».
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._connection: AbstractRobustConnection | None = None
        self._channel: AbstractRobustChannel | None = None
        self._exchange: aio_pika.abc.AbstractExchange | None = None

    async def connect(self) -> None:
        if self._exchange is not None:
            return
        self._connection = await aio_pika.connect_robust(self._settings.rabbit_url)
        # publisher_confirms=True: publish ждёт подтверждения брокера
        self._channel = await self._connection.channel(publisher_confirms=True)
        self._exchange = await self._channel.declare_exchange(
            self._settings.rabbit_exchange,
            aio_pika.ExchangeType.TOPIC,
            durable=True,
        )
        log.info("издатель подключён к %s, обменник %s",
                 self._settings.rabbit_url, self._settings.rabbit_exchange)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
        self._connection = self._channel = self._exchange = None

    async def publish(self, target: str, payload: str, message_id: str | None = None) -> None:
        """Отправить одно событие. Бросает исключение, если брокер не подтвердил."""
        await self.connect()
        assert self._exchange is not None
        message = aio_pika.Message(
            body=payload.encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=message_id,
            headers={"target": target},
        )
        # mandatory=True заставляет брокер вернуть сообщение, которое некуда
        # положить. Вернуть, а не бросить исключение: aio-pika отдаёт результат
        # Basic.Return обычным значением, и молчаливая потеря выглядит как
        # успешная отправка ровно до того момента, когда её замечает человек.
        # Поэтому возврат разбирается здесь и превращается в исключение,
        # которое очередь в базе видит как неудачу и ставит событие на повтор.
        confirmation = await self._exchange.publish(
            message, routing_key(target), mandatory=True)
        delivery = getattr(confirmation, "delivery", confirmation)
        if isinstance(delivery, pamqp.commands.Basic.Return):
            raise Unrouted(
                f"обменник {self._settings.rabbit_exchange} вернул сообщение "
                f"с ключом {routing_key(target)}: нет подходящей очереди "
                f"({delivery.reply_text})")
        if isinstance(delivery, pamqp.commands.Basic.Nack):
            raise Unrouted(
                f"брокер не подтвердил запись сообщения {message_id}")

    async def __aenter__(self) -> RabbitPublisher:
        await self.connect()
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None,
                        exc: BaseException | None, tb: TracebackType | None) -> None:
        await self.close()


async def declare_consumer_topology(channel: aio_pika.abc.AbstractChannel,
                                    target: str,
                                    settings: Settings | None = None) -> aio_pika.abc.AbstractQueue:
    """Объявить очередь приёмника вместе с её мёртвой очередью.

    Топология объявляется приёмником, а не издателем, потому что издатель
    не знает и не должен знать, кто читает. Объявление идемпотентно, его
    можно выполнять при каждом старте.
    """
    settings = settings or get_settings()
    dead_exchange_name = settings.rabbit_exchange + DEAD_EXCHANGE_SUFFIX

    exchange = await channel.declare_exchange(
        settings.rabbit_exchange, aio_pika.ExchangeType.TOPIC, durable=True)
    dead_exchange = await channel.declare_exchange(
        dead_exchange_name, aio_pika.ExchangeType.TOPIC, durable=True)

    name = queue_name(settings, target)
    dead_queue = await channel.declare_queue(name + DEAD_EXCHANGE_SUFFIX, durable=True)
    await dead_queue.bind(dead_exchange, routing_key(target))

    queue = await channel.declare_queue(
        name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": dead_exchange_name,
            "x-dead-letter-routing-key": routing_key(target),
        },
    )
    await queue.bind(exchange, routing_key(target))
    return queue
