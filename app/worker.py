"""Отправщик событий.

Отдельный процесс, а не фоновая задача внутри веб-приложения. Причина
практическая: веб-процесс перезапускают при выкате, и фоновая задача внутри
него теряет незавершённую отправку. Отдельный воркер переживает выкат
приложения, а очередь в базе переживает перезапуск обоих.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

import httpx

from app.config import get_settings
from app.db import SessionMaker
from app.services.outbox import dispatch_once
from app.services.rabbit import RabbitPublisher

log = logging.getLogger("stocksync.worker")

TARGET_URLS = {
    "wb": "https://example.invalid/wb/stock",
    "ozon": "https://example.invalid/ozon/stock",
}


async def http_sender(target: str, payload: str, key: str) -> None:
    """Настоящая отправка. В демонстрации адреса заведомо недоступны,
    чтобы было видно, как работают повторы и мёртвая очередь."""
    url = TARGET_URLS.get(target)
    if url is None:
        raise ValueError(f"нет адреса для цели {target}")
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(url, json=json.loads(payload))
        response.raise_for_status()


async def logging_sender(target: str, payload: str, key: str) -> None:
    """Отправка в лог: режим, в котором сервис можно поднять без приёмников."""
    log.info("отправлено в %s событие %s: %s", target, key, payload)


def rabbit_sender(publisher: RabbitPublisher):
    """Отправитель в брокер, совместимый с протоколом Sender.

    Отдельная функция, а не метод издателя: dispatch_once ничего не знает
    про транспорт и принимает любую корутину вида (цель, тело, ключ). По
    той же причине http_sender и logging_sender подставляются в неё без
    изменений, а брокер добавился без единой правки в самой очереди.

    Ключ события уходит в message_id: по нему приёмник отличает повторную
    доставку от нового события.
    """
    async def send(target: str, payload: str, key: str) -> None:
        await publisher.publish(target, payload, message_id=key)
    return send


async def run(interval: float, once: bool, dry_run: bool,
              transport: str = "http") -> None:
    settings = get_settings()
    if dry_run:
        transport = "log"
    publisher: RabbitPublisher | None = None
    if transport == "rabbit":
        publisher = RabbitPublisher(settings)
        await publisher.connect()
        sender = rabbit_sender(publisher)
    elif transport == "log":
        sender = logging_sender
    else:
        sender = http_sender
    while True:
        async with SessionMaker() as session:
            result = await dispatch_once(session, sender, settings)
            await session.commit()
        if result.delivered or result.retried or result.dead:
            log.info("доставлено %s, отложено %s, в мёртвой очереди %s",
                     result.delivered, result.retried, result.dead)
        if once:
            break
        await asyncio.sleep(interval)
    if publisher is not None:
        await publisher.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    parser = argparse.ArgumentParser(description="Отправщик событий из очереди")
    parser.add_argument("--interval", type=float, default=2.0)
    parser.add_argument("--once", action="store_true", help="Один проход и выход.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Писать в лог вместо отправки по сети.")
    parser.add_argument("--transport", choices=("http", "rabbit", "log"), default="http",
                        help="Куда отдавать события: приёмнику по HTTP, "
                             "в RabbitMQ или в лог.")
    args = parser.parse_args()
    asyncio.run(run(args.interval, args.once, args.dry_run, args.transport))


if __name__ == "__main__":
    main()
