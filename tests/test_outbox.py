"""Очередь доставки: повторы, мёртвые события, возврат в строй."""

import asyncio

import pytest
from sqlalchemy import select

from app.config import Settings
from app.models import DeliveryStatus, OutboxEvent
from app.services.outbox import dispatch_once, next_delay, revive_dead

FAST = Settings(delivery_attempts=3, delivery_base_delay=0.001, delivery_max_delay=0.002)


async def queue_one_change(client, update, quantity=5):
    return await client.post("/v1/stock", json={"updates": [update(quantity=quantity)]})


async def drain(session, sender, settings=None, rounds=12):
    """Прогнать очередь до конца, уважая расписание повторов.

    После неудачи событию назначается время следующей попытки, и вызывать
    dispatch_once подряд без пауз бессмысленно: события просто не попадут
    в выборку. Первая версия теста этого не учитывала и падала через раз.
    """
    settings = settings or FAST
    totals = [0, 0, 0]
    for _ in range(rounds):
        result = await dispatch_once(session, sender, settings)
        totals[0] += result.delivered
        totals[1] += result.retried
        totals[2] += result.dead
        remaining = (await session.execute(
            select(OutboxEvent).where(
                OutboxEvent.status.in_([DeliveryStatus.PENDING, DeliveryStatus.FAILED])
            )
        )).scalars().all()
        if not remaining:
            break
        await asyncio.sleep(settings.delivery_max_delay + 0.005)
    return totals


async def test_successful_delivery_marks_events_done(client, session, update):
    await queue_one_change(client, update)
    sent = []

    async def sender(target, payload):
        sent.append((target, payload))

    result = await dispatch_once(session, sender, FAST)
    assert result.delivered == 2 and len(sent) == 2
    statuses = (await session.execute(select(OutboxEvent.status))).scalars().all()
    assert set(statuses) == {DeliveryStatus.DELIVERED}


async def test_failure_schedules_a_retry_and_keeps_the_reason(client, session, update):
    await queue_one_change(client, update)

    async def broken(target, payload):
        raise ConnectionError("приёмник недоступен")

    result = await dispatch_once(session, broken, FAST)
    assert result.retried == 2 and result.dead == 0

    event = (await session.execute(select(OutboxEvent))).scalars().first()
    assert event.status == DeliveryStatus.FAILED
    assert "приёмник недоступен" in event.last_error


async def test_events_go_to_the_dead_queue_after_the_declared_attempts(client, session, update):
    """Мёртвая очередь это отдельное состояние, а не «просто не доставлено».
    Такие события требуют человека, а не очередного повтора, и смешивать
    их с обычными неудачами значит никогда их не заметить."""
    await queue_one_change(client, update)

    async def broken(target, payload):
        raise TimeoutError("нет ответа")

    await drain(session, broken)

    statuses = (await session.execute(select(OutboxEvent.status))).scalars().all()
    assert set(statuses) == {DeliveryStatus.DEAD}


async def test_dead_events_can_be_revived_after_the_cause_is_fixed(client, session, update):
    await queue_one_change(client, update)

    async def broken(target, payload):
        raise TimeoutError("нет ответа")

    await drain(session, broken)

    assert await revive_dead(session) == 2
    event = (await session.execute(select(OutboxEvent))).scalars().first()
    assert event.status == DeliveryStatus.PENDING
    assert event.attempts == 0, "иначе событие уйдёт в мёртвые с первой же неудачи"

    delivered = []

    async def working(target, payload):
        delivered.append(target)

    result = await dispatch_once(session, working, FAST)
    assert result.delivered == 2


async def test_one_broken_target_does_not_block_the_other(client, session, update):
    await queue_one_change(client, update)

    async def half_broken(target, payload):
        if target == "wb":
            raise ConnectionError("wb лежит")

    result = await dispatch_once(session, half_broken, FAST)
    assert result.delivered == 1 and result.retried == 1


async def test_delivered_events_are_not_sent_twice(client, session, update):
    await queue_one_change(client, update)
    calls = []

    async def sender(target, payload):
        calls.append(target)

    await dispatch_once(session, sender, FAST)
    await dispatch_once(session, sender, FAST)
    assert len(calls) == 2


@pytest.mark.parametrize("attempt", [1, 2, 3, 8])
def test_retry_delay_grows_but_stays_under_the_ceiling(attempt):
    settings = Settings(delivery_base_delay=1, delivery_max_delay=60)
    assert 0 < next_delay(attempt, settings) <= 60


def test_retry_delay_has_spread():
    """Без разброса все накопленные события уходят одной волной ровно в тот
    момент, когда приёмник поднялся, и роняют его повторно."""
    settings = Settings(delivery_base_delay=10, delivery_max_delay=600)
    values = {round(next_delay(4, settings), 6) for _ in range(20)}
    assert len(values) > 15


async def test_health_reports_the_queue(client, session, update):
    await queue_one_change(client, update)
    health = (await client.get("/health")).json()
    assert health["status"] == "ok"
    assert health["pending_events"] == 2
    assert health["dead_events"] == 0
