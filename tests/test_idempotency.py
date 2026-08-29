"""Повторный вебхук не должен двигать остаток второй раз."""

from sqlalchemy import func, select

from app.models import ChangeLog, OutboxEvent


async def test_same_key_applies_once(client, session, update):
    body = {"updates": [update(quantity=50)], "actor": "wb"}
    headers = {"Idempotency-Key": "wb-evt-001"}

    first = await client.post("/v1/stock", json=body, headers=headers)
    second = await client.post("/v1/stock", json=body, headers=headers)

    assert first.status_code == second.status_code == 200
    assert first.json() == second.json(), "повтор обязан вернуть тот же ответ"
    assert second.headers.get("Idempotent-Replay") == "true"

    changes = (await session.execute(select(func.count()).select_from(ChangeLog))).scalar_one()
    assert changes == 1, "остаток изменился дважды"


async def test_replay_does_not_queue_events_again(client, session, update):
    headers = {"Idempotency-Key": "wb-evt-002"}
    body = {"updates": [update(quantity=7)]}
    await client.post("/v1/stock", json=body, headers=headers)
    before = (await session.execute(select(func.count()).select_from(OutboxEvent))).scalar_one()
    await client.post("/v1/stock", json=body, headers=headers)
    after = (await session.execute(select(func.count()).select_from(OutboxEvent))).scalar_one()
    assert before == after


async def test_same_key_with_a_different_body_is_a_conflict(client, update):
    headers = {"Idempotency-Key": "wb-evt-003"}
    await client.post("/v1/stock", json={"updates": [update(quantity=1)]}, headers=headers)
    response = await client.post("/v1/stock", json={"updates": [update(quantity=2)]},
                                 headers=headers)
    assert response.status_code == 409
    assert "другим телом" in response.json()["detail"]


async def test_without_a_key_each_request_is_applied(client, session, update):
    """Ключ необязателен: без него сервис ведёт себя как обычная ручка."""
    await client.post("/v1/stock", json={"updates": [update(quantity=1)]})
    await client.post("/v1/stock", json={"updates": [update(quantity=2)]})
    changes = (await session.execute(select(func.count()).select_from(ChangeLog))).scalar_one()
    assert changes == 2


async def test_expired_keys_are_purged(client, session, update):
    from datetime import timedelta

    from app.models import IdempotencyKey, utcnow
    await client.post("/v1/stock", json={"updates": [update()]},
                      headers={"Idempotency-Key": "old-key"})
    record = await session.get(IdempotencyKey, "old-key")
    record.created_at = utcnow() - timedelta(days=3)
    await session.flush()

    response = await client.post("/v1/maintenance/purge-idempotency")
    assert response.json() == {"removed": 1, "left": 0}
