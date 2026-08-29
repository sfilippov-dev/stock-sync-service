"""Запись остатков: журнал, холостые правки, атомарность партии."""

from sqlalchemy import func, select

from app.models import ChangeLog, OutboxEvent, Stock


async def test_new_sku_is_created_with_no_previous_value(client, session, update):
    response = await client.post("/v1/stock", json={"updates": [update(quantity=42)]})
    assert response.status_code == 200
    assert response.json()["applied"] == 1

    change = (await session.execute(select(ChangeLog))).scalar_one()
    assert change.previous_quantity is None
    assert change.new_quantity == 42


async def test_change_log_keeps_the_previous_value(client, session, update):
    """Ради этого свойства откат становится одной командой, а не
    восстановлением из резервной копии."""
    await client.post("/v1/stock", json={"updates": [update(quantity=10)]})
    await client.post("/v1/stock", json={"updates": [update(quantity=3)]})

    changes = (await session.execute(select(ChangeLog).order_by(ChangeLog.id))).scalars().all()
    assert [(c.previous_quantity, c.new_quantity) for c in changes] == [(None, 10), (10, 3)]


async def test_writing_the_same_value_changes_nothing(client, session, update):
    """Площадки шлют одно и то же состояние по расписанию. Без этой
    проверки журнал за неделю разрастается до миллионов строк, в которых
    ничего не менялось, а очередь событий забивается пустыми правками."""
    await client.post("/v1/stock", json={"updates": [update(quantity=5)]})
    response = await client.post("/v1/stock", json={"updates": [update(quantity=5)]})

    assert response.json() == {**response.json(), "applied": 0, "unchanged": 1}
    assert (await session.execute(select(func.count()).select_from(ChangeLog))).scalar_one() == 1
    assert (await session.execute(select(func.count()).select_from(OutboxEvent))).scalar_one() == 2


async def test_version_grows_with_every_real_change(client, update):
    await client.post("/v1/stock", json={"updates": [update(quantity=1)]})
    response = await client.post("/v1/stock", json={"updates": [update(quantity=2)]})
    assert response.json()["items"][0]["version"] == 2


async def test_unknown_warehouse_is_rejected(client, session, update):
    response = await client.post("/v1/stock",
                                 json={"updates": [update(warehouse="berlin-1")]})
    assert response.status_code == 422
    assert "berlin-1" in response.json()["detail"]


async def test_a_bad_row_cancels_the_whole_batch(client, session, update):
    """Партия применяется в одной транзакции. Иначе половина остатков
    уехала, половина нет, и никто не знает, где именно оборвалось."""
    body = {"updates": [update(sku="ART-A"), update(sku="ART-B", warehouse="berlin-1")]}
    response = await client.post("/v1/stock", json=body)
    assert response.status_code == 422
    assert (await session.execute(select(func.count()).select_from(Stock))).scalar_one() == 0


async def test_negative_quantity_is_rejected_by_the_schema(client, update):
    response = await client.post("/v1/stock", json={"updates": [update(quantity=-1)]})
    assert response.status_code == 422


async def test_empty_batch_is_rejected(client):
    assert (await client.post("/v1/stock", json={"updates": []})).status_code == 422


async def test_stock_can_be_listed_and_filtered(client, update):
    await client.post("/v1/stock", json={"updates": [
        update(sku="ART-A", warehouse="spb-1", quantity=3),
        update(sku="ART-B", warehouse="msk-1", quantity=8),
    ]})
    everything = (await client.get("/v1/stock")).json()
    only_msk = (await client.get("/v1/stock", params={"warehouse": "msk-1"})).json()
    assert len(everything) == 2
    assert [row["sku"] for row in only_msk] == ["ART-B"]


async def test_events_are_queued_for_every_target(client, session, update):
    """Событие пишется в той же транзакции, что и изменение. Иначе бывает
    «остаток изменён, но никто не узнал» и «все узнали об изменении,
    которого не было»."""
    await client.post("/v1/stock", json={"updates": [update()]})
    targets = (await session.execute(select(OutboxEvent.target))).scalars().all()
    assert sorted(targets) == ["ozon", "wb"]
