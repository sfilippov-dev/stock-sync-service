"""Откат партии."""

from sqlalchemy import select

from app.models import ChangeLog


async def test_revert_restores_the_previous_value(client, session, update):
    await client.post("/v1/stock", json={"updates": [update(quantity=100)]})
    second = await client.post("/v1/stock", json={"updates": [update(quantity=7)]})
    batch_id = second.json()["batch_id"]

    response = await client.post(f"/v1/batches/{batch_id}/revert")
    assert response.status_code == 200
    assert response.json()["reverted"] == 1

    stock = (await client.get("/v1/stock")).json()[0]
    assert stock["quantity"] == 100


async def test_reverting_a_creation_zeroes_the_row_instead_of_deleting_it(client, update):
    """Строки до правки не существовало. Удалять её опасно: на неё уже
    могли сослаться, поэтому остаток обнуляется, а строка остаётся."""
    created = await client.post("/v1/stock", json={"updates": [update(quantity=25)]})
    await client.post(f"/v1/batches/{created.json()['batch_id']}/revert")

    rows = (await client.get("/v1/stock")).json()
    assert len(rows) == 1 and rows[0]["quantity"] == 0


async def test_revert_marks_the_original_change(client, session, update):
    batch = (await client.post("/v1/stock", json={"updates": [update()]})).json()["batch_id"]
    await client.post(f"/v1/batches/{batch}/revert")

    original = (await session.execute(
        select(ChangeLog).where(ChangeLog.batch_id == batch)
    )).scalar_one()
    assert original.reverted_by is not None


async def test_revert_is_itself_written_to_the_log(client, session, update):
    """Откат это тоже изменение остатка, и он обязан быть виден в журнале
    наравне с остальными: иначе история врёт."""
    created = await client.post("/v1/stock", json={"updates": [update(quantity=9)]})
    batch = created.json()["batch_id"]
    await client.post(f"/v1/batches/{batch}/revert")

    changes = (await client.get("/v1/changes")).json()
    assert any("откат" in change["reason"] for change in changes)


async def test_second_revert_of_the_same_batch_is_a_no_op(client, update):
    batch = (await client.post("/v1/stock", json={"updates": [update()]})).json()["batch_id"]
    await client.post(f"/v1/batches/{batch}/revert")
    response = await client.post(f"/v1/batches/{batch}/revert")
    assert response.status_code == 404


async def test_unknown_batch_is_not_found(client):
    assert (await client.post("/v1/batches/нетакой/revert")).status_code == 404


async def test_multi_row_batch_reverts_completely(client, update):
    batch = (await client.post("/v1/stock", json={"updates": [
        update(sku="ART-A", quantity=1), update(sku="ART-B", quantity=2),
        update(sku="ART-C", warehouse="msk-1", quantity=3),
    ]})).json()["batch_id"]

    response = await client.post(f"/v1/batches/{batch}/revert")
    assert response.json()["reverted"] == 3
    assert all(row["quantity"] == 0 for row in (await client.get("/v1/stock")).json())


async def test_revert_queues_events_for_the_external_systems(client, update):
    """Внешние системы обязаны узнать об откате так же, как узнали о правке."""
    batch = (await client.post("/v1/stock", json={"updates": [update()]})).json()["batch_id"]
    before = (await client.get("/v1/outbox")).json()["pending"]
    await client.post(f"/v1/batches/{batch}/revert")
    after = (await client.get("/v1/outbox")).json()["pending"]
    assert after == before + 2
