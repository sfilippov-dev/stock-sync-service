import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault(
    "STOCKSYNC_DATABASE_URL",
    "postgresql+asyncpg://stocksync:stocksync@localhost:5434/stocksync",
)

import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine  # noqa: E402

from app.config import get_settings  # noqa: E402
from app.db import get_session  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Base, Warehouse  # noqa: E402


@pytest_asyncio.fixture(scope="function")
async def engine():
    """Своя база на каждый тест-модуль, поднимаемая из моделей.

    Схема создаётся напрямую, а не миграциями: миграции проверяются
    отдельным тестом, и путать эти две проверки не стоит. Если схема в
    моделях и схема после миграций разойдутся, упадёт именно тот тест.
    """
    engine = create_async_engine(get_settings().database_url, poolclass=None)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session(engine):
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        session.add_all([
            Warehouse(code="spb-1", name="Санкт-Петербург"),
            Warehouse(code="msk-1", name="Москва"),
        ])
        await session.commit()
        yield session


@pytest_asyncio.fixture
async def client(engine, session):
    """Клиент, работающий на той же сессии, что и тест.

    Иначе тест пишет в своей транзакции, приложение читает в другой, и
    половина проверок падает по причинам, не имеющим отношения к коду.
    """
    async def override():
        yield session

    app.dependency_overrides[get_session] = override
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def update():
    def make(sku="ART-0001", warehouse="spb-1", quantity=10, reason="поставка"):
        return {"sku": sku, "warehouse": warehouse, "quantity": quantity, "reason": reason}
    return make
