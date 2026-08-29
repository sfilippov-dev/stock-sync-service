"""Подключение к базе."""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings

_settings = get_settings()

engine = create_async_engine(
    _settings.database_url,
    echo=_settings.echo_sql,
    pool_pre_ping=True,   # соединение могло протухнуть, пока сервис простаивал
)

SessionMaker = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    """Сессия на один запрос.

    Транзакция закрывается здесь, а не в обработчике: иначе половина
    обработчиков забудет откатить её при ошибке, и соединение утечёт.
    """
    async with SessionMaker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
