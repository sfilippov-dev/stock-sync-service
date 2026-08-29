"""Точка входа."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import admin, stock
from app.config import get_settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)


@asynccontextmanager
async def lifespan(application: FastAPI):
    logging.getLogger("stocksync").info("сервис запущен")
    yield
    from app.db import engine
    await engine.dispose()


app = FastAPI(
    title="Синхронизация остатков",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Один источник правды по остаткам товара и рассылка изменений во внешние "
        "системы.\n\n"
        "Три свойства, ради которых сервис существует: повторный вебхук не двигает "
        "остаток второй раз, любое изменение откатывается одной командой, а событие "
        "для внешних систем пишется в той же транзакции, что и само изменение."
    ),
)
app.include_router(admin.router)
app.include_router(stock.router)


@app.get("/", include_in_schema=False)
async def root() -> dict:
    return {"service": get_settings().app_name, "docs": "/docs"}
