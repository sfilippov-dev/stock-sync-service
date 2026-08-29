"""Настройки сервиса.

Всё, что меняется между машинами, приходит из окружения. Значения по
умолчанию рассчитаны на docker-compose из этого репозитория, чтобы
`make up` работал без единой правки файла.
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STOCKSYNC_", env_file=".env",
                                      extra="ignore")

    database_url: str = "postgresql+asyncpg://stocksync:stocksync@localhost:5434/stocksync"
    app_name: str = "stock-sync-service"

    #: сколько раз пытаться доставить событие приёмнику
    delivery_attempts: int = Field(default=5, ge=1, le=20)
    #: базовая пауза перед повтором доставки, секунды
    delivery_base_delay: float = Field(default=1.0, gt=0)
    delivery_max_delay: float = Field(default=300.0, gt=0)
    #: сколько событий забирает воркер за один проход
    delivery_batch: int = Field(default=50, ge=1, le=1000)
    #: срок жизни ключа идемпотентности
    idempotency_ttl_hours: int = Field(default=24, ge=1)

    echo_sql: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
