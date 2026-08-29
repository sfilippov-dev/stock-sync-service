DSN ?= postgresql+asyncpg://stocksync:stocksync@localhost:5434/stocksync
AMQP ?= amqp://stocksync:stocksync@localhost:5672/
export STOCKSYNC_DATABASE_URL = $(DSN)
export STOCKSYNC_RABBIT_URL = $(AMQP)

.PHONY: up down migrate seed run worker consumer test lint demo clean

up:
	docker compose up -d --wait db rabbit

migrate:
	python -m alembic upgrade head

seed:
	python scripts/seed.py

run:
	uvicorn app.main:app --reload --port 8000

worker:
	python -m app.worker --interval 2 --transport rabbit

consumer:
	python -m app.consumer --target wb

test:
	python -m pytest

lint:
	ruff check app tests scripts

demo: up migrate seed
	@bash scripts/demo.sh

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf .pytest_cache .ruff_cache
	find . -name __pycache__ -type d -exec rm -rf {} +
