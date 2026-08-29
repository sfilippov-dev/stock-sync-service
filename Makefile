DSN ?= postgresql+asyncpg://stocksync:stocksync@localhost:5434/stocksync
export STOCKSYNC_DATABASE_URL = $(DSN)

.PHONY: up down migrate seed run worker test lint demo clean

up:
	docker compose up -d --wait db

migrate:
	python -m alembic upgrade head

seed:
	python scripts/seed.py

run:
	uvicorn app.main:app --reload --port 8000

worker:
	python -m app.worker --interval 2 --dry-run

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
