.PHONY: up down logs sync migrate test test-unit test-integration test-e2e \
        lint fmt typecheck check clean

COMPOSE = docker compose
CORE    = cd core &&

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

sync:
	$(CORE) uv sync

migrate:
	$(CORE) uv run alembic upgrade head

test: test-unit test-integration test-e2e

test-unit:
	$(CORE) uv run pytest -m unit -v

test-integration:
	$(CORE) uv run pytest -m integration -v

test-e2e:
	$(CORE) uv run pytest -m e2e -v

lint:
	$(CORE) uv run ruff check src tests
	$(CORE) uv run lint-imports

fmt:
	$(CORE) uv run ruff format src tests
	$(CORE) uv run ruff check --fix src tests

typecheck:
	$(CORE) uv run mypy src

check: lint typecheck test

clean:
	$(CORE) rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
