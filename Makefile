.PHONY: up down logs migrate test test-unit test-integration test-e2e lint fmt typecheck check clean

COMPOSE = docker compose
CORE    = cd core &&

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

migrate:
	$(CORE) alembic upgrade head

test: test-unit test-integration test-e2e

test-unit:
	$(CORE) pytest -m unit -v

test-integration:
	$(CORE) pytest -m integration -v

test-e2e:
	$(CORE) pytest -m e2e -v

lint:
	$(CORE) ruff check src tests
	$(CORE) lint-imports

fmt:
	$(CORE) ruff format src tests
	$(CORE) ruff check --fix src tests

typecheck:
	$(CORE) mypy src

check: lint typecheck test

clean:
	$(CORE) rm -rf .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov
