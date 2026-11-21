# Crypto Intelligence Platform (CIP) Makefile
# Standard targets: up, down, lint, test, contracts, docs

VENV := .venv
UV := $(shell which uv 2>/dev/null || echo ~/.local/bin/uv)
PYTHON := $(VENV)/bin/python
RUFF := $(VENV)/bin/ruff
MYPY := $(VENV)/bin/mypy
PYTEST := $(VENV)/bin/pytest
COMPOSE_FILE := tools/docker-compose.yml

.PHONY: all help venv up down status lint test contracts docs clean

all: help

help:            ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

venv:            ## Create virtual environment and install dependencies
	$(UV) sync

up:              ## Start local docker-compose infrastructure
	docker compose -f $(COMPOSE_FILE) up -d --wait

down:            ## Stop local docker-compose infrastructure
	docker compose -f $(COMPOSE_FILE) down -v

status:          ## Check status of local infrastructure
	docker compose -f $(COMPOSE_FILE) ps

lint:            ## Run ruff and mypy strict across codebase
	$(RUFF) check .
	$(RUFF) format --check .
	$(MYPY) libs services

format:          ## Format code with ruff
	$(RUFF) format .
	$(RUFF) check --fix .

test:            ## Run pytest unit and integration tests
	$(PYTEST) -v libs services tests

contracts:       ## Validate Avro schemas and API contracts
	$(PYTHON) tools/validate_contracts.py

docs:            ## Regenerate agent-readable Markdown from the .drawio
	$(PYTHON) tools/drawio_to_md.py docs/architecture/CIP-architecture-design-pack.drawio docs/design

design:          ## Rebuild the .drawio from generator, then refresh Markdown
	$(PYTHON) tools/design-pack/build.py docs/architecture/CIP-architecture-design-pack.drawio
	$(MAKE) docs

clean:           ## Remove cache and temporary files
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
