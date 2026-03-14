PYTHON ?= python
PIP ?= pip
SRC_DIR := src

.PHONY: install install-dev run-local test lint format clean

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

install-dev:
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .[dev]

run-local:
	$(PYTHON) -m transport_etl.main --job daily --config config/dev.yaml --run-date 2026-01-01 --no-register-hive

test:
	pytest -q

lint:
	ruff check $(SRC_DIR) tests

format:
	black $(SRC_DIR) tests

clean:
	powershell -NoProfile -Command "Get-ChildItem -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force"
	powershell -NoProfile -Command "if (Test-Path .pytest_cache) { Remove-Item .pytest_cache -Recurse -Force }"
