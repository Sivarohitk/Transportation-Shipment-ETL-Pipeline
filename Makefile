PYTHON ?= python

.PHONY: install install-dev run-local run-backfill test lint format clean

install:
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install -e .

install-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt
	$(PYTHON) -m pip install -e .

run-local:
	$(PYTHON) -m transport_etl.main --job daily --config dev --run-date 2026-01-01

run-backfill:
	$(PYTHON) -m transport_etl.main --job backfill --config dev --start-date 2026-01-01 --end-date 2026-01-02

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m black --check .

format:
	$(PYTHON) -m black .

clean:
	powershell -NoProfile -Command "Get-ChildItem -Recurse -Directory -Filter __pycache__ | Remove-Item -Recurse -Force"
	powershell -NoProfile -Command "if (Test-Path .pytest_cache) { Remove-Item .pytest_cache -Recurse -Force }"
