.PHONY: all install install-dev format lint test test-cov test-integration clean build publish help pre-commit

PYTHON ?= python
UV ?= uv

all: help

help:
	@echo "Available commands:"
	@echo "  make install          Install package"
	@echo "  make install-dev      Install package with dev dependencies"
	@echo "  make format           Format code with ruff"
	@echo "  make lint             Run linters (ruff + mypy)"
	@echo "  make test             Run unit tests"
	@echo "  make test-cov         Run tests with coverage"
	@echo "  make test-integration Run integration tests (requires Snowflake)"
	@echo "  make pre-commit       Install and run pre-commit hooks"
	@echo "  make clean            Clean build artifacts"
	@echo "  make build            Build package"
	@echo "  make publish          Publish to PyPI"

install:
	$(UV) pip install -e .

install-dev:
	$(UV) pip install -e ".[dev]"

format:
	ruff format src tests
	ruff check --fix src tests

lint:
	ruff check src tests
	mypy src/langgraph_checkpoint_snowflake

test:
	pytest tests -v --ignore=tests/test_integration.py

test-cov:
	pytest tests -v --cov=src/langgraph_checkpoint_snowflake --cov-report=html --cov-report=term-missing --ignore=tests/test_integration.py

test-integration:
	pytest tests -v -m integration

pre-commit:
	pre-commit install
	pre-commit run --all-files

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf src/*.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	$(PYTHON) -m build

publish: build
	$(PYTHON) -m twine upload dist/*
