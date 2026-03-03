.PHONY: all install install-dev format lint typecheck test test-cov test-property test-integration test-all clean build publish docs docs-build pre-commit benchmark help

PYTHON ?= python
UV ?= uv

all: help

help:
	@echo "Available commands:"
	@echo "  make install          Install package"
	@echo "  make install-dev      Install package with dev dependencies"
	@echo "  make format           Format code with ruff"
	@echo "  make lint             Run linters (ruff + mypy)"
	@echo "  make typecheck        Run mypy type checking"
	@echo "  make test             Run unit tests"
	@echo "  make test-cov         Run tests with coverage"
	@echo "  make test-property    Run property-based tests"
	@echo "  make test-integration Run integration tests (requires Snowflake)"
	@echo "  make test-all         Run lint, unit tests, and property tests"
	@echo "  make pre-commit       Install and run pre-commit hooks"
	@echo "  make clean            Clean build artifacts"
	@echo "  make build            Build package"
	@echo "  make publish          Publish to PyPI"
	@echo "  make docs             Serve documentation locally"
	@echo "  make docs-build       Build documentation"
	@echo "  make benchmark        Run benchmarks"

install:
	$(UV) pip install -e .

install-dev:
	$(UV) pip install -e ".[dev]"
	pre-commit install

format:
	ruff format src tests
	ruff check --fix src tests

lint: typecheck
	ruff check src tests
	ruff format --check src tests

typecheck:
	mypy src/langgraph_checkpoint_snowflake

test:
	pytest tests -v --ignore=tests/test_integration.py

test-cov:
	pytest tests -v --cov=src/langgraph_checkpoint_snowflake --cov-report=html --cov-report=term-missing --ignore=tests/test_integration.py

test-property:
	pytest tests/test_property.py -v --hypothesis-seed=0

test-integration:
	pytest tests -v -m integration

test-all: lint test test-property

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
	rm -rf coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

build: clean
	$(PYTHON) -m build

publish: build
	$(PYTHON) -m twine upload dist/*

docs:
	mkdocs serve

docs-build:
	mkdocs build

benchmark:
	$(PYTHON) -c "from langgraph_checkpoint_snowflake import SnowflakeSaver; \
		with SnowflakeSaver.from_env() as s: \
			s.setup(); \
			for k, v in s.benchmark(iterations=20).items(): print(v)"
