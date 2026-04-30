.PHONY: setup test test-fast test-unit test-contract test-integration test-gui lint format coverage coverage-html clean

setup:
	python -m pip install --upgrade pip
	python -m pip install -r requirements-dev.txt
	pre-commit install
	playwright install chromium

test-fast:
	TESTING=1 pytest tests/unit -q

test-unit:
	TESTING=1 pytest tests/unit -v

test-contract:
	TESTING=1 pytest tests/contract -v

test-integration:
	TESTING=1 pytest tests/integration -v

test-gui:
	TESTING=1 pytest tests/gui -v

test:
	TESTING=1 pytest tests/unit tests/contract tests/integration tests/gui -v

coverage:
	TESTING=1 pytest tests/unit tests/contract --cov --cov-report=term-missing --cov-report=html
	@echo "Open htmlcov/index.html to view detailed coverage."

coverage-html: coverage
	@python -c "import os, webbrowser; webbrowser.open('file://' + os.path.abspath('htmlcov/index.html'))"

lint:
	ruff check app tests
	ruff format --check app tests
	mypy app

format:
	ruff format app tests
	ruff check --fix app tests

clean:
	rm -rf .pytest_cache .coverage htmlcov .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
