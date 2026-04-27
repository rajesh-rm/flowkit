.PHONY: setup test test-unit test-integration lint typecheck clean

# --- Setup ---

setup: ## Set up local dev environment with uv
	uv venv .venv --python 3.11
	uv pip install -e ".[dev]"
	@echo "\nEnvironment ready. Run: source .venv/bin/activate"

# --- Tests ---

test: ## Run all tests
	.venv/bin/python -m pytest -v

test-unit: ## Run unit tests only (no Docker needed)
	.venv/bin/python -m pytest tests/unit/ -v

test-cov: ## Run unit tests with coverage report
	.venv/bin/python -m pytest tests/unit/ --cov=src/data_assets --cov-report=term-missing

test-cov-full: ## Run ALL tests with coverage (unit + integration, as used by SonarQube)
	.venv/bin/python -m pytest tests/ --cov=src/data_assets --cov-report=term-missing --cov-report=xml:coverage.xml

test-integration: ## Run integration tests (requires Docker)
	.venv/bin/python -m pytest tests/integration/ -v -m integration

# --- Code quality ---

lint: ## Run ruff linter
	.venv/bin/python -m ruff check src/ tests/

lint-fix: ## Auto-fix lint issues
	.venv/bin/python -m ruff check src/ tests/ --fix

typecheck: ## Run mypy type checker
	.venv/bin/python -m mypy src/data_assets/

# --- Cleanup ---

clean: ## Remove build artifacts and caches
	rm -rf .venv .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# --- Help ---

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
