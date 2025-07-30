.PHONY: help install install-dev test lint format type-check security clean setup

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install production dependencies
	poetry install --only main

install-dev: ## Install all dependencies including dev dependencies
	poetry install
	poetry run pre-commit install

setup: install-dev ## Complete project setup
	cp .env.example .env
	@echo "✅ Project setup complete! Please edit .env with your database credentials."

test: ## Run tests
	poetry run pytest

test-cov: ## Run tests with coverage
	poetry run pytest --cov=src --cov-report=html --cov-report=term

lint: ## Run linting
	poetry run black --check .
	poetry run isort --check-only .
	poetry run flake8 .

format: ## Format code
	poetry run black .
	poetry run isort .

type-check: ## Run type checking
	poetry run mypy src

security: ## Run security checks
	poetry run bandit -r src
	poetry run safety check

pre-commit: ## Run all pre-commit hooks
	poetry run pre-commit run --all-files

clean: ## Clean up generated files
	find . -type d -name __pycache__ -delete
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .coverage htmlcov/ .pytest_cache/ .mypy_cache/ dist/ build/

build: ## Build the package
	poetry build

dev: ## Run in development mode
	poetry run python -m data_quality.cli --help
