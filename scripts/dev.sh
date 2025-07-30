#!/bin/bash

# Data Quality Tool - Development Helper Script
# Quick commands for development workflow

set -e

COMMAND=${1:-help}

case $COMMAND in
    "setup")
        echo "🚀 Running full setup..."
        python scripts/setup.py
        ;;
    "install")
        echo "📦 Installing dependencies..."
        poetry install --no-root
        ;;
    "test")
        echo "🧪 Running tests..."
        poetry run pytest
        ;;
    "test-cov")
        echo "🧪 Running tests with coverage..."
        poetry run pytest --cov=src --cov-report=html --cov-report=term
        ;;
    "lint")
        echo "🔍 Running linting..."
        poetry run black --check .
        poetry run isort --check-only .
        poetry run flake8 .
        ;;
    "format")
        echo "🎨 Formatting code..."
        poetry run black .
        poetry run isort .
        ;;
    "type-check")
        echo "📝 Running type checking..."
        poetry run mypy src
        ;;
    "security")
        echo "🔒 Running security checks..."
        poetry run bandit -r src
        ;;
    "pre-commit")
        echo "🔧 Running pre-commit hooks..."
        poetry run pre-commit run --all-files
        ;;
    "clean")
        echo "🧹 Cleaning up..."
        find . -type d -name __pycache__ -delete
        find . -type f -name "*.pyc" -delete
        find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
        rm -rf .coverage htmlcov/ .pytest_cache/ .mypy_cache/ dist/ build/
        ;;
    "connect")
        echo "🔌 Testing database connection..."
        poetry run data-quality test-connection
        ;;
    "tables")
        echo "📋 Listing database tables..."
        poetry run data-quality list-tables
        ;;
    "tables-real")
        echo "📋 Listing database tables with real counts..."
        poetry run data-quality list-tables --real-count
        ;;
    "describe")
        if [ -z "$2" ]; then
            echo "❌ Please provide table name: ./scripts/dev.sh describe <table_name>"
            exit 1
        fi
        echo "🏗️  Describing table: $2"
        poetry run data-quality describe-table "$2"
        ;;
    "validate")
        echo "🔍 Running data quality validations..."
        poetry run data-quality validate
        ;;
    "shell")
        echo "🐚 Starting Poetry shell..."
        poetry shell
        ;;
    "help"|*)
        echo "🔍 Data Quality Tool - Development Commands"
        echo ""
        echo "Setup & Installation:"
        echo "  setup       - Run full project setup"
        echo "  install     - Install dependencies only"
        echo ""
        echo "Development:"
        echo "  test        - Run tests"
        echo "  test-cov    - Run tests with coverage"
        echo "  lint        - Run linting checks"
        echo "  format      - Format code"
        echo "  type-check  - Run type checking"
        echo "  security    - Run security checks"
        echo "  pre-commit  - Run pre-commit hooks"
        echo "  clean       - Clean up generated files"
        echo ""
        echo "Database:"
        echo "  connect     - Test database connection"
        echo "  tables      - List database tables (estimated counts)"
        echo "  tables-real - List database tables (real counts - slower)"
        echo "  describe <table> - Describe table structure"
        echo "  validate    - Run data quality validations"
        echo ""
        echo "Utilities:"
        echo "  shell       - Start Poetry shell"
        echo "  help        - Show this help"
        echo ""
        echo "Usage: ./scripts/dev.sh <command>"
        ;;
esac
