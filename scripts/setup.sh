#!/bin/bash

# Data Quality Tool - Setup Script
# This script sets up the development environment

set -e  # Exit on any error

echo "🚀 Setting up Data Quality Tool environment..."

# Check if Poetry is installed
if ! command -v poetry &> /dev/null; then
    echo "❌ Poetry is not installed. Please install Poetry first:"
    echo "   curl -sSL https://install.python-poetry.org | python3 -"
    exit 1
fi

# Check if .env file exists
if [ ! -f .env ]; then
    echo "📋 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please edit .env file with your database credentials before proceeding."
    echo "   Database configuration required: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_DRIVER"
    echo ""
    read -p "Press Enter after updating .env file..."
fi

# Install dependencies
echo "📦 Installing dependencies..."
poetry install --no-root

# Install pre-commit hooks
echo "🔧 Setting up pre-commit hooks..."
poetry run pre-commit install

# Create reports directory
echo "📁 Creating reports directory..."
mkdir -p reports

# Test database connection
echo "🔌 Testing database connection..."
if poetry run python -c "
import sys
sys.path.append('src')
from data_quality.config import load_config
from data_quality.connectors.factory import DatabaseConnectorFactory

try:
    config = load_config()
    db_config = config['database']
    connector = DatabaseConnectorFactory.create_connector(db_config.connection_string, db_config.driver)
    connector.connect()
    if connector.test_connection():
        print('✅ Database connection successful!')
        connector.disconnect()
    else:
        print('❌ Database connection failed!')
        sys.exit(1)
except Exception as e:
    print(f'❌ Error: {e}')
    sys.exit(1)
"; then
    echo "✅ Setup completed successfully!"
    echo ""
    echo "🎯 Available commands:"
    echo "   poetry run data-quality --help          # Show CLI help"
    echo "   poetry run data-quality test-connection # Test database connection"
    echo "   poetry run data-quality list-tables     # List all tables"
    echo "   poetry run data-quality describe-table <name> # Describe table structure"
    echo "   make test                               # Run tests"
    echo "   make lint                               # Run linting"
    echo "   make format                             # Format code"
    echo ""
    echo "🔍 Ready to validate data quality!"
else
    echo "❌ Setup failed due to database connection issues."
    echo "   Please check your .env configuration and try again."
    exit 1
fi
