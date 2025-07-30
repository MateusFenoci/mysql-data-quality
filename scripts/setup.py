#!/usr/bin/env python3
"""Data Quality Tool - Setup Script (Python version)."""

import subprocess
import sys
from pathlib import Path


def run_command(command, description, check=True):
    """Run a shell command with description."""
    print(f"🔧 {description}...")
    try:
        result = subprocess.run(
            command, shell=True, check=check, capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"✅ {description} completed successfully!")
            return True
        else:
            print(f"❌ {description} failed!")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return False
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed: {e}")
        return False


def check_poetry():
    """Check if Poetry is installed."""
    try:
        subprocess.run(["poetry", "--version"], check=True, capture_output=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Poetry is not installed. Please install Poetry first:")
        print("   curl -sSL https://install.python-poetry.org | python3 -")
        return False


def setup_env_file():
    """Setup .env file from template."""
    env_file = Path(".env")
    env_example = Path(".env.example")

    if not env_file.exists():
        if env_example.exists():
            print("📋 Creating .env file from template...")
            env_file.write_text(env_example.read_text())
            print("⚠️  Please edit .env file with your database credentials!")
            print(
                "   Required: DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_DRIVER"
            )
            return False  # Need user to update
        else:
            print("❌ .env.example not found!")
            return False
    return True


def test_database_connection():
    """Test database connection."""
    print("🔌 Testing database connection...")

    test_script = """
import sys
sys.path.append("src")
try:
    from data_quality.config import load_config
    from data_quality.connectors.factory import DatabaseConnectorFactory

    config = load_config()
    db_config = config["database"]
    connector = DatabaseConnectorFactory.create_connector(db_config.connection_string, db_config.driver)
    connector.connect()

    if connector.test_connection():
        print("✅ Database connection successful!")
        connector.disconnect()
        sys.exit(0)
    else:
        print("❌ Database connection failed!")
        sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
"""

    try:
        subprocess.run(
            ["poetry", "run", "python", "-c", test_script],
            check=True,
            capture_output=True,
            text=True,
        )
        return True
    except subprocess.CalledProcessError:
        print("❌ Database connection test failed!")
        return False


def main():
    """Main setup function."""
    print("🚀 Setting up Data Quality Tool environment...")

    # Check Poetry
    if not check_poetry():
        sys.exit(1)

    # Setup .env file
    if not setup_env_file():
        print("\n🛑 Please update .env file and run setup again.")
        sys.exit(1)

    # Install dependencies
    if not run_command("poetry install --no-root", "Installing dependencies"):
        sys.exit(1)

    # Setup pre-commit hooks
    if not run_command("poetry run pre-commit install", "Setting up pre-commit hooks"):
        print("⚠️  Pre-commit setup failed, but continuing...")

    # Create reports directory
    Path("reports").mkdir(exist_ok=True)
    print("✅ Reports directory created!")

    # Test database connection
    if not test_database_connection():
        print("❌ Setup failed due to database connection issues.")
        print("   Please check your .env configuration and try again.")
        sys.exit(1)

    print("\n🎯 Setup completed successfully!")
    print("\n📋 Available commands:")
    print("   poetry run data-quality --help              # Show CLI help")
    print("   poetry run data-quality test-connection     # Test database connection")
    print("   poetry run data-quality list-tables         # List all tables")
    print("   poetry run data-quality describe-table <name> # Describe table structure")
    print("   make test                                   # Run tests")
    print("   make lint                                   # Run linting")
    print("   make format                                 # Format code")
    print("\n🔍 Ready to validate data quality!")


if __name__ == "__main__":
    main()
