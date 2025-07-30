"""Tests for configuration module."""

import os
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from data_quality.config import AppConfig, DatabaseConfig, load_config


class TestDatabaseConfig:
    """Test database configuration."""

    def test_default_values(self):
        """Test default configuration values."""
        config = DatabaseConfig(name="test_db", user="test_user", password="test_pass")
        assert config.host == "localhost"
        assert config.port == 3306
        assert config.driver == "mysql"

    def test_mysql_connection_string(self):
        """Test MySQL connection string generation."""
        config = DatabaseConfig(
            host="db.example.com",
            port=3306,
            name="mydb",
            user="myuser",
            password="mypass",
            driver="mysql",
        )
        expected = "mysql+pymysql://myuser:mypass@db.example.com:3306/mydb"
        assert config.connection_string == expected

    def test_invalid_driver(self):
        """Test invalid driver validation."""
        with pytest.raises(ValidationError):
            DatabaseConfig(
                name="test_db",
                user="test_user",
                password="test_pass",
                driver="postgresql",
            )


class TestAppConfig:
    """Test application configuration."""

    def test_default_values(self):
        """Test default configuration values."""
        config = AppConfig(secret_key="test-key")
        assert config.log_level == "INFO"
        assert config.max_connections == 10
        assert config.environment == "test"

    def test_log_level_validation(self):
        """Test log level validation."""
        config = AppConfig(log_level="debug", secret_key="test-key")
        assert config.log_level == "DEBUG"

        with pytest.raises(ValidationError):
            AppConfig(log_level="INVALID", secret_key="test-key")

    def test_reports_dir_creation(self, temp_dir: str):
        """Test reports directory creation."""
        reports_path = Path(temp_dir) / "reports"
        config = AppConfig(reports_output_dir=reports_path, secret_key="test-key")
        assert config.reports_output_dir.exists()
        assert config.reports_output_dir.is_dir()


def test_load_config():
    """Test configuration loading."""
    with tempfile.TemporaryDirectory() as temp_dir:
        env_file = Path(temp_dir) / ".env"
        env_file.write_text(
            "DB_NAME=test_db\n"
            "DB_USER=test_user\n"
            "DB_PASSWORD=test_pass\n"
            "SECRET_KEY=test-secret\n"
        )

        old_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            config = load_config()
            assert "app" in config
            assert "database" in config
            assert config["database"].name == os.getenv("DB_NAME")
        finally:
            os.chdir(old_cwd)
