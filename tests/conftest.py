"""Pytest configuration and fixtures."""

import os
import tempfile
from typing import Generator
from unittest.mock import Mock

import pytest
from sqlalchemy.engine import Engine

from data_quality.config import AppConfig, DatabaseConfig


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create temporary directory for tests."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_engine() -> Mock:
    """Mock SQLAlchemy engine."""
    return Mock(spec=Engine)


@pytest.fixture
def test_db_config() -> DatabaseConfig:
    """Test database configuration."""
    return DatabaseConfig(
        host="localhost",
        port=5432,
        name="test_db",
        user="test_user",
        password="test_password",
        driver="postgresql",
    )


@pytest.fixture
def test_app_config(temp_dir: str) -> AppConfig:
    """Test application configuration."""
    return AppConfig(
        log_level="DEBUG",
        reports_output_dir=temp_dir,
        max_connections=5,
        environment="test",
        secret_key="test-secret-key",
    )


@pytest.fixture(autouse=True)
def set_test_env():
    """Set test environment variables."""
    os.environ["ENVIRONMENT"] = "test"
    yield
    os.environ.pop("ENVIRONMENT", None)
