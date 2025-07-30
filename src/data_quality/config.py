"""Configuration management for data quality tool."""

import os
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel, field_validator


class DatabaseConfig(BaseModel):
    """Database configuration."""

    host: str = "localhost"
    port: int = 3306
    name: str = ""
    user: str = ""
    password: str = ""
    driver: str = "mysql"

    def __init__(self, **kwargs):
        # Load from environment variables with DB_ prefix
        env_data = {}
        for key in ["host", "port", "name", "user", "password", "driver"]:
            env_key = f"DB_{key.upper()}"
            if env_key in os.environ:
                env_data[key] = os.environ[env_key]

        # Override with any passed kwargs
        env_data.update(kwargs)
        super().__init__(**env_data)

    @field_validator("driver")
    @classmethod
    def validate_driver(cls, v: str) -> str:
        """Validate database driver."""
        allowed_drivers = ["mysql"]
        if v not in allowed_drivers:
            raise ValueError(f"Driver must be one of {allowed_drivers}")
        return v

    @property
    def connection_string(self) -> str:
        """Get database connection string."""
        if self.driver == "mysql":
            return f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"
        else:
            raise ValueError(f"Unsupported driver: {self.driver}")


class AppConfig(BaseModel):
    """Application configuration."""

    log_level: str = "INFO"
    reports_output_dir: Path = Path("./reports")
    max_connections: int = 10
    environment: str = "development"
    secret_key: str = ""

    def __init__(self, **kwargs):
        # Load from environment variables
        env_data = {}
        for key in ["log_level", "max_connections", "environment", "secret_key"]:
            env_key = key.upper()
            if env_key in os.environ:
                env_data[key] = os.environ[env_key]

        if "REPORTS_OUTPUT_DIR" in os.environ:
            env_data["reports_output_dir"] = Path(os.environ["REPORTS_OUTPUT_DIR"])

        # Override with any passed kwargs
        env_data.update(kwargs)
        super().__init__(**env_data)

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        allowed_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in allowed_levels:
            raise ValueError(f"Log level must be one of {allowed_levels}")
        return v.upper()

    @field_validator("reports_output_dir")
    @classmethod
    def create_reports_dir(cls, v: Path) -> Path:
        """Create reports directory if it doesn't exist."""
        v.mkdir(parents=True, exist_ok=True)
        return v


def load_config() -> Dict[str, Any]:
    """Load application configuration."""
    # Load .env file if it exists
    env_file = Path(".env")
    if env_file.exists():
        from dotenv import load_dotenv

        load_dotenv()

    app_config = AppConfig()
    db_config = DatabaseConfig()

    return {
        "app": app_config,
        "database": db_config,
    }
