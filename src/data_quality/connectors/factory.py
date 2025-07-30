"""Database connector factory."""

from typing import Dict, Type

from .base import DatabaseConnector
from .mysql import MySQLConnector


class DatabaseConnectorFactory:
    """Factory for creating database connectors."""

    _connectors: Dict[str, Type[DatabaseConnector]] = {
        "mysql": MySQLConnector,
    }

    @classmethod
    def create_connector(cls, connection_string: str, driver: str) -> DatabaseConnector:
        """Create database connector based on driver type."""
        if driver not in cls._connectors:
            raise ValueError(f"Unsupported database driver: {driver}")

        connector_class = cls._connectors[driver]
        return connector_class(connection_string)

    @classmethod
    def register_connector(
        cls, driver: str, connector_class: Type[DatabaseConnector]
    ) -> None:
        """Register a new connector type."""
        cls._connectors[driver] = connector_class

    @classmethod
    def get_supported_drivers(cls) -> list[str]:
        """Get list of supported database drivers."""
        return list(cls._connectors.keys())
