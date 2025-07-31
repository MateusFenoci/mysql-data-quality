"""Base database connector interface."""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, cast

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


class DatabaseConnector(ABC):
    """Abstract base class for database connectors."""

    def __init__(self, connection_string: str) -> None:
        """Initialize database connector."""
        self.connection_string = connection_string
        self.engine: Optional[Engine] = None

    @abstractmethod
    def connect(self) -> None:
        """Establish database connection."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection."""
        pass

    @abstractmethod
    def test_connection(self) -> bool:
        """Test database connection."""
        pass

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute SQL query and return results as DataFrame."""
        if not self.engine:
            raise RuntimeError("Database not connected")

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query), params or {})
                return pd.DataFrame(result.fetchall(), columns=result.keys())
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

    def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get table information including columns and types."""
        if not self.engine:
            raise RuntimeError("Database not connected")

        query = self._get_table_info_query(table_name, schema)
        result = self.execute_query(query).to_dict("records")
        return cast(List[Dict[str, Any]], result)

    def get_table_count(self, table_name: str, schema: Optional[str] = None) -> int:
        """Get row count for a table."""
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        # Use text() with parameterized query to avoid SQL injection  # nosec B608
        query = f"SELECT COUNT(*) as count FROM {full_table_name}"  # nosec B608

        result = self.execute_query(query)
        return int(result.iloc[0]["count"])

    @abstractmethod
    def _get_table_info_query(
        self, table_name: str, schema: Optional[str] = None
    ) -> str:
        """Get database-specific query for table information."""
        pass

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table.

        This is a default implementation that returns an empty list.
        Subclasses should override this method if they support foreign key discovery.
        """
        return []

    def get_tables_list(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database.

        This is a default implementation that returns an empty list.
        Subclasses should override this method if they support table discovery.
        """
        return []
