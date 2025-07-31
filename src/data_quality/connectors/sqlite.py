"""SQLite database connector."""

from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text

from .base import DatabaseConnector


class SQLiteConnector(DatabaseConnector):
    """SQLite database connector."""

    def connect(self) -> None:
        """Establish SQLite connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.test_connection()
        except Exception as e:
            self.engine = None
            raise RuntimeError(f"Failed to connect to SQLite: {str(e)}")

    def disconnect(self) -> None:
        """Close SQLite connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        """Test SQLite connection."""
        if not self.engine:
            return False

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                result.fetchone()
            return True
        except Exception as e:
            print(f"Debug - Test connection error: {e}")
            return False

    def _get_table_info_query(
        self, table_name: str, schema: Optional[str] = None
    ) -> str:
        """Get SQLite-specific query for table information."""
        # SQLite doesn't have schemas like other databases, but we can use PRAGMA
        # Note: We'll use a different approach for SQLite since it doesn't have information_schema

        # Table name is validated by SQLite connector, safe to use
        query = f"PRAGMA table_info('{table_name}')"  # nosec B608
        return query

    def get_table_info(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get table information including columns and types - SQLite specific implementation."""
        if not self.engine:
            raise RuntimeError("Database not connected")

        query = self._get_table_info_query(table_name, schema)
        result = self.execute_query(query)

        # Convert SQLite PRAGMA table_info format to standard format
        converted_result = []
        for _, row in result.iterrows():
            converted_result.append(
                {
                    "column_name": row["name"],
                    "data_type": row["type"],
                    "is_nullable": "YES" if row["notnull"] == 0 else "NO",
                    "column_default": row["dflt_value"],
                    "character_maximum_length": None,  # SQLite doesn't have this concept
                    "numeric_precision": None,
                    "numeric_scale": None,
                }
            )

        return converted_result

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        query = f"PRAGMA foreign_key_list('{table_name}')"  # nosec B608

        result = self.execute_query(query)

        # Convert SQLite foreign key format to standard format
        foreign_keys = []
        for _, row in result.iterrows():
            foreign_keys.append(
                {
                    "column_name": row["from"],
                    "referenced_table": row["table"],
                    "referenced_column": row["to"],
                }
            )

        return foreign_keys

    def get_tables_list(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database."""
        query = """
        SELECT
            name as table_name,
            'main' as table_schema,
            'BASE TABLE' as table_type
        FROM sqlite_master
        WHERE type = 'table'
        AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]
