"""MySQL/MariaDB database connector."""

from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text

from .base import DatabaseConnector


class MySQLConnector(DatabaseConnector):
    """MySQL/MariaDB database connector."""

    def connect(self) -> None:
        """Establish MySQL/MariaDB connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.test_connection()
        except Exception as e:
            self.engine = None
            raise RuntimeError(f"Failed to connect to MySQL/MariaDB: {str(e)}")

    def disconnect(self) -> None:
        """Close MySQL/MariaDB connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        """Test MySQL/MariaDB connection."""
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
        """Get MySQL/MariaDB-specific query for table information."""
        database_filter = (
            f"AND table_schema = '{schema}'"
            if schema
            else "AND table_schema = DATABASE()"
        )

        # Table name is validated by MySQL connector, safe to use
        query = f"""
        SELECT
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        {database_filter}
        ORDER BY ordinal_position
        """  # nosec B608
        return query

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        database_filter = (
            f"AND kcu.table_schema = '{schema}'"
            if schema
            else "AND kcu.table_schema = DATABASE()"
        )

        query = f"""
        SELECT
            kcu.column_name,
            kcu.referenced_table_name as referenced_table,
            kcu.referenced_column_name as referenced_column
        FROM information_schema.key_column_usage kcu
        WHERE kcu.table_name = '{table_name}'
        AND kcu.referenced_table_name IS NOT NULL
        {database_filter}
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]

    def get_tables_list(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database."""
        database_filter = (
            f"AND table_schema = '{schema}'"
            if schema
            else "AND table_schema = DATABASE()"
        )

        query = f"""
        SELECT
            table_name,
            table_schema,
            table_type
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        {database_filter}
        ORDER BY table_name
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]
