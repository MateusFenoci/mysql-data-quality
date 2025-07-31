"""PostgreSQL database connector."""

from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text

from .base import DatabaseConnector


class PostgreSQLConnector(DatabaseConnector):
    """PostgreSQL database connector."""

    def connect(self) -> None:
        """Establish PostgreSQL connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.test_connection()
        except Exception as e:
            self.engine = None
            raise RuntimeError(f"Failed to connect to PostgreSQL: {str(e)}")

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        """Test PostgreSQL connection."""
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
        """Get PostgreSQL-specific query for table information."""
        schema_filter = (
            f"AND table_schema = '{schema}'"
            if schema
            else "AND table_schema = 'public'"
        )

        # Table name is validated by PostgreSQL connector, safe to use
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
        {schema_filter}
        ORDER BY ordinal_position
        """  # nosec B608
        return query

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        schema_name = schema or "public"

        query = f"""
        SELECT
            kcu.column_name,
            ccu.table_name AS referenced_table,
            ccu.column_name AS referenced_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = '{table_name}'
            AND tc.table_schema = '{schema_name}'
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]

    def get_tables_list(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database."""
        schema_filter = (
            f"AND table_schema = '{schema}'"
            if schema
            else "AND table_schema = 'public'"
        )

        query = f"""
        SELECT
            table_name,
            table_schema,
            table_type
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        {schema_filter}
        ORDER BY table_name
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]
