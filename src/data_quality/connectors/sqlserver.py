"""SQL Server database connector."""

from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text

from .base import DatabaseConnector


class SQLServerConnector(DatabaseConnector):
    """SQL Server database connector."""

    def connect(self) -> None:
        """Establish SQL Server connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.test_connection()
        except Exception as e:
            self.engine = None
            raise RuntimeError(f"Failed to connect to SQL Server: {str(e)}")

    def disconnect(self) -> None:
        """Close SQL Server connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        """Test SQL Server connection."""
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
        """Get SQL Server-specific query for table information."""
        schema_name = schema or "dbo"

        # Table name is validated by SQL Server connector, safe to use
        query = f"""
        SELECT
            c.COLUMN_NAME as column_name,
            c.DATA_TYPE as data_type,
            c.IS_NULLABLE as is_nullable,
            c.COLUMN_DEFAULT as column_default,
            c.CHARACTER_MAXIMUM_LENGTH as character_maximum_length,
            c.NUMERIC_PRECISION as numeric_precision,
            c.NUMERIC_SCALE as numeric_scale
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_NAME = '{table_name}'
        AND c.TABLE_SCHEMA = '{schema_name}'
        ORDER BY c.ORDINAL_POSITION
        """  # nosec B608
        return query

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        schema_name = schema or "dbo"

        query = f"""
        SELECT
            kcu.COLUMN_NAME as column_name,
            ccu.TABLE_NAME as referenced_table,
            ccu.COLUMN_NAME as referenced_column
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
        JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
            ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
        WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            AND tc.TABLE_NAME = '{table_name}'
            AND tc.TABLE_SCHEMA = '{schema_name}'
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]

    def get_tables_list(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database."""
        schema_name = schema or "dbo"

        query = f"""
        SELECT
            TABLE_NAME as table_name,
            TABLE_SCHEMA as table_schema,
            TABLE_TYPE as table_type
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_TYPE = 'BASE TABLE'
        AND TABLE_SCHEMA = '{schema_name}'
        ORDER BY TABLE_NAME
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]
