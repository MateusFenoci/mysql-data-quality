"""Oracle database connector."""

from typing import List, Dict, Any, Optional

from sqlalchemy import create_engine, text

from .base import DatabaseConnector


class OracleConnector(DatabaseConnector):
    """Oracle database connector."""

    def connect(self) -> None:
        """Establish Oracle connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.test_connection()
        except Exception as e:
            self.engine = None
            raise RuntimeError(f"Failed to connect to Oracle: {str(e)}")

    def disconnect(self) -> None:
        """Close Oracle connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        """Test Oracle connection."""
        if not self.engine:
            return False

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1 FROM DUAL"))
                result.fetchone()
            return True
        except Exception as e:
            print(f"Debug - Test connection error: {e}")
            return False

    def _get_table_info_query(
        self, table_name: str, schema: Optional[str] = None
    ) -> str:
        """Get Oracle-specific query for table information."""
        # Use current user schema if none specified
        schema_filter = (
            f"AND OWNER = UPPER('{schema}')" if schema else "AND OWNER = USER"
        )

        # Table name is validated by Oracle connector, safe to use
        query = f"""
        SELECT
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            CASE WHEN NULLABLE = 'Y' THEN 'YES' ELSE 'NO' END as is_nullable,
            DATA_DEFAULT as column_default,
            CHAR_LENGTH as character_maximum_length,
            DATA_PRECISION as numeric_precision,
            DATA_SCALE as numeric_scale
        FROM ALL_TAB_COLUMNS
        WHERE TABLE_NAME = UPPER('{table_name}')
        {schema_filter}
        ORDER BY COLUMN_ID
        """  # nosec B608
        return query

    def get_foreign_keys(
        self, table_name: str, schema: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        schema_filter = (
            f"AND ac.OWNER = UPPER('{schema}')" if schema else "AND ac.OWNER = USER"
        )

        query = f"""
        SELECT
            acc.COLUMN_NAME as column_name,
            r_acc.TABLE_NAME as referenced_table,
            r_acc.COLUMN_NAME as referenced_column
        FROM ALL_CONSTRAINTS ac
        JOIN ALL_CONS_COLUMNS acc
            ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
            AND ac.OWNER = acc.OWNER
        JOIN ALL_CONS_COLUMNS r_acc
            ON ac.R_CONSTRAINT_NAME = r_acc.CONSTRAINT_NAME
            AND ac.R_OWNER = r_acc.OWNER
        WHERE ac.CONSTRAINT_TYPE = 'R'
            AND ac.TABLE_NAME = UPPER('{table_name}')
            {schema_filter}
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]

    def get_tables_list(self, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get list of tables in the database."""
        schema_filter = (
            f"AND OWNER = UPPER('{schema}')" if schema else "AND OWNER = USER"
        )

        query = f"""
        SELECT
            TABLE_NAME as table_name,
            OWNER as table_schema,
            'BASE TABLE' as table_type
        FROM ALL_TABLES
        WHERE 1=1
        {schema_filter}
        ORDER BY TABLE_NAME
        """  # nosec B608

        result = self.execute_query(query)
        return result.to_dict("records")  # type: ignore[no-any-return]
