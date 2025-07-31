"""Tests for PostgreSQL connector."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from data_quality.connectors.postgresql import PostgreSQLConnector


class TestPostgreSQLConnector:
    """Test PostgreSQL connector functionality."""

    def test_connector_initialization(self):
        """Test PostgreSQL connector initialization."""
        # Arrange & Act
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")

        # Assert
        assert connector.connection_string == "postgresql://user:pass@localhost/db"
        assert connector.engine is None

    @patch("data_quality.connectors.postgresql.create_engine")
    def test_connect_success(self, mock_create_engine):
        """Test successful PostgreSQL connection."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        with patch.object(connector, "test_connection", return_value=True):
            # Act
            connector.connect()

            # Assert
            assert connector.engine == mock_engine
            mock_create_engine.assert_called_once_with(
                "postgresql://user:pass@localhost/db"
            )

    @patch("data_quality.connectors.postgresql.create_engine")
    def test_connect_failure(self, mock_create_engine):
        """Test PostgreSQL connection failure."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_create_engine.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to connect to PostgreSQL"):
            connector.connect()

        assert connector.engine is None

    def test_disconnect(self):
        """Test PostgreSQL disconnection."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_engine = Mock()
        connector.engine = mock_engine

        # Act
        connector.disconnect()

        # Assert
        mock_engine.dispose.assert_called_once()
        assert connector.engine is None

    def test_disconnect_no_engine(self):
        """Test disconnection when no engine exists."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")

        # Act & Assert (should not raise exception)
        connector.disconnect()

    def test_test_connection_success(self):
        """Test successful connection test."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_engine = Mock()
        mock_conn = Mock()
        mock_result = Mock()

        # Set up context manager properly
        mock_engine.connect.return_value = mock_conn
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.return_value = mock_result
        connector.engine = mock_engine

        # Act
        result = connector.test_connection()

        # Assert
        assert result is True
        mock_conn.execute.assert_called_once()

    def test_test_connection_failure(self):
        """Test connection test failure."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_engine = Mock()
        mock_conn = Mock()

        # Set up context manager properly
        mock_engine.connect.return_value = mock_conn
        mock_conn.__enter__ = Mock(return_value=mock_conn)
        mock_conn.__exit__ = Mock(return_value=None)
        mock_conn.execute.side_effect = Exception("Query failed")
        connector.engine = mock_engine

        # Act
        result = connector.test_connection()

        # Assert
        assert result is False

    def test_test_connection_no_engine(self):
        """Test connection test with no engine."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")

        # Act
        result = connector.test_connection()

        # Assert
        assert result is False

    def test_get_table_info_query_with_schema(self):
        """Test table info query generation with schema."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")

        # Act
        query = connector._get_table_info_query("test_table", "test_schema")

        # Assert
        assert "test_table" in query
        assert "test_schema" in query
        assert "information_schema.columns" in query

    def test_get_table_info_query_without_schema(self):
        """Test table info query generation without schema."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")

        # Act
        query = connector._get_table_info_query("test_table")

        # Assert
        assert "test_table" in query
        assert "public" in query
        assert "information_schema.columns" in query

    def test_get_foreign_keys(self):
        """Test foreign keys retrieval."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_result = pd.DataFrame(
            [
                {
                    "column_name": "user_id",
                    "referenced_table": "users",
                    "referenced_column": "id",
                }
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            foreign_keys = connector.get_foreign_keys("orders")

            # Assert
            assert len(foreign_keys) == 1
            assert foreign_keys[0]["column_name"] == "user_id"
            assert foreign_keys[0]["referenced_table"] == "users"
            assert foreign_keys[0]["referenced_column"] == "id"

    def test_get_tables_list(self):
        """Test tables list retrieval."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_result = pd.DataFrame(
            [
                {
                    "table_name": "users",
                    "table_schema": "public",
                    "table_type": "BASE TABLE",
                },
                {
                    "table_name": "orders",
                    "table_schema": "public",
                    "table_type": "BASE TABLE",
                },
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            tables = connector.get_tables_list()

            # Assert
            assert len(tables) == 2
            assert tables[0]["table_name"] == "users"
            assert tables[1]["table_name"] == "orders"

    def test_get_tables_list_with_schema(self):
        """Test tables list retrieval with specific schema."""
        # Arrange
        connector = PostgreSQLConnector("postgresql://user:pass@localhost/db")
        mock_result = pd.DataFrame(
            [
                {
                    "table_name": "test_table",
                    "table_schema": "test_schema",
                    "table_type": "BASE TABLE",
                }
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            tables = connector.get_tables_list("test_schema")

            # Assert
            assert len(tables) == 1
            assert tables[0]["table_schema"] == "test_schema"
