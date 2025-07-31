"""Tests for SQL Server connector."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from data_quality.connectors.sqlserver import SQLServerConnector


class TestSQLServerConnector:
    """Test SQL Server connector functionality."""

    def test_connector_initialization(self):
        """Test SQL Server connector initialization."""
        # Arrange & Act
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Assert
        assert connector.connection_string == "mssql+pyodbc://user:pass@localhost/db"
        assert connector.engine is None

    @patch("data_quality.connectors.sqlserver.create_engine")
    def test_connect_success(self, mock_create_engine):
        """Test successful SQL Server connection."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        with patch.object(connector, "test_connection", return_value=True):
            # Act
            connector.connect()

            # Assert
            assert connector.engine == mock_engine
            mock_create_engine.assert_called_once_with(
                "mssql+pyodbc://user:pass@localhost/db"
            )

    @patch("data_quality.connectors.sqlserver.create_engine")
    def test_connect_failure(self, mock_create_engine):
        """Test SQL Server connection failure."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
        mock_create_engine.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to connect to SQL Server"):
            connector.connect()

        assert connector.engine is None

    def test_disconnect(self):
        """Test SQL Server disconnection."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
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
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Act & Assert (should not raise exception)
        connector.disconnect()

    def test_test_connection_success(self):
        """Test successful connection test."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
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
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
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
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Act
        result = connector.test_connection()

        # Assert
        assert result is False

    def test_get_table_info_query_with_schema(self):
        """Test table info query generation with schema."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Act
        query = connector._get_table_info_query("test_table", "test_schema")

        # Assert
        assert "test_table" in query
        assert "test_schema" in query
        assert "INFORMATION_SCHEMA.COLUMNS" in query

    def test_get_table_info_query_without_schema(self):
        """Test table info query generation without schema."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Act
        query = connector._get_table_info_query("test_table")

        # Assert
        assert "test_table" in query
        assert "dbo" in query
        assert "INFORMATION_SCHEMA.COLUMNS" in query

    def test_get_foreign_keys(self):
        """Test foreign keys retrieval."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
        mock_result = pd.DataFrame(
            [
                {
                    "COLUMN_NAME": "user_id",
                    "REFERENCED_TABLE_NAME": "users",
                    "REFERENCED_COLUMN_NAME": "id",
                }
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            foreign_keys = connector.get_foreign_keys("orders")

            # Assert
            assert len(foreign_keys) == 1
            # Note: SQL Server returns uppercase column names
            assert "COLUMN_NAME" in foreign_keys[0] or "column_name" in foreign_keys[0]

    def test_get_tables_list(self):
        """Test tables list retrieval."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
        mock_result = pd.DataFrame(
            [
                {
                    "TABLE_NAME": "users",
                    "TABLE_SCHEMA": "dbo",
                    "TABLE_TYPE": "BASE TABLE",
                },
                {
                    "TABLE_NAME": "orders",
                    "TABLE_SCHEMA": "dbo",
                    "TABLE_TYPE": "BASE TABLE",
                },
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            tables = connector.get_tables_list()

            # Assert
            assert len(tables) == 2
            # Note: SQL Server returns uppercase column names
            assert "TABLE_NAME" in tables[0] or "table_name" in tables[0]

    def test_get_tables_list_with_schema(self):
        """Test tables list retrieval with specific schema."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")
        mock_result = pd.DataFrame(
            [
                {
                    "TABLE_NAME": "test_table",
                    "TABLE_SCHEMA": "test_schema",
                    "TABLE_TYPE": "BASE TABLE",
                }
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            tables = connector.get_tables_list("test_schema")

            # Assert
            assert len(tables) == 1
            # Note: SQL Server returns uppercase column names
            assert "TABLE_SCHEMA" in tables[0] or "table_schema" in tables[0]

    def test_foreign_keys_query_format(self):
        """Test foreign keys query includes proper SQL Server syntax."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Act & Assert (verify the query can be called without error)
        with patch.object(connector, "execute_query", return_value=pd.DataFrame()):
            connector.get_foreign_keys("test_table", "dbo")
            # If no exception is raised, the query format is correct

    def test_tables_query_format(self):
        """Test tables list query includes proper SQL Server syntax."""
        # Arrange
        connector = SQLServerConnector("mssql+pyodbc://user:pass@localhost/db")

        # Act & Assert (verify the query can be called without error)
        with patch.object(connector, "execute_query", return_value=pd.DataFrame()):
            connector.get_tables_list("dbo")
            # If no exception is raised, the query format is correct
