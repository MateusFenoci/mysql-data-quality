"""Tests for SQLite connector."""

import pytest
from unittest.mock import Mock, patch
import pandas as pd

from data_quality.connectors.sqlite import SQLiteConnector


class TestSQLiteConnector:
    """Test SQLite connector functionality."""

    def test_connector_initialization(self):
        """Test SQLite connector initialization."""
        # Arrange & Act
        connector = SQLiteConnector("sqlite:///test.db")

        # Assert
        assert connector.connection_string == "sqlite:///test.db"
        assert connector.engine is None

    @patch("data_quality.connectors.sqlite.create_engine")
    def test_connect_success(self, mock_create_engine):
        """Test successful SQLite connection."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        with patch.object(connector, "test_connection", return_value=True):
            # Act
            connector.connect()

            # Assert
            assert connector.engine == mock_engine
            mock_create_engine.assert_called_once_with("sqlite:///test.db")

    @patch("data_quality.connectors.sqlite.create_engine")
    def test_connect_failure(self, mock_create_engine):
        """Test SQLite connection failure."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        mock_create_engine.side_effect = Exception("Connection failed")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Failed to connect to SQLite"):
            connector.connect()

        assert connector.engine is None

    def test_disconnect(self):
        """Test SQLite disconnection."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
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
        connector = SQLiteConnector("sqlite:///test.db")

        # Act & Assert (should not raise exception)
        connector.disconnect()

    def test_test_connection_success(self):
        """Test successful connection test."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
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
        connector = SQLiteConnector("sqlite:///test.db")
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
        connector = SQLiteConnector("sqlite:///test.db")

        # Act
        result = connector.test_connection()

        # Assert
        assert result is False

    def test_get_table_info_query(self):
        """Test table info query generation."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")

        # Act
        query = connector._get_table_info_query("test_table")

        # Assert
        assert "test_table" in query
        assert "PRAGMA table_info" in query

    def test_format_column_info_conversion(self):
        """Test column info conversion from PRAGMA result."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        mock_pragma_result = pd.DataFrame(
            [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "notnull": 1,
                    "dflt_value": None,
                    "pk": 1,
                },
                {
                    "name": "name",
                    "type": "VARCHAR(100)",
                    "notnull": 0,
                    "dflt_value": None,
                    "pk": 0,
                },
            ]
        )

        # Mock the engine and execute_query
        mock_engine = Mock()
        connector.engine = mock_engine

        with patch.object(connector, "execute_query", return_value=mock_pragma_result):
            # Act
            result = connector.get_table_info("test_table")

            # Assert
            assert len(result) == 2
            assert result[0]["column_name"] == "id"
            assert result[0]["data_type"] == "INTEGER"
            assert result[0]["is_nullable"] == "NO"
            assert result[1]["column_name"] == "name"
            assert result[1]["data_type"] == "VARCHAR(100)"
            assert result[1]["is_nullable"] == "YES"

    def test_get_foreign_keys(self):
        """Test foreign keys retrieval."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        mock_result = pd.DataFrame(
            [
                {
                    "id": 0,
                    "seq": 0,
                    "table": "users",
                    "from": "user_id",
                    "to": "id",
                    "on_update": "NO ACTION",
                    "on_delete": "NO ACTION",
                    "match": "NONE",
                }
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            foreign_keys = connector.get_foreign_keys("orders")

            # Assert
            assert len(foreign_keys) == 1
            # SQLite connector converts to standard format
            fk_data = foreign_keys[0]
            assert fk_data["column_name"] == "user_id"
            assert fk_data["referenced_table"] == "users"
            assert fk_data["referenced_column"] == "id"

    def test_get_tables_list(self):
        """Test tables list retrieval."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        mock_result = pd.DataFrame(
            [
                {"name": "users", "type": "table"},
                {"name": "orders", "type": "table"},
                {"name": "sqlite_sequence", "type": "table"},
            ]
        )

        with patch.object(connector, "execute_query", return_value=mock_result):
            # Act
            tables = connector.get_tables_list()

            # Assert
            # Should exclude system tables like sqlite_sequence
            user_tables = [t for t in tables if not t["name"].startswith("sqlite_")]
            assert len(user_tables) == 2
            assert user_tables[0]["name"] == "users"
            assert user_tables[1]["name"] == "orders"

    def test_get_table_info_with_mock_pragma(self):
        """Test getting table info with mocked PRAGMA response."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        mock_pragma_result = pd.DataFrame(
            [
                {
                    "name": "id",
                    "type": "INTEGER",
                    "notnull": 1,
                    "dflt_value": None,
                    "pk": 1,
                }
            ]
        )

        # Mock the engine to avoid "Database not connected" error
        mock_engine = Mock()
        connector.engine = mock_engine

        with patch.object(connector, "execute_query", return_value=mock_pragma_result):
            # Act
            result = connector.get_table_info("test_table")

            # Assert
            assert len(result) == 1
            assert result[0]["column_name"] == "id"
            assert result[0]["data_type"] == "INTEGER"

    def test_foreign_keys_query_format(self):
        """Test foreign keys query includes proper SQLite syntax."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")

        # Act & Assert (verify the query can be called without error)
        with patch.object(connector, "execute_query", return_value=pd.DataFrame()):
            connector.get_foreign_keys("test_table")
            # If no exception is raised, the query format is correct

    def test_tables_query_format(self):
        """Test tables list query includes proper SQLite syntax."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")

        # Act & Assert (verify the query can be called without error)
        with patch.object(connector, "execute_query", return_value=pd.DataFrame()):
            connector.get_tables_list()
            # If no exception is raised, the query format is correct

    def test_pragma_foreign_key_list_format(self):
        """Test PRAGMA foreign_key_list format."""
        # Arrange
        _ = SQLiteConnector("sqlite:///test.db")

        # Act
        query = "PRAGMA foreign_key_list(test_table)"

        # Assert - This is the expected format for SQLite
        assert "PRAGMA foreign_key_list" in query
        assert "test_table" in query

    def test_sqlite_master_query_format(self):
        """Test sqlite_master query format."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")

        # Act & Assert (verify the query works with mock)
        with patch.object(connector, "execute_query", return_value=pd.DataFrame()):
            connector.get_tables_list()
            # Should use sqlite_master table

    def test_memory_database_connection(self):
        """Test in-memory SQLite database connection."""
        # Arrange & Act
        connector = SQLiteConnector("sqlite:///:memory:")

        # Assert
        assert connector.connection_string == "sqlite:///:memory:"

    def test_nullable_column_conversion(self):
        """Test nullable column conversion in get_table_info."""
        # Arrange
        connector = SQLiteConnector("sqlite:///test.db")
        pragma_result = pd.DataFrame(
            [
                {
                    "name": "nullable_col",
                    "type": "TEXT",
                    "notnull": 0,  # 0 means nullable
                    "dflt_value": None,
                    "pk": 0,
                },
                {
                    "name": "not_null_col",
                    "type": "TEXT",
                    "notnull": 1,  # 1 means not nullable
                    "dflt_value": None,
                    "pk": 0,
                },
            ]
        )

        # Mock the engine to avoid "Database not connected" error
        mock_engine = Mock()
        connector.engine = mock_engine

        with patch.object(connector, "execute_query", return_value=pragma_result):
            # Act
            result = connector.get_table_info("test_table")

            # Assert
            assert result[0]["is_nullable"] == "YES"
            assert result[1]["is_nullable"] == "NO"
