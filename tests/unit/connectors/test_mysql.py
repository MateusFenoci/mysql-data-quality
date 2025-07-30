"""Tests for MySQL connector."""

import pytest
from unittest.mock import Mock, patch
from sqlalchemy.exc import SQLAlchemyError

from data_quality.connectors.mysql import MySQLConnector


class TestMySQLConnector:
    """Test cases for MySQLConnector."""

    def test_init(self):
        """Test MySQL connector initialization."""
        # Arrange
        connection_string = "mysql://user:pass@host:3306/db"

        # Act
        connector = MySQLConnector(connection_string)

        # Assert
        assert connector.connection_string == connection_string
        assert connector.engine is None

    @patch("data_quality.connectors.mysql.create_engine")
    def test_connect_success(self, mock_create_engine):
        """Test successful MySQL connection."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # Mock test_connection to return True
        connector.test_connection = Mock(return_value=True)

        # Act
        connector.connect()

        # Assert
        assert connector.engine == mock_engine
        mock_create_engine.assert_called_once_with("mysql://user:pass@host:3306/db")
        connector.test_connection.assert_called_once()

    @patch("data_quality.connectors.mysql.create_engine")
    def test_connect_failure(self, mock_create_engine):
        """Test MySQL connection failure."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")
        mock_create_engine.side_effect = SQLAlchemyError("Connection failed")

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Failed to connect to MySQL/MariaDB: Connection failed"
        ):
            connector.connect()

        assert connector.engine is None

    @patch("data_quality.connectors.mysql.create_engine")
    def test_connect_test_connection_failure(self, mock_create_engine):
        """Test MySQL connection when test_connection fails."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # Mock test_connection to raise exception
        connector.test_connection = Mock(side_effect=Exception("Test failed"))

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Failed to connect to MySQL/MariaDB: Test failed"
        ):
            connector.connect()

        assert connector.engine is None

    def test_disconnect_with_engine(self):
        """Test MySQL disconnection when engine exists."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")
        mock_engine = Mock()
        connector.engine = mock_engine

        # Act
        connector.disconnect()

        # Assert
        mock_engine.dispose.assert_called_once()
        assert connector.engine is None

    def test_disconnect_without_engine(self):
        """Test MySQL disconnection when no engine."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")

        # Act (should not raise error)
        connector.disconnect()

        # Assert
        assert connector.engine is None

    def test_test_connection_no_engine(self):
        """Test connection test when no engine."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")

        # Act
        result = connector.test_connection()

        # Assert
        assert result is False

    @patch("data_quality.connectors.mysql.text")
    @patch("builtins.print")  # Mock print to avoid output during tests
    def test_test_connection_success(self, mock_print, mock_text):
        """Test successful connection test."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")

        # Mock connection and result
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=None)

        mock_engine = Mock()
        mock_engine.connect.return_value = mock_connection
        connector.engine = mock_engine

        mock_text.return_value = "SELECT 1"

        # Act
        result = connector.test_connection()

        # Assert
        assert result is True
        mock_text.assert_called_once_with("SELECT 1")
        mock_connection.execute.assert_called_once_with("SELECT 1")
        mock_result.fetchone.assert_called_once()

    @patch("data_quality.connectors.mysql.text")
    @patch("builtins.print")  # Mock print to capture debug output
    def test_test_connection_failure(self, mock_print, mock_text):
        """Test connection test failure."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")

        mock_connection = Mock()
        mock_connection.execute.side_effect = SQLAlchemyError("Connection test failed")
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=None)

        mock_engine = Mock()
        mock_engine.connect.return_value = mock_connection
        connector.engine = mock_engine

        # Act
        result = connector.test_connection()

        # Assert
        assert result is False
        mock_print.assert_called_once()  # Debug message should be printed

    def test_get_table_info_query_without_schema(self):
        """Test table info query generation without schema."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")

        # Act
        query = connector._get_table_info_query("test_table")

        # Assert
        assert "test_table" in query
        assert "information_schema.columns" in query
        assert "table_schema = DATABASE()" in query
        assert "column_name" in query
        assert "data_type" in query
        assert "is_nullable" in query
        assert "column_default" in query
        assert "character_maximum_length" in query
        assert "numeric_precision" in query
        assert "numeric_scale" in query
        assert "ORDER BY ordinal_position" in query

    def test_get_table_info_query_with_schema(self):
        """Test table info query generation with schema."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")

        # Act
        query = connector._get_table_info_query("test_table", "myschema")

        # Assert
        assert "test_table" in query
        assert "information_schema.columns" in query
        assert "table_schema = 'myschema'" in query
        assert "DATABASE()" not in query
        assert "column_name" in query
        assert "data_type" in query
        assert "is_nullable" in query
        assert "column_default" in query
        assert "character_maximum_length" in query
        assert "numeric_precision" in query
        assert "numeric_scale" in query
        assert "ORDER BY ordinal_position" in query

    @patch("data_quality.connectors.mysql.create_engine")
    def test_full_connection_lifecycle(self, mock_create_engine):
        """Test complete connection lifecycle."""
        # Arrange
        connector = MySQLConnector("mysql://user:pass@host:3306/db")
        mock_engine = Mock()
        mock_create_engine.return_value = mock_engine

        # Mock successful connection test
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchone.return_value = (1,)
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=None)
        mock_engine.connect.return_value = mock_connection

        # Act - Connect
        connector.connect()

        # Assert - Connection established
        assert connector.engine == mock_engine
        assert connector.test_connection() is True

        # Act - Disconnect
        connector.disconnect()

        # Assert - Connection closed
        mock_engine.dispose.assert_called_once()
        assert connector.engine is None
        assert connector.test_connection() is False

    def test_connection_string_variations(self):
        """Test connector with various connection string formats."""
        # Test different connection strings
        connection_strings = [
            "mysql://user:pass@localhost:3306/db",
            "mysql+pymysql://user:pass@host:3306/db",
            "mariadb://user:pass@host:3306/db",
        ]

        for conn_str in connection_strings:
            connector = MySQLConnector(conn_str)
            assert connector.connection_string == conn_str
            assert connector.engine is None
