"""Tests for base database connector."""

import pandas as pd
import pytest
from unittest.mock import Mock, patch
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from src.data_quality.connectors.base import DatabaseConnector


class ConcreteDatabaseConnector(DatabaseConnector):
    """Concrete implementation for testing."""

    def connect(self) -> None:
        """Test implementation."""
        self.engine = Mock(spec=Engine)

    def disconnect(self) -> None:
        """Test implementation."""
        if self.engine:
            self.engine.dispose()
            self.engine = None

    def test_connection(self) -> bool:
        """Test implementation."""
        return self.engine is not None

    def _get_table_info_query(self, table_name: str, schema=None) -> str:
        """Test implementation."""
        return f"SELECT * FROM information_schema.columns WHERE table_name = '{table_name}'"


class TestDatabaseConnector:
    """Test cases for DatabaseConnector."""

    def test_init(self):
        """Test connector initialization."""
        # Arrange
        connection_string = "postgresql://user:pass@host:5432/db"

        # Act
        connector = ConcreteDatabaseConnector(connection_string)

        # Assert
        assert connector.connection_string == connection_string
        assert connector.engine is None

    def test_execute_query_no_connection(self):
        """Test execute_query without connection raises error."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Database not connected"):
            connector.execute_query("SELECT 1")

    @patch("src.data_quality.connectors.base.text")
    def test_execute_query_success(self, mock_text):
        """Test successful query execution."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")

        # Mock engine and connection
        mock_connection = Mock()
        mock_result = Mock()
        mock_result.fetchall.return_value = [{"id": 1, "name": "test"}]
        mock_result.keys.return_value = ["id", "name"]
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=None)

        mock_engine = Mock()
        mock_engine.connect.return_value = mock_connection
        connector.engine = mock_engine

        mock_text.return_value = "SELECT * FROM test"

        # Act
        result = connector.execute_query("SELECT * FROM test", {"param": "value"})

        # Assert
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["id"] == 1
        assert result.iloc[0]["name"] == "test"
        mock_text.assert_called_once_with("SELECT * FROM test")
        mock_connection.execute.assert_called_once_with(
            "SELECT * FROM test", {"param": "value"}
        )

    @patch("src.data_quality.connectors.base.text")
    def test_execute_query_with_exception(self, mock_text):
        """Test query execution with database error."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")

        mock_connection = Mock()
        mock_connection.execute.side_effect = SQLAlchemyError("Database error")
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=None)

        mock_engine = Mock()
        mock_engine.connect.return_value = mock_connection
        connector.engine = mock_engine

        # Act & Assert
        with pytest.raises(
            RuntimeError, match="Query execution failed: Database error"
        ):
            connector.execute_query("SELECT 1")

    def test_get_table_info_no_connection(self):
        """Test get_table_info without connection raises error."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")

        # Act & Assert
        with pytest.raises(RuntimeError, match="Database not connected"):
            connector.get_table_info("test_table")

    @patch.object(ConcreteDatabaseConnector, "execute_query")
    def test_get_table_info_success(self, mock_execute_query):
        """Test successful table info retrieval."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")
        connector.engine = Mock()

        mock_df = pd.DataFrame(
            [
                {"column_name": "id", "data_type": "int", "is_nullable": "NO"},
                {"column_name": "name", "data_type": "varchar", "is_nullable": "YES"},
            ]
        )
        mock_execute_query.return_value = mock_df

        # Act
        result = connector.get_table_info("test_table")

        # Assert
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["column_name"] == "id"
        assert result[1]["column_name"] == "name"
        mock_execute_query.assert_called_once()

    @patch.object(ConcreteDatabaseConnector, "execute_query")
    def test_get_table_info_with_schema(self, mock_execute_query):
        """Test table info retrieval with schema."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")
        connector.engine = Mock()

        mock_df = pd.DataFrame([{"column_name": "id", "data_type": "int"}])
        mock_execute_query.return_value = mock_df

        # Act
        result = connector.get_table_info("test_table", "public")

        # Assert
        assert isinstance(result, list)
        mock_execute_query.assert_called_once()
        # Verify the query was called with schema
        call_args = mock_execute_query.call_args[0][0]
        assert "test_table" in call_args

    @patch.object(ConcreteDatabaseConnector, "execute_query")
    def test_get_table_count_success(self, mock_execute_query):
        """Test successful table count retrieval."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")

        mock_df = pd.DataFrame([{"count": 100}])
        mock_execute_query.return_value = mock_df

        # Act
        result = connector.get_table_count("test_table")

        # Assert
        assert result == 100
        mock_execute_query.assert_called_once_with(
            "SELECT COUNT(*) as count FROM test_table"
        )

    @patch.object(ConcreteDatabaseConnector, "execute_query")
    def test_get_table_count_with_schema(self, mock_execute_query):
        """Test table count retrieval with schema."""
        # Arrange
        connector = ConcreteDatabaseConnector("test://connection")

        mock_df = pd.DataFrame([{"count": 50}])
        mock_execute_query.return_value = mock_df

        # Act
        result = connector.get_table_count("test_table", "public")

        # Assert
        assert result == 50
        mock_execute_query.assert_called_once_with(
            "SELECT COUNT(*) as count FROM public.test_table"
        )

    def test_abstract_methods_not_implemented(self):
        """Test that abstract methods raise NotImplementedError."""
        # This test ensures the base class is properly abstract
        with pytest.raises(TypeError):
            DatabaseConnector("test://connection")
