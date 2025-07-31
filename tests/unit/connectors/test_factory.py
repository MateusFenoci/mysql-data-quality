"""Tests for database connector factory."""

import pytest

from data_quality.connectors.factory import DatabaseConnectorFactory
from data_quality.connectors.base import DatabaseConnector
from data_quality.connectors.mysql import MySQLConnector
from data_quality.connectors.postgresql import PostgreSQLConnector
from data_quality.connectors.sqlserver import SQLServerConnector
from data_quality.connectors.oracle import OracleConnector
from data_quality.connectors.sqlite import SQLiteConnector


class MockConnector(DatabaseConnector):
    """Mock connector for testing."""

    def connect(self) -> None:
        """Mock implementation."""
        pass

    def disconnect(self) -> None:
        """Mock implementation."""
        pass

    def test_connection(self) -> bool:
        """Mock implementation."""
        return True

    def _get_table_info_query(self, table_name: str, schema=None) -> str:
        """Mock implementation."""
        return f"SELECT * FROM {table_name}"


class TestDatabaseConnectorFactory:
    """Test cases for DatabaseConnectorFactory."""

    def test_create_connector_mysql(self):
        """Test creating MySQL connector."""
        # Arrange
        connection_string = "mysql://user:pass@host:3306/db"
        driver = "mysql"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, MySQLConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_mariadb(self):
        """Test creating MariaDB connector (should use MySQL connector)."""
        # Arrange
        connection_string = "mysql://user:pass@host:3306/db"
        driver = "mariadb"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, MySQLConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_postgresql(self):
        """Test creating PostgreSQL connector."""
        # Arrange
        connection_string = "postgresql://user:pass@host:5432/db"
        driver = "postgresql"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, PostgreSQLConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_postgres(self):
        """Test creating PostgreSQL connector with 'postgres' alias."""
        # Arrange
        connection_string = "postgresql://user:pass@host:5432/db"
        driver = "postgres"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, PostgreSQLConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_sqlserver(self):
        """Test creating SQL Server connector."""
        # Arrange
        connection_string = "mssql+pyodbc://user:pass@host/db"
        driver = "sqlserver"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, SQLServerConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_mssql(self):
        """Test creating SQL Server connector with 'mssql' alias."""
        # Arrange
        connection_string = "mssql+pyodbc://user:pass@host/db"
        driver = "mssql"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, SQLServerConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_oracle(self):
        """Test creating Oracle connector."""
        # Arrange
        connection_string = "oracle+cx_oracle://user:pass@host:1521/xe"
        driver = "oracle"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, OracleConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_sqlite(self):
        """Test creating SQLite connector."""
        # Arrange
        connection_string = "sqlite:///test.db"
        driver = "sqlite"

        # Act
        connector = DatabaseConnectorFactory.create_connector(connection_string, driver)

        # Assert
        assert isinstance(connector, SQLiteConnector)
        assert connector.connection_string == connection_string

    def test_create_connector_unsupported_driver(self):
        """Test creating connector with unsupported driver."""
        # Arrange
        connection_string = "unknown://user:pass@host:5432/db"
        driver = "unknown"

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported database driver: unknown"):
            DatabaseConnectorFactory.create_connector(connection_string, driver)

    def test_register_connector(self):
        """Test registering a new connector type."""
        # Arrange
        original_connectors = DatabaseConnectorFactory._connectors.copy()

        # Act
        DatabaseConnectorFactory.register_connector("mock", MockConnector)

        # Assert
        assert "mock" in DatabaseConnectorFactory._connectors
        assert DatabaseConnectorFactory._connectors["mock"] == MockConnector

        # Test creating the registered connector
        connection_string = "mock://test"
        connector = DatabaseConnectorFactory.create_connector(connection_string, "mock")
        assert isinstance(connector, MockConnector)
        assert connector.connection_string == connection_string

        # Cleanup - restore original connectors
        DatabaseConnectorFactory._connectors = original_connectors

    def test_register_connector_override_existing(self):
        """Test overriding an existing connector type."""
        # Arrange
        original_connectors = DatabaseConnectorFactory._connectors.copy()

        # Act - Override mysql with mock connector
        DatabaseConnectorFactory.register_connector("mysql", MockConnector)

        # Assert
        assert DatabaseConnectorFactory._connectors["mysql"] == MockConnector

        # Test creating the overridden connector
        connection_string = "mysql://test"
        connector = DatabaseConnectorFactory.create_connector(
            connection_string, "mysql"
        )
        assert isinstance(connector, MockConnector)

        # Cleanup - restore original connectors
        DatabaseConnectorFactory._connectors = original_connectors

    def test_get_supported_drivers(self):
        """Test getting list of supported drivers."""
        # Act
        drivers = DatabaseConnectorFactory.get_supported_drivers()

        # Assert
        assert isinstance(drivers, list)
        assert "mysql" in drivers
        assert len(drivers) >= 1

    def test_get_supported_drivers_after_registration(self):
        """Test getting supported drivers after registering new one."""
        # Arrange
        original_connectors = DatabaseConnectorFactory._connectors.copy()
        original_drivers = DatabaseConnectorFactory.get_supported_drivers()

        # Act
        DatabaseConnectorFactory.register_connector("mock", MockConnector)
        new_drivers = DatabaseConnectorFactory.get_supported_drivers()

        # Assert
        assert len(new_drivers) == len(original_drivers) + 1
        assert "mock" in new_drivers
        assert all(driver in new_drivers for driver in original_drivers)

        # Cleanup - restore original connectors
        DatabaseConnectorFactory._connectors = original_connectors

    def test_connectors_class_attribute_integrity(self):
        """Test that _connectors class attribute maintains integrity."""
        # Arrange
        original_connectors = DatabaseConnectorFactory._connectors.copy()

        # Act - Modify the class attribute directly
        DatabaseConnectorFactory._connectors["test"] = MockConnector

        # Assert
        assert "test" in DatabaseConnectorFactory._connectors
        assert DatabaseConnectorFactory._connectors["test"] == MockConnector

        # Test that original connectors are still there
        assert "mysql" in DatabaseConnectorFactory._connectors

        # Cleanup
        DatabaseConnectorFactory._connectors = original_connectors

    def test_factory_creates_different_instances(self):
        """Test that factory creates different instances for same driver."""
        # Arrange
        connection_string1 = "mysql://user1:pass1@host1:3306/db1"
        connection_string2 = "mysql://user2:pass2@host2:3306/db2"

        # Act
        connector1 = DatabaseConnectorFactory.create_connector(
            connection_string1, "mysql"
        )
        connector2 = DatabaseConnectorFactory.create_connector(
            connection_string2, "mysql"
        )

        # Assert
        assert connector1 is not connector2
        assert isinstance(connector1, MySQLConnector)
        assert isinstance(connector2, MySQLConnector)
        assert connector1.connection_string == connection_string1
        assert connector2.connection_string == connection_string2

    def test_create_connector_with_empty_driver(self):
        """Test creating connector with empty driver string."""
        # Arrange
        connection_string = "mysql://user:pass@host:3306/db"
        driver = ""

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported database driver: "):
            DatabaseConnectorFactory.create_connector(connection_string, driver)

    def test_create_connector_with_none_driver(self):
        """Test creating connector with None driver."""
        # Arrange
        connection_string = "mysql://user:pass@host:3306/db"
        driver = None

        # Act & Assert
        with pytest.raises(ValueError, match="Unsupported database driver: None"):
            DatabaseConnectorFactory.create_connector(connection_string, driver)

    def test_register_connector_with_invalid_class(self):
        """Test registering connector with non-DatabaseConnector class."""

        # Arrange
        class InvalidConnector:
            """Not a DatabaseConnector subclass."""

            pass

        original_connectors = DatabaseConnectorFactory._connectors.copy()

        # Act - This should work (factory doesn't validate inheritance at registration)
        DatabaseConnectorFactory.register_connector("invalid", InvalidConnector)

        # But creation should fail when instantiating
        with pytest.raises(TypeError):
            # InvalidConnector doesn't accept connection_string parameter
            DatabaseConnectorFactory.create_connector("test://conn", "invalid")

        # Cleanup
        DatabaseConnectorFactory._connectors = original_connectors

    def test_factory_case_sensitivity(self):
        """Test that driver names are case sensitive."""
        # Arrange
        connection_string = "mysql://user:pass@host:3306/db"

        # Act & Assert
        # Correct case should work
        connector = DatabaseConnectorFactory.create_connector(
            connection_string, "mysql"
        )
        assert isinstance(connector, MySQLConnector)

        # Wrong case should fail
        with pytest.raises(ValueError, match="Unsupported database driver: MySQL"):
            DatabaseConnectorFactory.create_connector(connection_string, "MySQL")

        with pytest.raises(ValueError, match="Unsupported database driver: MYSQL"):
            DatabaseConnectorFactory.create_connector(connection_string, "MYSQL")
