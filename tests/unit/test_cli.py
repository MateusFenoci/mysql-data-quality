"""Tests for CLI module."""

import pandas as pd
from click.testing import CliRunner
from unittest.mock import Mock, patch
from pathlib import Path

from src.data_quality.cli import (
    main,
    _display_validation_results,
    _display_single_result,
)
from src.data_quality.validators.base import ValidationResult, ValidationSeverity


class TestCLI:
    """Test cases for CLI commands."""

    def test_main_help(self):
        """Test main command help."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(main, ["--help"])

        # Assert
        assert result.exit_code == 0
        assert "Data Quality validation and reporting tool" in result.output
        assert "analyze" in result.output
        assert "test-connection" in result.output
        assert "list-tables" in result.output
        assert "describe-table" in result.output
        assert "validate" in result.output

    def test_main_version(self):
        """Test main command version."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(main, ["--version"])

        # Assert
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_main_command_execution(self):
        """Test main command execution with Data Quality Tool message."""
        # Arrange
        runner = CliRunner()

        # Act - main without arguments shows help
        result = runner.invoke(main, ["--help"])

        # Assert
        assert result.exit_code == 0

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_test_connection_success(self, mock_factory, mock_load_config):
        """Test successful database connection test."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.driver = "mysql"
        mock_db_config.host = "localhost"
        mock_db_config.port = 3306
        mock_db_config.name = "testdb"
        mock_db_config.connection_string = "mysql://test"

        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.test_connection.return_value = True
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["test-connection"])

        # Assert
        assert result.exit_code == 0
        assert "Testing connection to mysql" in result.output
        assert "Host: localhost:3306" in result.output
        assert "Database: testdb" in result.output
        assert "Connection successful!" in result.output

        mock_factory.create_connector.assert_called_once_with("mysql://test", "mysql")
        mock_connector.connect.assert_called_once()
        mock_connector.test_connection.assert_called_once()
        mock_connector.disconnect.assert_called_once()

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_test_connection_failure(self, mock_factory, mock_load_config):
        """Test database connection test failure."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.driver = "mysql"
        mock_db_config.host = "localhost"
        mock_db_config.port = 3306
        mock_db_config.name = "testdb"
        mock_db_config.connection_string = "mysql://test"

        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.test_connection.return_value = False
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["test-connection"])

        # Assert
        assert result.exit_code == 0
        assert "Connection failed!" in result.output

    @patch("src.data_quality.cli.load_config")
    def test_test_connection_exception(self, mock_load_config):
        """Test database connection test with exception."""
        # Arrange
        runner = CliRunner()
        mock_load_config.side_effect = Exception("Config error")

        # Act
        result = runner.invoke(main, ["test-connection"])

        # Assert
        assert result.exit_code == 0
        assert "Error: Config error" in result.output

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_list_tables_without_real_count(self, mock_factory, mock_load_config):
        """Test list tables command without real count."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()

        # Mock tables query result
        tables_df = pd.DataFrame({"table_name": ["users", "orders", "products"]})

        # Mock estimates query result
        estimates_df = pd.DataFrame(
            {
                "table_name": ["users", "orders", "products"],
                "table_rows": [100, 250, 50],
            }
        )

        mock_connector.execute_query.side_effect = [tables_df, estimates_df]
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["list-tables"])

        # Assert
        assert result.exit_code == 0
        assert "Database Tables" in result.output
        assert "users" in result.output
        assert "orders" in result.output
        assert "products" in result.output
        assert "Use --real-count flag for accurate row counts" in result.output

        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()
        assert mock_connector.execute_query.call_count == 2

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_list_tables_with_real_count(self, mock_factory, mock_load_config):
        """Test list tables command with real count."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()

        # Mock tables query result
        tables_df = pd.DataFrame({"table_name": ["users", "orders"]})

        mock_connector.execute_query.return_value = tables_df
        mock_connector.get_table_count.side_effect = [150, 300]
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["list-tables", "--real-count"])

        # Assert
        assert result.exit_code == 0
        assert "Database Tables" in result.output
        assert "Real Row Count" in result.output
        assert "Getting real counts - this may take a while" in result.output
        assert "users" in result.output
        assert "orders" in result.output

        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()
        assert mock_connector.get_table_count.call_count == 2

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_list_tables_no_tables_found(self, mock_factory, mock_load_config):
        """Test list tables command when no tables found."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        empty_df = pd.DataFrame({"table_name": []})
        mock_connector.execute_query.return_value = empty_df
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["list-tables"])

        # Assert
        assert result.exit_code == 0
        assert "No tables found." in result.output

    @patch("src.data_quality.cli.load_config")
    def test_list_tables_exception(self, mock_load_config):
        """Test list tables command with exception."""
        # Arrange
        runner = CliRunner()
        mock_load_config.side_effect = Exception("Database error")

        # Act
        result = runner.invoke(main, ["list-tables"])

        # Assert
        assert result.exit_code == 0
        assert "Error: Database error" in result.output

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_describe_table_success(self, mock_factory, mock_load_config):
        """Test describe table command success."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()

        columns_info = [
            {
                "column_name": "id",
                "data_type": "integer",
                "is_nullable": "NO",
                "column_default": "nextval('users_id_seq')",
            },
            {
                "column_name": "name",
                "data_type": "varchar",
                "is_nullable": "YES",
                "column_default": None,
            },
        ]

        mock_connector.get_table_info.return_value = columns_info
        mock_connector.get_table_count.return_value = 1500
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["describe-table", "users"])

        # Assert
        assert result.exit_code == 0
        assert "Table: users" in result.output
        assert "Records: 1,500" in result.output
        assert "Column Information" in result.output
        assert "id" in result.output
        assert "name" in result.output
        assert "integer" in result.output
        assert "varchar" in result.output

        mock_connector.connect.assert_called_once()
        mock_connector.get_table_info.assert_called_once_with("users")
        mock_connector.get_table_count.assert_called_once_with("users")
        mock_connector.disconnect.assert_called_once()

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    def test_describe_table_no_columns(self, mock_factory, mock_load_config):
        """Test describe table command with no column info."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.get_table_info.return_value = []
        mock_connector.get_table_count.return_value = 0
        mock_factory.create_connector.return_value = mock_connector

        # Act
        result = runner.invoke(main, ["describe-table", "empty_table"])

        # Assert
        assert result.exit_code == 0
        assert "No column information found." in result.output

    @patch("src.data_quality.cli.load_config")
    def test_describe_table_exception(self, mock_load_config):
        """Test describe table command with exception."""
        # Arrange
        runner = CliRunner()
        mock_load_config.side_effect = Exception("Table error")

        # Act
        result = runner.invoke(main, ["describe-table", "test_table"])

        # Assert
        assert result.exit_code == 0
        assert "Error: Table error" in result.output

    def test_analyze_help(self):
        """Test analyze command help."""
        # Arrange
        runner = CliRunner()

        # Act
        result = runner.invoke(main, ["analyze", "--help"])

        # Assert
        assert result.exit_code == 0
        assert "Run complete data quality analysis" in result.output
        assert "--sample-size" in result.output
        assert "--validators" in result.output
        assert "--formats" in result.output
        assert "--separate-reports" in result.output

    @patch("src.data_quality.core.DataQualityOrchestrator")
    def test_analyze_command_basic(self, mock_orchestrator_class):
        """Test basic analyze command execution."""
        # Arrange
        runner = CliRunner()
        mock_orchestrator = Mock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.run_complete_analysis.return_value = {
            "html": Path("/path/to/report.html"),
            "json": Path("/path/to/report.json"),
        }

        # Act
        result = runner.invoke(main, ["analyze", "test_table"])

        # Assert
        assert result.exit_code == 0
        assert "Analysis completed successfully!" in result.output
        assert "Generated 2 report(s)" in result.output

        mock_orchestrator_class.assert_called_once_with("reports")
        mock_orchestrator.run_complete_analysis.assert_called_once()

        # Verify call arguments
        call_args = mock_orchestrator.run_complete_analysis.call_args
        assert call_args[1]["table_name"] == "test_table"
        assert call_args[1]["sample_size"] == 10000  # default
        assert call_args[1]["unified_reports"] is True  # default

    @patch("src.data_quality.core.DataQualityOrchestrator")
    def test_analyze_command_with_options(self, mock_orchestrator_class):
        """Test analyze command with various options."""
        # Arrange
        runner = CliRunner()
        mock_orchestrator = Mock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.run_complete_analysis.return_value = {
            "html": Path("/path/to/report.html")
        }

        # Act
        result = runner.invoke(
            main,
            [
                "analyze",
                "test_table",
                "--sample-size",
                "5000",
                "--validators",
                "completeness",
                "--validators",
                "duplicates",
                "--formats",
                "html",
                "--separate-reports",
                "--report-name",
                "custom_report",
                "--output-dir",
                "custom_output",
            ],
        )

        # Assert
        assert result.exit_code == 0
        mock_orchestrator_class.assert_called_once_with("custom_output")

        # Verify call arguments
        call_args = mock_orchestrator.run_complete_analysis.call_args
        assert call_args[1]["table_name"] == "test_table"
        assert call_args[1]["sample_size"] == 5000
        assert call_args[1]["validators"] == ["completeness", "duplicates"]
        assert call_args[1]["report_formats"] == ["html"]
        assert call_args[1]["unified_reports"] is False  # --separate-reports
        assert call_args[1]["report_name"] == "custom_report"

    @patch("src.data_quality.core.DataQualityOrchestrator")
    def test_analyze_command_failure(self, mock_orchestrator_class):
        """Test analyze command when orchestrator fails."""
        # Arrange
        runner = CliRunner()
        mock_orchestrator = Mock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.run_complete_analysis.return_value = {}  # Empty = failure

        # Act
        result = runner.invoke(main, ["analyze", "test_table"])

        # Assert
        assert result.exit_code == 0  # CLI doesn't exit with error, just prints message
        assert "Analysis failed" in result.output

    @patch("src.data_quality.core.DataQualityOrchestrator")
    def test_analyze_command_exception(self, mock_orchestrator_class):
        """Test analyze command when exception occurs."""
        # Arrange
        runner = CliRunner()
        mock_orchestrator_class.side_effect = Exception("Test error")

        # Act
        result = runner.invoke(main, ["analyze", "test_table"])

        # Assert
        assert result.exit_code == 0  # CLI handles exceptions
        assert "Error: Test error" in result.output

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    @patch("src.data_quality.validators.ValidationEngine")
    def test_validate_command_basic(
        self, mock_engine_class, mock_factory, mock_load_config
    ):
        """Test basic validate command execution."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.get_table_count.return_value = 100
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        mock_connector.execute_query.return_value = test_data
        mock_factory.create_connector.return_value = mock_connector

        mock_engine = Mock()
        mock_results = [
            Mock(
                rule_name="test_rule",
                passed=True,
                severity=Mock(value="INFO"),
                column_name="id",
                message="Test passed",
                affected_rows=0,
                total_rows=100,
                details={},
            )
        ]
        mock_engine.validate_data.return_value = mock_results
        mock_engine_class.return_value = mock_engine

        # Act
        result = runner.invoke(main, ["validate", "test_table"])

        # Assert
        assert result.exit_code == 0
        assert "Running data quality validations" in result.output
        assert "Dataset: 3 rows × 2 columns" in result.output

        mock_connector.connect.assert_called_once()
        mock_connector.disconnect.assert_called_once()
        mock_engine.validate_data.assert_called_once()

    @patch("src.data_quality.cli.load_config")
    @patch("src.data_quality.cli.DatabaseConnectorFactory")
    @patch("src.data_quality.validators.ValidationEngine")
    @patch("src.data_quality.reports.HTMLReportGenerator")
    @patch("src.data_quality.reports.JSONReportGenerator")
    def test_validate_command_with_reports(
        self,
        mock_json_gen,
        mock_html_gen,
        mock_engine_class,
        mock_factory,
        mock_load_config,
    ):
        """Test validate command with report generation."""
        # Arrange
        runner = CliRunner()

        mock_db_config = Mock()
        mock_db_config.connection_string = "mysql://test"
        mock_db_config.driver = "mysql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.get_table_count.return_value = 5000  # Larger than sample
        test_data = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
        mock_connector.execute_query.return_value = test_data
        mock_factory.create_connector.return_value = mock_connector

        mock_engine = Mock()
        mock_results = [Mock()]
        mock_engine.validate_data.return_value = mock_results
        mock_engine_class.return_value = mock_engine

        # Mock report generators
        mock_html_generator = Mock()
        mock_html_generator.generate_report.return_value = Path("/path/to/report.html")
        mock_html_gen.return_value = mock_html_generator

        mock_json_generator = Mock()
        mock_json_generator.generate_report.return_value = Path("/path/to/report.json")
        mock_json_gen.return_value = mock_json_generator

        # Act
        result = runner.invoke(
            main,
            [
                "validate",
                "test_table",
                "--report-format",
                "html",
                "--report-format",
                "json",
                "--sample-size",
                "1000",
            ],
        )

        # Assert
        assert result.exit_code == 0
        assert "Table has 5,000 rows. Using sample of 1,000 rows." in result.output
        assert "Generating reports..." in result.output
        assert "Reports generated successfully!" in result.output
        assert "HTML: /path/to/report.html" in result.output
        assert "JSON: /path/to/report.json" in result.output

    @patch("src.data_quality.cli.load_config")
    def test_validate_command_exception(self, mock_load_config):
        """Test validate command with exception."""
        # Arrange
        runner = CliRunner()
        mock_load_config.side_effect = Exception("Validation error")

        # Act
        result = runner.invoke(main, ["validate", "test_table"])

        # Assert
        assert result.exit_code == 0
        assert "Error: Validation error" in result.output

    def test_display_validation_results(self):
        """Test display validation results function."""
        # Arrange
        results = [
            ValidationResult(
                rule_name="critical_rule",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.CRITICAL,
                passed=False,
                message="Critical issue",
                details={},
                timestamp=None,
                affected_rows=50,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="error_rule",
                table_name="test_table",
                column_name="col2",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Error issue",
                details={},
                timestamp=None,
                affected_rows=25,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="warning_rule",
                table_name="test_table",
                column_name="col3",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message="Warning issue",
                details={},
                timestamp=None,
                affected_rows=10,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="info_rule",
                table_name="test_table",
                column_name="col4",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="Info message",
                details={},
                timestamp=None,
                affected_rows=0,
                total_rows=100,
            ),
        ]

        # Act & Assert - should not raise exception
        _display_validation_results(results)

    def test_display_single_result_passed(self):
        """Test display single result for passed validation."""
        # Arrange
        result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            column_name="test_col",
            severity=ValidationSeverity.INFO,
            passed=True,
            message="Test passed",
            details={"completeness_ratio": 0.95},
            timestamp=None,
            affected_rows=0,
            total_rows=100,
        )

        # Act & Assert - should not raise exception
        _display_single_result(result)

    def test_display_single_result_failed_with_details(self):
        """Test display single result for failed validation with details."""
        # Arrange
        result = ValidationResult(
            rule_name="completeness_check",
            table_name="test_table",
            column_name="name",
            severity=ValidationSeverity.ERROR,
            passed=False,
            message="Missing values found",
            details={
                "completeness_ratio": 0.8,
                "duplicate_count": 5,
                "duplicate_values": ["value1", "value2", "value3", "value4"],
            },
            timestamp=None,
            affected_rows=20,
            total_rows=100,
        )

        # Act & Assert - should not raise exception
        _display_single_result(result)

    def test_display_single_result_no_column(self):
        """Test display single result without column name."""
        # Arrange
        result = ValidationResult(
            rule_name="table_rule",
            table_name="test_table",
            column_name=None,
            severity=ValidationSeverity.WARNING,
            passed=False,
            message="Table issue",
            details={},
            timestamp=None,
            affected_rows=0,
            total_rows=100,
        )

        # Act & Assert - should not raise exception
        _display_single_result(result)
