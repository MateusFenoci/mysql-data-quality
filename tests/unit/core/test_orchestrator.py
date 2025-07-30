"""Tests for DataQualityOrchestrator."""

import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch

from data_quality.core.orchestrator import DataQualityOrchestrator
from data_quality.core.data_analyzer import DataAnalyzer
from data_quality.core.report_manager import ReportManager
from data_quality.core.volumetry_calculator import VolumetryCalculator
from data_quality.validators.base import ValidationResult, ValidationSeverity


class TestDataQualityOrchestrator:
    """Test cases for DataQualityOrchestrator."""

    @patch("data_quality.core.orchestrator.load_config")
    def test_init_default_components(self, mock_load_config):
        """Test orchestrator initialization with default components."""
        # Arrange
        mock_config = {
            "database": Mock(
                host="localhost",
                port=5432,
                name="test",
                driver="postgresql",
                connection_string="postgresql://test",
            )
        }
        mock_load_config.return_value = mock_config

        # Act
        orchestrator = DataQualityOrchestrator()

        # Assert
        assert orchestrator.config == mock_config
        assert orchestrator.db_config == mock_config["database"]
        assert isinstance(orchestrator.analyzer, DataAnalyzer)
        assert isinstance(orchestrator.report_manager, ReportManager)
        assert isinstance(orchestrator.volumetry_calculator, VolumetryCalculator)
        assert orchestrator.connector is None

        # Verify validators are registered (IntegrityValidator only registered after connection)
        assert len(orchestrator.analyzer.engine._validators) == 3
        assert "completeness" in orchestrator.analyzer.engine._validators
        assert "duplicates" in orchestrator.analyzer.engine._validators
        assert "patterns" in orchestrator.analyzer.engine._validators

    @patch("data_quality.core.orchestrator.load_config")
    def test_init_custom_components(self, mock_load_config):
        """Test orchestrator initialization with custom components."""
        # Arrange
        mock_config = {
            "database": Mock(connection_string="postgresql://test", driver="postgresql")
        }
        mock_load_config.return_value = mock_config

        custom_analyzer = Mock(spec=DataAnalyzer)
        custom_report_manager = Mock(spec=ReportManager)
        custom_volumetry_calculator = Mock(spec=VolumetryCalculator)

        # Act
        orchestrator = DataQualityOrchestrator(
            output_dir="custom_reports",
            analyzer=custom_analyzer,
            report_manager=custom_report_manager,
            volumetry_calculator=custom_volumetry_calculator,
        )

        # Assert
        assert orchestrator.analyzer == custom_analyzer
        assert orchestrator.report_manager == custom_report_manager
        assert orchestrator.volumetry_calculator == custom_volumetry_calculator

        # Verify _register_validators is called on custom analyzer (only basic validators, no IntegrityValidator)
        custom_analyzer.register_validator.assert_called()
        assert custom_analyzer.register_validator.call_count == 3

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.console")
    @patch("data_quality.core.orchestrator.sys")
    def test_init_config_error(self, mock_sys, mock_console, mock_load_config):
        """Test orchestrator initialization with config error."""
        # Arrange
        mock_load_config.side_effect = Exception("Config error")

        # Act
        DataQualityOrchestrator()

        # Assert
        mock_console.print.assert_called_with(
            "❌ [bold red]Configuration error: Config error[/bold red]"
        )
        mock_sys.exit.assert_called_with(1)

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.DatabaseConnectorFactory")
    def test_connect_database_success(self, mock_factory, mock_load_config):
        """Test successful database connection."""
        # Arrange
        mock_config = {
            "database": Mock(connection_string="postgresql://test", driver="postgresql")
        }
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.test_connection.return_value = True
        mock_factory.create_connector.return_value = mock_connector

        orchestrator = DataQualityOrchestrator()

        # Act
        result = orchestrator._connect_database()

        # Assert
        assert result is True
        assert orchestrator.connector == mock_connector
        mock_factory.create_connector.assert_called_once_with(
            "postgresql://test", "postgresql"
        )
        mock_connector.connect.assert_called_once()
        mock_connector.test_connection.assert_called_once()

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.DatabaseConnectorFactory")
    @patch("data_quality.core.orchestrator.console")
    def test_connect_database_test_failure(
        self, mock_console, mock_factory, mock_load_config
    ):
        """Test database connection when test_connection fails."""
        # Arrange
        mock_config = {
            "database": Mock(connection_string="postgresql://test", driver="postgresql")
        }
        mock_load_config.return_value = mock_config

        mock_connector = Mock()
        mock_connector.test_connection.return_value = False
        mock_factory.create_connector.return_value = mock_connector

        orchestrator = DataQualityOrchestrator()

        # Act
        result = orchestrator._connect_database()

        # Assert
        assert result is False
        mock_console.print.assert_called_with(
            "❌ [bold red]Database connection failed![/bold red]"
        )

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.DatabaseConnectorFactory")
    @patch("data_quality.core.orchestrator.console")
    def test_connect_database_exception(
        self, mock_console, mock_factory, mock_load_config
    ):
        """Test database connection with exception."""
        # Arrange
        mock_config = {
            "database": Mock(connection_string="postgresql://test", driver="postgresql")
        }
        mock_load_config.return_value = mock_config

        mock_factory.create_connector.side_effect = Exception("Connection error")

        orchestrator = DataQualityOrchestrator()

        # Act
        result = orchestrator._connect_database()

        # Assert
        assert result is False
        mock_console.print.assert_called_with(
            "❌ [bold red]Database connection error: Connection error[/bold red]"
        )

    @patch("data_quality.core.orchestrator.load_config")
    def test_disconnect_database_with_connector(self, mock_load_config):
        """Test database disconnection when connector exists."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()
        mock_connector = Mock()
        orchestrator.connector = mock_connector

        # Act
        orchestrator._disconnect_database()

        # Assert
        mock_connector.disconnect.assert_called_once()

    @patch("data_quality.core.orchestrator.load_config")
    def test_disconnect_database_without_connector(self, mock_load_config):
        """Test database disconnection when no connector."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()

        # Act (should not raise error)
        orchestrator._disconnect_database()

        # Assert - no exception raised
        assert orchestrator.connector is None

    @patch("data_quality.core.orchestrator.load_config")
    def test_build_metadata(self, mock_load_config):
        """Test metadata building."""
        # Arrange
        mock_db_config = Mock()
        mock_db_config.host = "localhost"
        mock_db_config.port = 5432
        mock_db_config.name = "testdb"
        mock_db_config.driver = "postgresql"
        mock_config = {"database": mock_db_config}
        mock_load_config.return_value = mock_config

        # Mock volumetry calculator
        mock_volumetry_calc = Mock()
        mock_volume_metrics = {
            "total_rows": 100,
            "total_columns": 3,
            "memory_usage": "1.2 MB",
        }
        mock_sampling_info = {
            "sample_size": 100,
            "total_rows": 100,
            "sampling_ratio": 1.0,
        }
        mock_volumetry_calc.calculate_volume_metrics.return_value = mock_volume_metrics
        mock_volumetry_calc.get_sampling_info.return_value = mock_sampling_info

        orchestrator = DataQualityOrchestrator(volumetry_calculator=mock_volumetry_calc)

        # Create test data
        data = pd.DataFrame(
            {"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10.5, 20.3, 30.1]}
        )

        # Act
        metadata = orchestrator._build_metadata("test_table", data, 100)

        # Assert
        assert metadata["table_name"] == "test_table"
        assert "analysis_timestamp" in metadata
        assert metadata["database_info"]["host"] == "localhost"
        assert metadata["database_info"]["port"] == 5432
        assert metadata["database_info"]["database"] == "testdb"
        assert metadata["database_info"]["driver"] == "postgresql"
        assert metadata["data_volume"] == mock_volume_metrics
        assert metadata["sampling_info"] == mock_sampling_info
        assert metadata["table_structure"]["columns"] == ["id", "name", "value"]
        assert "data_types" in metadata["table_structure"]

        mock_volumetry_calc.calculate_volume_metrics.assert_called_once_with(data)
        mock_volumetry_calc.get_sampling_info.assert_called_once_with(100, 3)

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.console")
    def test_analyze_table_connection_failure(self, mock_console, mock_load_config):
        """Test analyze_table when database connection fails."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()
        orchestrator._connect_database = Mock(return_value=False)

        # Act
        result = orchestrator.analyze_table("test_table")

        # Assert
        assert result == {"error": "Database connection failed"}
        orchestrator._connect_database.assert_called_once()

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.Progress")
    @patch("data_quality.core.orchestrator.console")
    def test_analyze_table_success_full_table(
        self, mock_console, mock_progress_class, mock_load_config
    ):
        """Test successful table analysis with full table data."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        # Mock progress
        mock_progress = Mock()
        mock_task = "mock_task"
        mock_progress.add_task.return_value = mock_task
        mock_progress_class.return_value.__enter__.return_value = mock_progress
        mock_progress_class.return_value.__exit__.return_value = None

        # Mock connector
        mock_connector = Mock()
        mock_connector.get_table_count.return_value = 100
        mock_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        mock_connector.execute_query.return_value = mock_data

        # Mock components
        mock_analyzer = Mock()
        mock_volumetry_calc = Mock()
        mock_volumetry_calc.display_volume_info = Mock()

        mock_results = [
            Mock(spec=ValidationResult, passed=True, severity=ValidationSeverity.INFO)
        ]
        mock_analyzer.analyze_dataframe.return_value = mock_results
        mock_analyzer.get_analysis_summary.return_value = {
            "total_validations": 1,
            "passed_validations": 1,
            "success_rate": 100.0,
        }

        orchestrator = DataQualityOrchestrator(
            analyzer=mock_analyzer, volumetry_calculator=mock_volumetry_calc
        )
        orchestrator._connect_database = Mock(return_value=True)
        orchestrator._disconnect_database = Mock()
        orchestrator._build_metadata = Mock(
            return_value={"data_volume": {"test": "volume"}, "metadata": "test"}
        )
        orchestrator.connector = mock_connector

        # Act
        result = orchestrator.analyze_table("test_table", sample_size=1000)

        # Assert
        assert "error" not in result
        assert result["table_name"] == "test_table"
        assert "metadata" in result
        assert "validation_results" in result
        assert "analysis_summary" in result

        mock_connector.get_table_count.assert_called_once_with("test_table")
        mock_connector.execute_query.assert_called_once_with("SELECT * FROM test_table")
        mock_analyzer.analyze_dataframe.assert_called_once_with(
            mock_data, "test_table", None
        )
        orchestrator._disconnect_database.assert_called_once()

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.Progress")
    @patch("data_quality.core.orchestrator.console")
    def test_analyze_table_success_sample(
        self, mock_console, mock_progress_class, mock_load_config
    ):
        """Test successful table analysis with sampling."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        # Mock progress
        mock_progress = Mock()
        mock_task = "mock_task"
        mock_progress.add_task.return_value = mock_task
        mock_progress_class.return_value.__enter__.return_value = mock_progress
        mock_progress_class.return_value.__exit__.return_value = None

        # Mock connector
        mock_connector = Mock()
        mock_connector.get_table_count.return_value = 50000  # More than sample_size
        mock_data = pd.DataFrame({"id": [1, 2], "name": ["A", "B"]})
        mock_connector.execute_query.return_value = mock_data

        # Mock components
        mock_analyzer = Mock()
        mock_volumetry_calc = Mock()

        mock_results = []
        mock_analyzer.analyze_dataframe.return_value = mock_results
        mock_analyzer.get_analysis_summary.return_value = {"total_validations": 0}

        orchestrator = DataQualityOrchestrator(
            analyzer=mock_analyzer, volumetry_calculator=mock_volumetry_calc
        )
        orchestrator._connect_database = Mock(return_value=True)
        orchestrator._disconnect_database = Mock()
        orchestrator._build_metadata = Mock(
            return_value={"data_volume": {"test": "volume"}, "metadata": "test"}
        )
        orchestrator.connector = mock_connector

        # Act
        result = orchestrator.analyze_table("test_table", sample_size=1000)

        # Assert
        assert "error" not in result
        mock_connector.get_table_count.assert_called_once_with("test_table")
        # Should use RAND() sampling query
        expected_query = "SELECT * FROM test_table ORDER BY RAND() LIMIT 1000"
        mock_connector.execute_query.assert_called_once_with(expected_query)

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.console")
    def test_analyze_table_exception(self, mock_console, mock_load_config):
        """Test analyze_table with exception during analysis."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()
        orchestrator._connect_database = Mock(return_value=True)
        orchestrator._disconnect_database = Mock()
        orchestrator.connector = Mock()
        orchestrator.connector.get_table_count.side_effect = Exception("Database error")

        # Act
        result = orchestrator.analyze_table("test_table")

        # Assert
        assert result == {"error": "Database error"}
        mock_console.print.assert_called_with(
            "❌ [bold red]Analysis error: Database error[/bold red]"
        )
        orchestrator._disconnect_database.assert_called_once()

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.console")
    def test_generate_reports_with_error(self, mock_console, mock_load_config):
        """Test generate_reports with error in analysis results."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()
        analysis_results = {"error": "Analysis failed"}

        # Act
        result = orchestrator.generate_reports(analysis_results)

        # Assert
        assert result == {}
        mock_console.print.assert_called_with(
            "❌ [bold red]Cannot generate report: Analysis failed[/bold red]"
        )

    @patch("data_quality.core.orchestrator.load_config")
    def test_generate_reports_unified(self, mock_load_config):
        """Test generate_reports with unified reports."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        mock_report_manager = Mock()
        expected_reports = {"html": Path("report.html"), "json": Path("report.json")}
        mock_report_manager.generate_unified_report.return_value = expected_reports

        orchestrator = DataQualityOrchestrator(report_manager=mock_report_manager)

        analysis_results = {
            "table_name": "test_table",
            "validation_results": [],
            "metadata": {"test": "metadata"},
        }

        # Act
        result = orchestrator.generate_reports(
            analysis_results,
            formats=["html", "json"],
            unified=True,
            report_name="custom_report",
        )

        # Assert
        assert result == expected_reports
        mock_report_manager.generate_unified_report.assert_called_once_with(
            [], "test_table", {"test": "metadata"}, "custom_report", ["html", "json"]
        )
        mock_report_manager.display_report_summary.assert_called_once_with(
            expected_reports
        )

    @patch("data_quality.core.orchestrator.load_config")
    def test_generate_reports_multiple(self, mock_load_config):
        """Test generate_reports with multiple separate reports."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        mock_report_manager = Mock()
        expected_reports = {"html": Path("report.html"), "txt": Path("report.txt")}
        mock_report_manager.generate_multiple_reports.return_value = expected_reports

        orchestrator = DataQualityOrchestrator(report_manager=mock_report_manager)

        analysis_results = {
            "table_name": "test_table",
            "validation_results": [],
            "metadata": {"test": "metadata"},
        }

        # Act
        result = orchestrator.generate_reports(
            analysis_results, formats=["html", "txt"], unified=False
        )

        # Assert
        assert result == expected_reports
        mock_report_manager.generate_multiple_reports.assert_called_once_with(
            [], "test_table", ["html", "txt"], {"test": "metadata"}, None
        )
        mock_report_manager.display_report_summary.assert_called_once_with(
            expected_reports
        )

    @patch("data_quality.core.orchestrator.load_config")
    def test_generate_reports_default_formats(self, mock_load_config):
        """Test generate_reports with default formats."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        mock_report_manager = Mock()
        mock_report_manager.generate_unified_report.return_value = {}

        orchestrator = DataQualityOrchestrator(report_manager=mock_report_manager)

        analysis_results = {
            "table_name": "test_table",
            "validation_results": [],
            "metadata": {},
        }

        # Act
        orchestrator.generate_reports(analysis_results)

        # Assert - should default to all formats
        mock_report_manager.generate_unified_report.assert_called_once_with(
            [], "test_table", {}, None, ["html", "json", "txt"]
        )

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.console")
    def test_run_complete_analysis_success(self, mock_console, mock_load_config):
        """Test run_complete_analysis success."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()

        analysis_results = {
            "table_name": "test_table",
            "validation_results": [],
            "metadata": {},
        }
        expected_reports = {"html": Path("report.html")}

        orchestrator.analyze_table = Mock(return_value=analysis_results)
        orchestrator.generate_reports = Mock(return_value=expected_reports)

        # Act
        result = orchestrator.run_complete_analysis(
            "test_table",
            sample_size=5000,
            validators=["completeness"],
            report_formats=["html"],
            unified_reports=False,
            report_name="custom",
        )

        # Assert
        assert result == expected_reports
        orchestrator.analyze_table.assert_called_once_with(
            "test_table", 5000, ["completeness"]
        )
        orchestrator.generate_reports.assert_called_once_with(
            analysis_results, formats=["html"], unified=False, report_name="custom"
        )
        mock_console.print.assert_any_call(
            "🚀 [bold blue]Starting Complete Data Quality Analysis[/bold blue]"
        )
        mock_console.print.assert_any_call(
            "\n🎉 [bold green]Complete analysis finished![/bold green]"
        )

    @patch("data_quality.core.orchestrator.load_config")
    @patch("data_quality.core.orchestrator.console")
    def test_run_complete_analysis_with_error(self, mock_console, mock_load_config):
        """Test run_complete_analysis when analysis has error."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        orchestrator = DataQualityOrchestrator()

        analysis_results = {"error": "Analysis failed"}
        orchestrator.analyze_table = Mock(return_value=analysis_results)

        # Act
        result = orchestrator.run_complete_analysis("test_table")

        # Assert
        assert result == {}
        orchestrator.analyze_table.assert_called_once_with("test_table", 10000, None)
        mock_console.print.assert_called_with(
            "🚀 [bold blue]Starting Complete Data Quality Analysis[/bold blue]"
        )
        # Should not print success message
        assert not any(
            "Complete analysis finished!" in str(call)
            for call in mock_console.print.call_args_list[1:]
        )

    @patch("data_quality.core.orchestrator.load_config")
    def test_register_validators(self, mock_load_config):
        """Test that all validators are properly registered."""
        # Arrange
        mock_config = {"database": Mock()}
        mock_load_config.return_value = mock_config

        mock_analyzer = Mock()

        # Act
        orchestrator = DataQualityOrchestrator(analyzer=mock_analyzer)

        # Assert - verify all 3 basic validators are registered (IntegrityValidator needs connection)
        assert orchestrator is not None
        assert mock_analyzer.register_validator.call_count == 3

        # Check that validator types are correct (only basic validators)
        call_args_list = mock_analyzer.register_validator.call_args_list
        validator_types = [call[0][0].__class__.__name__ for call in call_args_list]

        assert "CompletenessValidator" in validator_types
        assert "DuplicatesValidator" in validator_types
        assert "PatternsValidator" in validator_types
        # IntegrityValidator not included as it needs database connection
