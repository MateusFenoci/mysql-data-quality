"""Tests for ReportManager."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from data_quality.core.report_manager import ReportManager
from data_quality.validators.base import ValidationResult, ValidationSeverity
from datetime import datetime


class TestReportManager:
    """Test cases for ReportManager."""

    def test_init(self):
        """Test ReportManager initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Act
            manager = ReportManager(temp_dir)

            # Assert
            assert manager.output_dir == Path(temp_dir)
            assert manager.output_dir.exists()

    def test_init_creates_directory(self):
        """Test that ReportManager creates output directory if it doesn't exist."""
        with tempfile.TemporaryDirectory() as temp_dir:
            non_existent_dir = Path(temp_dir) / "new_reports"

            # Act
            manager = ReportManager(str(non_existent_dir))

            # Assert
            assert manager.output_dir.exists()

    @patch("data_quality.core.report_manager.HTMLReportGenerator")
    def test_generate_single_report_html(self, mock_html_generator_class):
        """Test generating single HTML report."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)
            mock_generator = Mock()
            mock_html_generator_class.return_value = mock_generator

            expected_path = Path(temp_dir) / "test_report.html"
            mock_generator.generate_report.return_value = expected_path

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="Test message",
                details={},
                timestamp=datetime.now(),
                affected_rows=0,
                total_rows=10,
            )

            with open(expected_path, "w") as f:
                f.write("test content")

            # Act
            result_path = manager.generate_single_report(
                [result], "test_table", "html", {"test": "metadata"}
            )

            # Assert
            assert result_path == expected_path
            mock_generator.generate_report.assert_called_once()

    def test_generate_single_report_invalid_format(self):
        """Test generating report with invalid format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="Test message",
                details={},
                timestamp=datetime.now(),
                affected_rows=0,
                total_rows=10,
            )

            # Act & Assert
            with pytest.raises(ValueError, match="Unsupported format: invalid"):
                manager.generate_single_report([result], "test_table", "invalid")

    @patch("data_quality.core.report_manager.Progress")
    @patch("data_quality.core.report_manager.HTMLReportGenerator")
    @patch("data_quality.core.report_manager.JSONReportGenerator")
    def test_generate_multiple_reports(
        self, mock_json_gen_class, mock_html_gen_class, mock_progress_class
    ):
        """Test generating multiple reports."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)

            # Mock progress
            mock_progress = Mock()
            mock_progress_class.return_value.__enter__.return_value = mock_progress

            # Mock generators
            mock_html_gen = Mock()
            mock_json_gen = Mock()
            mock_html_gen_class.return_value = mock_html_gen
            mock_json_gen_class.return_value = mock_json_gen

            html_path = Path(temp_dir) / "test.html"
            json_path = Path(temp_dir) / "test.json"
            mock_html_gen.generate_report.return_value = html_path
            mock_json_gen.generate_report.return_value = json_path

            # Create files
            html_path.touch()
            json_path.touch()

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="Test message",
                details={},
                timestamp=datetime.now(),
                affected_rows=0,
                total_rows=10,
            )

            # Act
            reports = manager.generate_multiple_reports(
                [result], "test_table", ["html", "json"]
            )

            # Assert
            assert len(reports) == 2
            assert "html" in reports
            assert "json" in reports
            assert reports["html"] == html_path
            assert reports["json"] == json_path

    @patch("data_quality.core.report_manager.console")
    def test_display_report_summary_with_reports(self, mock_console):
        """Test displaying report summary with generated reports."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)
            reports = {
                "html": Path(temp_dir) / "test.html",
                "json": Path(temp_dir) / "test.json",
            }

            # Act
            manager.display_report_summary(reports)

            # Assert
            assert (
                mock_console.print.call_count >= 3
            )  # Success message + 2 report files

            # Check calls contain expected content
            calls = [str(call) for call in mock_console.print.call_args_list]
            assert any("Reports generated successfully" in call for call in calls)
            assert any("HTML" in call for call in calls)
            assert any("JSON" in call for call in calls)

    @patch("data_quality.core.report_manager.console")
    def test_display_report_summary_no_reports(self, mock_console):
        """Test displaying report summary with no generated reports."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)

            # Act
            manager.display_report_summary({})

            # Assert
            mock_console.print.assert_called_once()
            call_str = str(mock_console.print.call_args_list[0])
            assert "No reports were generated" in call_str

    @patch("data_quality.core.report_manager.HTMLReportGenerator")
    def test_generate_single_report_with_custom_name(self, mock_html_generator_class):
        """Test generating single report with custom name."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)
            mock_generator = Mock()
            mock_html_generator_class.return_value = mock_generator

            original_path = Path(temp_dir) / "original_report.html"
            original_path.touch()  # Create the file so rename works
            mock_generator.generate_report.return_value = original_path

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="test_column",
                passed=True,
                message="Test message",
                severity=ValidationSeverity.INFO,
                details={},
                timestamp=datetime.now(),
            )

            # Act
            report_path = manager.generate_single_report(
                [result], "test_table", "html", custom_name="custom_report"
            )

            # Assert
            assert report_path.name.startswith("custom_report_")
            assert report_path.name.endswith(".html")
            assert report_path.exists()

    @patch("data_quality.core.report_manager.HTMLReportGenerator")
    @patch("data_quality.core.report_manager.console")
    def test_generate_multiple_reports_with_exception(
        self, mock_console, mock_html_generator_class
    ):
        """Test generating multiple reports when one format fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)
            mock_generator = Mock()
            mock_html_generator_class.return_value = mock_generator
            mock_generator.generate_report.side_effect = Exception("Test error")

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="test_column",
                passed=True,
                message="Test message",
                severity=ValidationSeverity.INFO,
                details={},
                timestamp=datetime.now(),
            )

            # Act
            reports = manager.generate_multiple_reports(
                [result], "test_table", ["html"]
            )

            # Assert
            assert len(reports) == 0  # No successful reports
            # Verify error was printed
            calls = [str(call) for call in mock_console.print.call_args_list]
            assert any("HTML report failed" in call for call in calls)

    @patch("data_quality.core.report_manager.HTMLReportGenerator")
    @patch("data_quality.core.report_manager.JSONReportGenerator")
    @patch("data_quality.core.report_manager.SummaryReportGenerator")
    def test_generate_unified_reports(
        self, mock_summary_gen, mock_json_gen, mock_html_gen
    ):
        """Test generating unified reports."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)

            # Create mock generators
            mock_html_generator = Mock()
            mock_json_generator = Mock()
            mock_summary_generator = Mock()

            mock_html_gen.return_value = mock_html_generator
            mock_json_gen.return_value = mock_json_generator
            mock_summary_gen.return_value = mock_summary_generator

            # Create temporary files for renaming
            temp_html = Path(temp_dir) / "temp.html"
            temp_json = Path(temp_dir) / "temp.json"
            temp_txt = Path(temp_dir) / "temp.txt"

            temp_html.touch()
            temp_json.touch()
            temp_txt.touch()

            mock_html_generator.generate_report.return_value = temp_html
            mock_json_generator.generate_report.return_value = temp_json
            mock_summary_generator.generate_report.return_value = temp_txt

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="test_column",
                passed=True,
                message="Test message",
                severity=ValidationSeverity.INFO,
                details={},
                timestamp=datetime.now(),
            )

            # Act
            reports = manager.generate_unified_report(
                [result], "test_table", report_name="custom_unified"
            )

            # Assert
            assert len(reports) == 3
            assert "html" in reports
            assert "json" in reports
            assert "txt" in reports

            # Check that files were renamed with unified naming
            assert "custom_unified" in reports["html"].name
            assert "custom_unified" in reports["json"].name
            assert "custom_unified" in reports["txt"].name

    @patch("data_quality.core.report_manager.HTMLReportGenerator")
    @patch("data_quality.core.report_manager.console")
    def test_generate_unified_reports_with_exception(self, mock_console, mock_html_gen):
        """Test generating unified reports when one format fails."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)
            mock_generator = Mock()
            mock_html_gen.return_value = mock_generator
            mock_generator.generate_report.side_effect = Exception("Test error")

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="test_column",
                passed=True,
                message="Test message",
                severity=ValidationSeverity.INFO,
                details={},
                timestamp=datetime.now(),
            )

            # Act
            reports = manager.generate_unified_report(
                [result], "test_table", formats=["html"]
            )

            # Assert
            assert len(reports) == 0  # No successful reports
            # Verify error was printed
            calls = [str(call) for call in mock_console.print.call_args_list]
            assert any("HTML report failed" in call for call in calls)

    def test_generate_multiple_reports_default_formats(self):
        """Test that generate_multiple_reports uses default formats when none provided."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            manager = ReportManager(temp_dir)

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="test_column",
                passed=True,
                message="Test message",
                severity=ValidationSeverity.INFO,
                details={},
                timestamp=datetime.now(),
            )

            with patch.object(manager, "generate_single_report") as mock_generate:
                mock_generate.return_value = Path(temp_dir) / "test.html"

                # Act
                manager.generate_multiple_reports([result], "test_table", formats=[])

                # Assert
                # Should be called 3 times for default formats (html, json, txt)
                assert mock_generate.call_count == 3

                # Check that all default formats were called
                calls = [
                    call[0][2] for call in mock_generate.call_args_list
                ]  # format_type argument
                assert "html" in calls
                assert "json" in calls
                assert "txt" in calls
