"""Tests for ReportManager."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import pytest

from src.data_quality.core.report_manager import ReportManager
from src.data_quality.validators.base import ValidationResult, ValidationSeverity
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

    @patch("src.data_quality.core.report_manager.HTMLReportGenerator")
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

    @patch("src.data_quality.core.report_manager.Progress")
    @patch("src.data_quality.core.report_manager.HTMLReportGenerator")
    @patch("src.data_quality.core.report_manager.JSONReportGenerator")
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

    @patch("src.data_quality.core.report_manager.console")
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

    @patch("src.data_quality.core.report_manager.console")
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
