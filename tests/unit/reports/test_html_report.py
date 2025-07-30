"""Tests for HTML report generator."""

import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

import pytest

from data_quality.reports.html_report import HTMLReportGenerator
from data_quality.validators.base import ValidationResult, ValidationSeverity


class TestHTMLReportGenerator:
    """Test cases for HTMLReportGenerator."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def sample_results(self):
        """Create sample validation results for testing."""
        return [
            ValidationResult(
                rule_name="completeness_check",
                table_name="test_table",
                column_name="name",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Column has missing values",
                details={"completeness_ratio": 0.8, "null_count": 20},
                timestamp=datetime.now(),
                affected_rows=20,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="duplicates_check",
                table_name="test_table",
                column_name="id",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message="Duplicate values found",
                details={"duplicate_count": 5},
                timestamp=datetime.now(),
                affected_rows=5,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="pattern_check",
                table_name="test_table",
                column_name="email",
                severity=ValidationSeverity.CRITICAL,
                passed=False,
                message="Invalid email format",
                details={"pattern_type": "email", "invalid_count": 3},
                timestamp=datetime.now(),
                affected_rows=3,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="data_type_check",
                table_name="test_table",
                column_name="age",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="All values are valid integers",
                details={},
                timestamp=datetime.now(),
                affected_rows=0,
                total_rows=100,
            ),
        ]

    def test_init(self, temp_dir):
        """Test HTML report generator initialization."""
        # Act
        generator = HTMLReportGenerator(temp_dir)

        # Assert
        assert generator.output_dir == temp_dir
        assert isinstance(generator, HTMLReportGenerator)

    @patch("data_quality.reports.html_report.datetime")
    def test_generate_report_creates_file(
        self, mock_datetime, temp_dir, sample_results
    ):
        """Test that generate_report creates HTML file."""
        # Arrange
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strftime = datetime.strftime

        generator = HTMLReportGenerator(temp_dir)

        # Act
        output_path = generator.generate_report(sample_results, "test_table")

        # Assert
        assert output_path.exists()
        assert output_path.suffix == ".html"
        assert "test_table" in output_path.name
        assert "20230101_120000" in output_path.name

        # Verify file content
        content = output_path.read_text(encoding="utf-8")
        assert "<!DOCTYPE html>" in content
        assert "Data Quality Report" in content
        assert "test_table" in content

    def test_generate_report_with_metadata(self, temp_dir, sample_results):
        """Test report generation with metadata."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        metadata = {
            "database": "test_db",
            "total_rows": 1000,
            "analysis_time": "2023-01-01 12:00:00",
        }

        # Act
        output_path = generator.generate_report(sample_results, "test_table", metadata)

        # Assert
        content = output_path.read_text(encoding="utf-8")
        # Check individual metadata fields instead of string representation
        assert "test_db" in content
        assert "1000" in content
        assert "2023-01-01 12:00:00" in content

    def test_generate_report_empty_results(self, temp_dir):
        """Test report generation with empty results."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        # Act
        output_path = generator.generate_report([], "empty_table")

        # Assert
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "empty_table" in content
        assert "Total Checks" in content

    def test_create_html_report_structure(self, temp_dir, sample_results):
        """Test HTML report structure and content."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        html_content = generator._create_html_report(
            sample_results, "test_table", summary
        )

        # Assert
        # Check basic HTML structure
        assert "<!DOCTYPE html>" in html_content
        assert '<html lang="en">' in html_content
        assert "<head>" in html_content
        assert "<body>" in html_content

        # Check title and header
        assert "Data Quality Report - test_table" in html_content
        assert "🔍 Data Quality Report" in html_content
        assert "Table: test_table" in html_content

        # Check summary section
        assert "📊 Summary" in html_content
        assert "Total Checks" in html_content
        assert "Passed" in html_content
        assert "Failed" in html_content
        assert "Success Rate" in html_content

        # Check severity sections
        assert "🚨 Critical Issues" in html_content
        assert "❌ Errors" in html_content
        assert "⚠️ Warnings" in html_content
        assert "💡 Information" in html_content

        # Check specific validation results
        assert "completeness_check" in html_content
        assert "duplicates_check" in html_content
        assert "pattern_check" in html_content
        assert "data_type_check" in html_content

    def test_create_validator_breakdown_section_with_data(
        self, temp_dir, sample_results
    ):
        """Test validator breakdown section with data."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        section_html = generator._create_validator_breakdown_section(summary)

        # Assert
        assert "🔧 Validator Breakdown" in section_html
        assert "<table>" in section_html
        assert "Validator" in section_html
        assert "Total" in section_html
        assert "Passed" in section_html
        assert "Failed" in section_html
        assert "Success Rate" in section_html

        # Check that validator types are included
        for validator_type in summary["validator_breakdown"].keys():
            assert validator_type.title() in section_html

    def test_create_validator_breakdown_section_empty(self, temp_dir):
        """Test validator breakdown section with empty data."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        summary = {"validator_breakdown": {}}

        # Act
        section_html = generator._create_validator_breakdown_section(summary)

        # Assert
        assert section_html == ""

    def test_create_severity_breakdown_section_with_data(
        self, temp_dir, sample_results
    ):
        """Test severity breakdown section with data."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        section_html = generator._create_severity_breakdown_section(summary)

        # Assert
        assert "⚖️ Severity Breakdown" in section_html
        assert "<table>" in section_html
        assert "Severity" in section_html
        assert "Total" in section_html
        assert "Passed" in section_html
        assert "Failed" in section_html

        # Check that severities are included
        for severity in summary["severity_breakdown"].keys():
            assert severity in section_html

    def test_create_severity_breakdown_section_empty(self, temp_dir):
        """Test severity breakdown section with empty data."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        summary = {"severity_breakdown": {}}

        # Act
        section_html = generator._create_severity_breakdown_section(summary)

        # Assert
        assert section_html == ""

    def test_create_results_section_with_results(self, temp_dir, sample_results):
        """Test results section with validation results."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        error_results = [
            r for r in sample_results if r.severity == ValidationSeverity.ERROR
        ]

        # Act
        section_html = generator._create_results_section(
            "❌ Errors", error_results, "error"
        )

        # Assert
        assert "❌ Errors" in section_html
        assert "results-section error" in section_html
        assert "completeness_check" in section_html
        assert "Column has missing values" in section_html
        assert "Affected: 20" in section_html
        assert "Completeness: 80.0%" in section_html

    def test_create_results_section_empty(self, temp_dir):
        """Test results section with no results."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        # Act
        section_html = generator._create_results_section("❌ Errors", [], "error")

        # Assert
        assert section_html == ""

    def test_create_results_section_different_details(self, temp_dir):
        """Test results section with different detail types."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        results_with_different_details = [
            ValidationResult(
                rule_name="completeness_check",
                table_name="test_table",
                column_name="name",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Missing values",
                details={"completeness_ratio": 0.9},
                timestamp=datetime.now(),
                affected_rows=10,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="duplicates_check",
                table_name="test_table",
                column_name="id",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message="Duplicates found",
                details={"duplicate_count": 5},
                timestamp=datetime.now(),
                affected_rows=5,
                total_rows=100,
            ),
            ValidationResult(
                rule_name="pattern_check",
                table_name="test_table",
                column_name="email",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Invalid patterns",
                details={"invalid_count": 3, "pattern_type": "email"},
                timestamp=datetime.now(),
                affected_rows=3,
                total_rows=100,
            ),
        ]

        # Act
        section_html = generator._create_results_section(
            "❌ Errors", results_with_different_details, "error"
        )

        # Assert
        assert "Completeness: 90.0%" in section_html
        assert "Duplicates: 5" in section_html
        assert "Invalid: 3" in section_html
        assert "Pattern: email" in section_html

    def test_create_results_section_no_column_name(self, temp_dir):
        """Test results section with results that have no column name."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        result_no_column = ValidationResult(
            rule_name="table_check",
            table_name="test_table",
            column_name=None,
            severity=ValidationSeverity.INFO,
            passed=True,
            message="Table structure is valid",
            details={},
            timestamp=datetime.now(),
            affected_rows=0,
            total_rows=100,
        )

        # Act
        section_html = generator._create_results_section(
            "💡 Information", [result_no_column], "info"
        )

        # Assert
        assert "table_check" in section_html
        assert "Table structure is valid" in section_html
        assert "[test_table]" not in section_html  # No column info should be displayed

    def test_create_results_section_no_affected_rows(self, temp_dir):
        """Test results section with results that have no affected rows."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        result_no_affected = ValidationResult(
            rule_name="validation_check",
            table_name="test_table",
            column_name="status",
            severity=ValidationSeverity.INFO,
            passed=True,
            message="All values are valid",
            details={},
            timestamp=datetime.now(),
            affected_rows=0,
            total_rows=100,
        )

        # Act
        section_html = generator._create_results_section(
            "💡 Information", [result_no_affected], "info"
        )

        # Assert
        assert "validation_check" in section_html
        assert "All values are valid" in section_html
        assert (
            "Affected:" not in section_html
        )  # No affected rows info should be displayed

    def test_get_css_styles(self, temp_dir):
        """Test CSS styles generation."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        # Act
        css = generator._get_css_styles()

        # Assert
        assert "body {" in css
        assert "font-family:" in css
        assert ".container {" in css
        assert ".summary-cards {" in css
        assert ".card {" in css
        assert "table {" in css
        assert ".result-item {" in css
        assert "@media (max-width: 768px)" in css

        # Check color classes
        assert ".success {" in css
        assert ".error {" in css
        assert ".warning {" in css
        assert ".info {" in css or ".card.info {" in css

    def test_result_icons_and_status(self, temp_dir):
        """Test that results show correct icons and status."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)

        passed_result = ValidationResult(
            rule_name="test_passed",
            table_name="test_table",
            column_name="col1",
            severity=ValidationSeverity.INFO,
            passed=True,
            message="Test passed",
            details={},
            timestamp=datetime.now(),
            affected_rows=0,
            total_rows=100,
        )

        failed_result = ValidationResult(
            rule_name="test_failed",
            table_name="test_table",
            column_name="col2",
            severity=ValidationSeverity.ERROR,
            passed=False,
            message="Test failed",
            details={},
            timestamp=datetime.now(),
            affected_rows=10,
            total_rows=100,
        )

        # Act
        section_html = generator._create_results_section(
            "Test Results", [passed_result, failed_result], "mixed"
        )

        # Assert
        assert "✅" in section_html  # Passed icon
        assert "❌" in section_html  # Failed icon
        assert "test_passed" in section_html
        assert "test_failed" in section_html

    def test_full_report_integration(self, temp_dir, sample_results):
        """Test full report generation integration."""
        # Arrange
        generator = HTMLReportGenerator(temp_dir)
        metadata = {"database": "test_db", "analysis_date": "2023-01-01"}

        # Act
        output_path = generator.generate_report(
            sample_results, "integration_test", metadata
        )

        # Assert
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")

        # Check that all components are present
        assert "integration_test" in content
        assert "🔍 Data Quality Report" in content
        assert "📊 Summary" in content
        assert "🔧 Validator Breakdown" in content
        assert "⚖️ Severity Breakdown" in content
        assert "🚨 Critical Issues" in content
        assert "❌ Errors" in content
        assert "⚠️ Warnings" in content
        assert "💡 Information" in content

        # Check that all sample results are included
        for result in sample_results:
            assert result.rule_name in content
            assert result.message in content

        # Check metadata is included (check individual fields)
        assert "test_db" in content
        assert "2023-01-01" in content
