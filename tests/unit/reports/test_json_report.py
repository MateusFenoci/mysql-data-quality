"""Tests for JSONReportGenerator."""

import json
import tempfile
from pathlib import Path
from datetime import datetime

from data_quality.reports.json_report import JSONReportGenerator
from data_quality.validators.base import ValidationResult, ValidationSeverity


class TestJSONReportGenerator:
    """Test cases for JSONReportGenerator."""

    def test_init(self):
        """Test JSONReportGenerator initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Act
            generator = JSONReportGenerator(temp_dir)

            # Assert
            assert generator.output_dir == Path(temp_dir)

    def test_generate_report(self):
        """Test JSON report generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            generator = JSONReportGenerator(temp_dir)

            result = ValidationResult(
                rule_name="test_rule",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Test error message",
                details={"count": 5, "threshold": 10},
                timestamp=datetime(2025, 1, 1, 12, 0, 0),
                affected_rows=5,
                total_rows=10,
            )

            metadata = {
                "table_name": "test_table",
                "analysis_date": "2025-01-01",
                "database": "test_db",
            }

            # Act
            report_path = generator.generate_report([result], "test_table", metadata)

            # Assert
            assert report_path.exists()
            assert report_path.suffix == ".json"

            # Verify JSON content
            with open(report_path, "r") as f:
                report_data = json.load(f)

            assert "report" in report_data
            report = report_data["report"]

            assert report["table_name"] == "test_table"
            assert report["metadata"] == metadata
            assert len(report["results"]) == 1

            # Verify result data
            result_data = report["results"][0]
            assert result_data["rule_name"] == "test_rule"
            assert result_data["severity"] == "ERROR"
            assert result_data["passed"] is False
            assert result_data["message"] == "Test error message"
            assert result_data["details"] == {"count": 5, "threshold": 10}
            assert result_data["affected_rows"] == 5
            assert result_data["total_rows"] == 10

    def test_generate_report_multiple_results(self):
        """Test JSON report generation with multiple results."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            generator = JSONReportGenerator(temp_dir)

            results = [
                ValidationResult(
                    rule_name="rule_1",
                    table_name="test_table",
                    column_name="col1",
                    severity=ValidationSeverity.ERROR,
                    passed=False,
                    message="Error 1",
                    details={},
                    timestamp=datetime.now(),
                    affected_rows=2,
                    total_rows=10,
                ),
                ValidationResult(
                    rule_name="rule_2",
                    table_name="test_table",
                    column_name="col2",
                    severity=ValidationSeverity.WARNING,
                    passed=False,
                    message="Warning 1",
                    details={},
                    timestamp=datetime.now(),
                    affected_rows=1,
                    total_rows=10,
                ),
                ValidationResult(
                    rule_name="rule_3",
                    table_name="test_table",
                    column_name="col3",
                    severity=ValidationSeverity.INFO,
                    passed=True,
                    message="Info 1",
                    details={},
                    timestamp=datetime.now(),
                    affected_rows=0,
                    total_rows=10,
                ),
            ]

            # Act
            report_path = generator.generate_report(results, "test_table")

            # Assert
            with open(report_path, "r") as f:
                report_data = json.load(f)

            assert len(report_data["report"]["results"]) == 3

            # Verify different severities are present
            severities = [r["severity"] for r in report_data["report"]["results"]]
            assert "ERROR" in severities
            assert "WARNING" in severities
            assert "INFO" in severities

    def test_generate_report_empty_results(self):
        """Test JSON report generation with empty results."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            generator = JSONReportGenerator(temp_dir)

            # Act
            report_path = generator.generate_report([], "empty_table")

            # Assert
            assert report_path.exists()

            with open(report_path, "r") as f:
                report_data = json.load(f)

            assert report_data["report"]["table_name"] == "empty_table"
            assert report_data["report"]["results"] == []

    def test_generate_report_filename_format(self):
        """Test that generated filename follows expected format."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Arrange
            generator = JSONReportGenerator(temp_dir)

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
            report_path = generator.generate_report([result], "customers")

            # Assert
            filename = report_path.name
            assert filename.startswith("data_quality_report_customers_")
            assert filename.endswith(".json")
            assert len(filename.split("_")) >= 5  # Has timestamp components
