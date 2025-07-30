"""Tests for Summary report generator."""

import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import patch

import pytest

from src.data_quality.reports.summary_report import SummaryReportGenerator
from src.data_quality.validators.base import ValidationResult, ValidationSeverity


class TestSummaryReportGenerator:
    """Test cases for SummaryReportGenerator."""

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
                affected_rows=150,  # High impact
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
            ValidationResult(
                rule_name="integrity_check",
                table_name="test_table",
                column_name="user_id",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Foreign key constraint violation",
                details={},
                timestamp=datetime.now(),
                affected_rows=10,
                total_rows=100,
            ),
        ]

    def test_init(self, temp_dir):
        """Test summary report generator initialization."""
        # Act
        generator = SummaryReportGenerator(temp_dir)

        # Assert
        assert generator.output_dir == temp_dir
        assert isinstance(generator, SummaryReportGenerator)

    @patch("src.data_quality.reports.summary_report.datetime")
    def test_generate_report_creates_file(
        self, mock_datetime, temp_dir, sample_results
    ):
        """Test that generate_report creates text file."""
        # Arrange
        mock_now = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = mock_now
        mock_datetime.strftime = datetime.strftime

        generator = SummaryReportGenerator(temp_dir)

        # Act
        output_path = generator.generate_report(sample_results, "test_table")

        # Assert
        assert output_path.exists()
        assert output_path.suffix == ".txt"
        assert "test_table" in output_path.name
        assert "20230101_120000" in output_path.name

        # Verify file content
        content = output_path.read_text(encoding="utf-8")
        assert "DATA QUALITY SUMMARY REPORT" in content
        assert "test_table" in content

    def test_generate_report_with_metadata(self, temp_dir, sample_results):
        """Test report generation with metadata."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        metadata = {
            "database": "test_db",
            "total_rows": 1000,
            "analysis_time": "2023-01-01 12:00:00",
        }

        # Act
        output_path = generator.generate_report(sample_results, "test_table", metadata)

        # Assert
        content = output_path.read_text(encoding="utf-8")
        assert str(metadata) in content

    def test_generate_report_empty_results(self, temp_dir):
        """Test report generation with empty results."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        # Act
        output_path = generator.generate_report([], "empty_table")

        # Assert
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")
        assert "empty_table" in content
        assert "Total Checks:" in content

    def test_create_summary_content_structure(self, temp_dir, sample_results):
        """Test summary content structure."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)
        metadata = {"test": "metadata"}

        # Act
        content = generator._create_summary_content(
            sample_results, "test_table", summary, metadata
        )

        # Assert
        # Check main sections
        assert "DATA QUALITY SUMMARY REPORT" in content
        assert "Table: test_table" in content
        assert "Generated:" in content
        assert "Metadata: {'test': 'metadata'}" in content

        # Check overall summary section
        assert "OVERALL SUMMARY" in content
        assert "Total Checks:" in content
        assert "Passed:" in content
        assert "Failed:" in content
        assert "Success Rate:" in content
        assert "Quality Score:" in content

        # Check validator breakdown section
        assert "VALIDATOR BREAKDOWN" in content

        # Check severity breakdown section
        assert "SEVERITY BREAKDOWN" in content

        # Check top issues section
        assert "TOP ISSUES" in content

        # Check recommendations section
        assert "RECOMMENDATIONS" in content

    def test_get_quality_score_ranges(self, temp_dir):
        """Test quality score ranges."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        # Act & Assert
        assert "🟢 EXCELLENT" in generator._get_quality_score(98.0)
        assert "🟡 GOOD" in generator._get_quality_score(90.0)
        assert "🟠 FAIR" in generator._get_quality_score(75.0)
        assert "🔴 POOR" in generator._get_quality_score(60.0)
        assert "💀 CRITICAL" in generator._get_quality_score(30.0)

    def test_get_status_indicator_ranges(self, temp_dir):
        """Test status indicator ranges."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        # Act & Assert
        assert generator._get_status_indicator(95.0) == "✅"
        assert generator._get_status_indicator(80.0) == "⚠️ "
        assert generator._get_status_indicator(50.0) == "❌"

    def test_get_severity_icon_mapping(self, temp_dir):
        """Test severity icon mapping."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        # Act & Assert
        assert generator._get_severity_icon("CRITICAL") == "🚨"
        assert generator._get_severity_icon("ERROR") == "❌"
        assert generator._get_severity_icon("WARNING") == "⚠️ "
        assert generator._get_severity_icon("INFO") == "💡"
        assert generator._get_severity_icon("UNKNOWN") == "❓"

    def test_validator_breakdown_section(self, temp_dir, sample_results):
        """Test validator breakdown section generation."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        content = generator._create_summary_content(
            sample_results, "test_table", summary
        )

        # Assert
        assert "VALIDATOR BREAKDOWN" in content

        # Check that validator types are included with their stats
        for validator_type in summary["validator_breakdown"].keys():
            assert validator_type.title() in content

    def test_severity_breakdown_section(self, temp_dir, sample_results):
        """Test severity breakdown section generation."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        content = generator._create_summary_content(
            sample_results, "test_table", summary
        )

        # Assert
        assert "SEVERITY BREAKDOWN" in content
        assert "🚨 CRITICAL" in content
        assert "❌ ERROR" in content
        assert "⚠️  WARNING" in content
        assert "💡 INFO" in content

    def test_top_issues_section_ordering(self, temp_dir, sample_results):
        """Test top issues section ordering by severity and impact."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        content = generator._create_summary_content(
            sample_results, "test_table", summary
        )

        # Assert
        assert "TOP ISSUES" in content

        # Check that issues are present
        assert "pattern_check" in content  # CRITICAL severity should be first
        assert "completeness_check" in content
        assert "duplicates_check" in content
        assert "integrity_check" in content

        # Check that messages are included
        assert "Invalid email format" in content
        assert "Column has missing values" in content
        assert "Duplicate values found" in content
        assert "Foreign key constraint violation" in content

    def test_top_issues_with_column_info(self, temp_dir, sample_results):
        """Test top issues include column information when available."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        content = generator._create_summary_content(
            sample_results, "test_table", summary
        )

        # Assert
        assert "[name]" in content  # Column name should be included
        assert "[id]" in content
        assert "[email]" in content
        assert "[user_id]" in content

        # Check affected rows information
        assert "150 /" in content  # High impact issue
        assert "20 /" in content
        assert "10 /" in content
        assert "5 /" in content

    def test_generate_recommendations_low_success_rate(self, temp_dir):
        """Test recommendations for low success rate."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        summary = {
            "success_rate": 50.0,
            "validator_breakdown": {},
            "severity_breakdown": {},
        }
        failed_results = []

        # Act
        recommendations = generator._generate_recommendations(summary, failed_results)

        # Assert
        assert (
            "Focus on critical issues first - success rate below 70%" in recommendations
        )

    def test_generate_recommendations_validator_specific(self, temp_dir):
        """Test validator-specific recommendations."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        summary = {
            "success_rate": 90.0,
            "validator_breakdown": {
                "completeness": {"passed": 5, "total": 10},  # 50% success rate
                "duplicates": {"passed": 6, "total": 10},  # 60% success rate
                "patterns": {"passed": 7, "total": 10},  # 70% success rate
                "integrity": {"passed": 6, "total": 10},  # 60% success rate
            },
            "severity_breakdown": {},
        }
        failed_results = []

        # Act
        recommendations = generator._generate_recommendations(summary, failed_results)

        # Assert
        assert "Address data completeness issues" in recommendations
        assert "Review duplicate data" in recommendations
        assert "Fix data format issues" in recommendations
        assert "Resolve referential integrity issues" in recommendations

    def test_generate_recommendations_high_impact_issues(
        self, temp_dir, sample_results
    ):
        """Test recommendations for high-impact issues."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        # Create high-impact results (>100 affected rows)
        high_impact_results = [
            result
            for result in sample_results
            if not result.passed and result.affected_rows > 100
        ]

        summary = {
            "success_rate": 80.0,
            "validator_breakdown": {},
            "severity_breakdown": {},
        }

        # Act
        recommendations = generator._generate_recommendations(
            summary, high_impact_results
        )

        # Assert
        assert (
            "Prioritize 1 high-impact issues affecting >100 rows each"
            in recommendations
        )

    def test_generate_recommendations_critical_errors(self, temp_dir, sample_results):
        """Test recommendations for critical and error severity issues."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        critical_error_results = [
            result
            for result in sample_results
            if not result.passed and result.severity.value in ["CRITICAL", "ERROR"]
        ]

        summary = {
            "success_rate": 80.0,
            "validator_breakdown": {},
            "severity_breakdown": {},
        }

        # Act
        recommendations = generator._generate_recommendations(
            summary, critical_error_results
        )

        # Assert
        assert "Address 3 critical/error issues immediately" in recommendations

    def test_generate_recommendations_excellent_quality(self, temp_dir):
        """Test recommendations for excellent data quality."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        summary = {
            "success_rate": 98.0,
            "validator_breakdown": {},
            "severity_breakdown": {},
        }
        failed_results = []

        # Act
        recommendations = generator._generate_recommendations(summary, failed_results)

        # Assert
        assert (
            "Data quality is excellent - maintain current standards" in recommendations
        )

    def test_generate_recommendations_default_case(self, temp_dir):
        """Test default recommendations when no specific issues."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        summary = {
            "success_rate": 85.0,
            "validator_breakdown": {},
            "severity_breakdown": {},
        }
        failed_results = []

        # Act
        recommendations = generator._generate_recommendations(summary, failed_results)

        # Assert
        assert "Continue monitoring data quality trends" in recommendations
        assert "Consider implementing automated data quality checks" in recommendations

    def test_create_summary_content_no_metadata(self, temp_dir, sample_results):
        """Test summary content without metadata."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        summary = generator._analyze_results(sample_results)

        # Act
        content = generator._create_summary_content(
            sample_results, "test_table", summary, None
        )

        # Assert
        assert "DATA QUALITY SUMMARY REPORT" in content
        assert "test_table" in content
        assert "Metadata:" not in content

    def test_create_summary_content_no_failed_results(self, temp_dir):
        """Test summary content with all passing results."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)

        passing_results = [
            ValidationResult(
                rule_name="test_check",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="All good",
                details={},
                timestamp=datetime.now(),
                affected_rows=0,
                total_rows=100,
            )
        ]
        summary = generator._analyze_results(passing_results)

        # Act
        content = generator._create_summary_content(
            passing_results, "test_table", summary
        )

        # Assert
        assert "DATA QUALITY SUMMARY REPORT" in content
        assert "TOP ISSUES" not in content  # No failed results, so no issues section

    def test_full_report_integration(self, temp_dir, sample_results):
        """Test full report generation integration."""
        # Arrange
        generator = SummaryReportGenerator(temp_dir)
        metadata = {"database": "test_db", "analysis_date": "2023-01-01"}

        # Act
        output_path = generator.generate_report(
            sample_results, "integration_test", metadata
        )

        # Assert
        assert output_path.exists()
        content = output_path.read_text(encoding="utf-8")

        # Check that all main sections are present
        assert "DATA QUALITY SUMMARY REPORT" in content
        assert "integration_test" in content
        assert "OVERALL SUMMARY" in content
        assert "VALIDATOR BREAKDOWN" in content
        assert "SEVERITY BREAKDOWN" in content
        assert "TOP ISSUES" in content
        assert "RECOMMENDATIONS" in content

        # Check that all sample results are referenced
        for result in sample_results:
            if not result.passed:  # Only failed results appear in issues
                assert result.rule_name in content
                assert result.message in content

        # Check metadata is included
        assert str(metadata) in content

        # Check footer
        assert "Report generated by Data Quality Tool" in content
