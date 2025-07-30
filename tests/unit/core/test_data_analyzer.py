"""Tests for DataAnalyzer."""

import pandas as pd
from unittest.mock import Mock, patch

from src.data_quality.core.data_analyzer import DataAnalyzer
from src.data_quality.validators.base import ValidationResult, ValidationSeverity
from src.data_quality.validators.completeness import CompletenessValidator
from datetime import datetime


class TestDataAnalyzer:
    """Test cases for DataAnalyzer."""

    def test_init(self):
        """Test DataAnalyzer initialization."""
        # Act
        analyzer = DataAnalyzer()

        # Assert
        assert analyzer.engine is not None

    def test_register_validator(self):
        """Test validator registration."""
        # Arrange
        analyzer = DataAnalyzer()
        validator = CompletenessValidator()

        # Act
        analyzer.register_validator(validator)

        # Assert
        # ValidationEngine stores validators in _validators dict by name
        assert "completeness" in analyzer.engine._validators
        assert analyzer.engine._validators["completeness"] == validator

    @patch("src.data_quality.core.data_analyzer.Progress")
    def test_analyze_dataframe(self, mock_progress_class):
        """Test dataframe analysis."""
        # Arrange
        mock_progress = Mock()
        mock_progress_class.return_value.__enter__.return_value = mock_progress

        analyzer = DataAnalyzer()

        # Mock the validation engine
        mock_result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            column_name="test_col",
            severity=ValidationSeverity.INFO,
            passed=True,
            message="Test passed",
            details={},
            timestamp=datetime.now(),
            affected_rows=0,
            total_rows=3,
        )
        analyzer.engine.validate_data = Mock(return_value=[mock_result])

        data = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        results = analyzer.analyze_dataframe(data, "test_table")

        # Assert
        assert len(results) == 1
        assert results[0] == mock_result
        analyzer.engine.validate_data.assert_called_once_with(data, "test_table", None)

    def test_get_analysis_summary_empty_results(self):
        """Test analysis summary with empty results."""
        # Arrange
        analyzer = DataAnalyzer()

        # Act
        summary = analyzer.get_analysis_summary([])

        # Assert
        assert summary["total_validations"] == 0
        assert summary["passed_validations"] == 0
        assert summary["failed_validations"] == 0
        assert summary["success_rate"] == 100.0
        assert summary["critical_issues"] == 0
        assert summary["error_issues"] == 0
        assert summary["warning_issues"] == 0
        assert summary["info_issues"] == 0

    def test_get_analysis_summary_with_results(self):
        """Test analysis summary with mixed results."""
        # Arrange
        analyzer = DataAnalyzer()

        results = [
            ValidationResult(
                rule_name="test_rule_1",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.ERROR,
                passed=False,
                message="Error message",
                details={},
                timestamp=datetime.now(),
                affected_rows=1,
                total_rows=3,
            ),
            ValidationResult(
                rule_name="test_rule_2",
                table_name="test_table",
                column_name="col2",
                severity=ValidationSeverity.WARNING,
                passed=False,
                message="Warning message",
                details={},
                timestamp=datetime.now(),
                affected_rows=1,
                total_rows=3,
            ),
            ValidationResult(
                rule_name="test_rule_3",
                table_name="test_table",
                column_name="col3",
                severity=ValidationSeverity.INFO,
                passed=True,
                message="Info message",
                details={},
                timestamp=datetime.now(),
                affected_rows=0,
                total_rows=3,
            ),
        ]

        # Act
        summary = analyzer.get_analysis_summary(results)

        # Assert
        assert summary["total_validations"] == 3
        assert summary["passed_validations"] == 1
        assert summary["failed_validations"] == 2
        assert abs(summary["success_rate"] - 33.33) < 0.1  # 1/3 * 100
        assert summary["critical_issues"] == 0
        assert summary["error_issues"] == 1
        assert summary["warning_issues"] == 1
        assert summary["info_issues"] == 0

    def test_get_analysis_summary_all_critical(self):
        """Test analysis summary with all critical failures."""
        # Arrange
        analyzer = DataAnalyzer()

        results = [
            ValidationResult(
                rule_name="critical_rule",
                table_name="test_table",
                column_name="col1",
                severity=ValidationSeverity.CRITICAL,
                passed=False,
                message="Critical error",
                details={},
                timestamp=datetime.now(),
                affected_rows=10,
                total_rows=10,
            )
        ]

        # Act
        summary = analyzer.get_analysis_summary(results)

        # Assert
        assert summary["total_validations"] == 1
        assert summary["passed_validations"] == 0
        assert summary["failed_validations"] == 1
        assert summary["success_rate"] == 0.0
        assert summary["critical_issues"] == 1
        assert summary["error_issues"] == 0
        assert summary["warning_issues"] == 0
        assert summary["info_issues"] == 0
