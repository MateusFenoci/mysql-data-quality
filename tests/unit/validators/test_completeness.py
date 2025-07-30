"""Tests for CompletenessValidator following Triple A pattern."""

import pandas as pd
import pytest

from data_quality.validators.base import ValidationRule, ValidationSeverity
from data_quality.validators.completeness import CompletenessValidator


class TestCompletenessValidator:
    """Test CompletenessValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        # Arrange & Act
        validator = CompletenessValidator()

        # Assert
        assert validator.name == "completeness"
        assert "completeness" in validator.description.lower()

    def test_validate_column_with_no_nulls(self):
        """Test validating column with no null values."""
        # Arrange
        validator = CompletenessValidator()
        rule = ValidationRule(
            name="no_nulls",
            description="No null values allowed",
            severity=ValidationSeverity.ERROR,
            parameters={"threshold": 1.0},
        )
        validator.add_rule(rule)

        data = pd.Series([1, 2, 3, 4, 5], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.rule_name == "no_nulls"
        assert result.table_name == "test_table"
        assert result.column_name == "test_column"
        assert result.affected_rows == 0
        assert result.total_rows == 5
        assert result.pass_rate == 100.0

    def test_validate_column_with_nulls_below_threshold(self):
        """Test validating column with nulls below acceptable threshold."""
        # Arrange
        validator = CompletenessValidator()
        rule = ValidationRule(
            name="allow_some_nulls",
            description="Allow up to 20% nulls",
            severity=ValidationSeverity.WARNING,
            parameters={"threshold": 0.8},  # 80% completeness required
        )
        validator.add_rule(rule)

        data = pd.Series([1, 2, None, 4, 5], name="test_column")  # 20% nulls

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.affected_rows == 1
        assert result.total_rows == 5
        assert result.pass_rate == 80.0

    def test_validate_column_with_nulls_above_threshold(self):
        """Test validating column with nulls above acceptable threshold."""
        # Arrange
        validator = CompletenessValidator()
        rule = ValidationRule(
            name="strict_completeness",
            description="Require 90% completeness",
            severity=ValidationSeverity.ERROR,
            parameters={"threshold": 0.9},
        )
        validator.add_rule(rule)

        data = pd.Series([1, None, None, 4, 5], name="test_column")  # 40% nulls

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.severity == ValidationSeverity.ERROR
        assert result.affected_rows == 2
        assert result.total_rows == 5
        assert result.pass_rate == 60.0
        assert "60.0%" in result.message

    def test_validate_column_with_empty_series(self):
        """Test validating empty column."""
        # Arrange
        validator = CompletenessValidator()
        rule = ValidationRule(
            name="empty_check",
            description="Check empty column",
            severity=ValidationSeverity.WARNING,
            parameters={"threshold": 1.0},
        )

        data = pd.Series([], name="empty_column", dtype=object)

        # Act
        results = validator.validate_column(data, "test_table", "empty_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True  # 100% completeness on empty data
        assert result.total_rows == 0
        assert result.affected_rows == 0
        assert result.pass_rate == 100.0

    def test_validate_table_with_multiple_columns(self):
        """Test validating entire table with multiple columns."""
        # Arrange
        validator = CompletenessValidator()
        rule = ValidationRule(
            name="table_completeness",
            description="Check all columns for completeness",
            severity=ValidationSeverity.WARNING,
            parameters={"threshold": 0.8},
        )
        validator.add_rule(rule)

        data = pd.DataFrame(
            {
                "col1": [1, 2, 3, None, 5],  # 80% complete
                "col2": [1, None, None, 4, 5],  # 60% complete
                "col3": [1, 2, 3, 4, 5],  # 100% complete
            }
        )

        # Act
        results = validator.validate_table(data, "test_table", [rule])

        # Assert
        assert len(results) == 3  # One result per column

        # Check col1 (80% complete - passes)
        col1_result = next(r for r in results if r.column_name == "col1")
        assert col1_result.passed is True
        assert col1_result.pass_rate == 80.0

        # Check col2 (60% complete - fails)
        col2_result = next(r for r in results if r.column_name == "col2")
        assert col2_result.passed is False
        assert col2_result.pass_rate == 60.0

        # Check col3 (100% complete - passes)
        col3_result = next(r for r in results if r.column_name == "col3")
        assert col3_result.passed is True
        assert col3_result.pass_rate == 100.0

    def test_validate_table_with_no_rules(self):
        """Test validating table when explicit empty rules are provided."""
        # Arrange
        validator = CompletenessValidator()
        data = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        results = validator.validate_table(data, "test_table", rules=[])

        # Assert
        assert len(results) == 0

    def test_validate_with_default_rules(self):
        """Test validation using validator's default rules."""
        # Arrange
        validator = CompletenessValidator()
        # Default rule should be added during initialization

        data = pd.Series([1, 2, None, 4, 5], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column")

        # Assert
        assert len(results) >= 1  # Should have at least default rule

    def test_rule_parameters_validation(self):
        """Test that rule parameters are validated."""
        # Arrange
        validator = CompletenessValidator()
        invalid_rule = ValidationRule(
            name="invalid_threshold",
            description="Invalid threshold",
            severity=ValidationSeverity.ERROR,
            parameters={"threshold": 1.5},  # Invalid: > 1.0
        )

        data = pd.Series([1, 2, 3], name="test_column")

        # Act & Assert
        with pytest.raises(ValueError, match="threshold must be between 0.0 and 1.0"):
            validator.validate_column(data, "test_table", "test_column", [invalid_rule])

    def test_detailed_results_information(self):
        """Test that results contain detailed information."""
        # Arrange
        validator = CompletenessValidator()
        rule = ValidationRule(
            name="detailed_check",
            description="Detailed completeness check",
            severity=ValidationSeverity.INFO,
            parameters={"threshold": 0.8},
        )

        data = pd.Series([1, None, 3, None, 5], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]

        details = result.details
        assert "null_count" in details
        assert "non_null_count" in details
        assert "completeness_ratio" in details
        assert details["null_count"] == 2
        assert details["non_null_count"] == 3
        assert details["completeness_ratio"] == 0.6
