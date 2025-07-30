"""Tests for DuplicatesValidator following Triple A pattern."""

import pandas as pd
import pytest

from data_quality.validators.base import ValidationRule, ValidationSeverity
from data_quality.validators.duplicates import DuplicatesValidator


class TestDuplicatesValidator:
    """Test DuplicatesValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        # Arrange & Act
        validator = DuplicatesValidator()

        # Assert
        assert validator.name == "duplicates"
        assert "duplicate" in validator.description.lower()

    def test_validate_column_with_no_duplicates(self):
        """Test validating column with no duplicate values."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="no_duplicates",
            description="No duplicate values allowed",
            severity=ValidationSeverity.ERROR,
            parameters={"max_duplicates": 0},
        )

        data = pd.Series([1, 2, 3, 4, 5], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.rule_name == "no_duplicates"
        assert result.affected_rows == 0
        assert result.total_rows == 5
        assert result.pass_rate == 100.0

    def test_validate_column_with_duplicates_below_threshold(self):
        """Test validating column with duplicates below acceptable threshold."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="allow_some_duplicates",
            description="Allow up to 2 duplicate values",
            severity=ValidationSeverity.WARNING,
            parameters={"max_duplicates": 2},
        )

        data = pd.Series([1, 2, 2, 3, 4], name="test_column")  # 1 duplicate value

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.affected_rows == 1  # 1 duplicate row (second "2")

    def test_validate_column_with_duplicates_above_threshold(self):
        """Test validating column with duplicates above acceptable threshold."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="strict_uniqueness",
            description="Allow only 1 duplicate value",
            severity=ValidationSeverity.ERROR,
            parameters={"max_duplicates": 1},
        )

        data = pd.Series([1, 1, 2, 2, 3], name="test_column")  # 2 duplicate values

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.severity == ValidationSeverity.ERROR
        assert result.affected_rows == 2  # 2 duplicate rows
        assert "2 duplicate" in result.message

    def test_validate_column_with_all_duplicates(self):
        """Test validating column where all values are the same."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="no_duplicates",
            description="No duplicates allowed",
            severity=ValidationSeverity.ERROR,
            parameters={"max_duplicates": 0},
        )

        data = pd.Series([1, 1, 1, 1, 1], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 4  # 4 duplicate rows (all but first)
        assert result.total_rows == 5
        assert result.pass_rate == 20.0  # Only 1 unique value

    def test_validate_table_composite_key_duplicates(self):
        """Test validating table for composite key duplicates."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="composite_key_unique",
            description="Composite key should be unique",
            severity=ValidationSeverity.ERROR,
            parameters={"columns": ["col1", "col2"], "max_duplicates": 0},
        )

        data = pd.DataFrame(
            {
                "col1": [1, 1, 2, 3, 1],
                "col2": ["A", "B", "A", "A", "A"],  # (1,A) appears twice
                "col3": ["X", "Y", "Z", "W", "V"],
            }
        )

        # Act
        results = validator.validate_table(data, "test_table", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.column_name is None  # Composite key validation
        assert result.affected_rows == 1  # 1 duplicate combination
        assert "(1, A)" in result.message or "1 duplicate" in result.message

    def test_validate_table_primary_key_duplicates(self):
        """Test validating table for primary key duplicates."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="primary_key_unique",
            description="Primary key must be unique",
            severity=ValidationSeverity.CRITICAL,
            parameters={"columns": ["id"], "max_duplicates": 0},
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 2, 3, 4],  # ID 2 appears twice
                "name": ["A", "B", "C", "D", "E"],
            }
        )

        # Act
        results = validator.validate_table(data, "test_table", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.severity == ValidationSeverity.CRITICAL
        assert result.affected_rows == 1  # 1 duplicate ID

    def test_validate_with_null_values(self):
        """Test validating data containing null values."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="nulls_duplicates",
            description="Check duplicates including nulls",
            severity=ValidationSeverity.WARNING,
            parameters={"max_duplicates": 0, "ignore_nulls": False},
        )

        data = pd.Series([1, None, None, 2, 3], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False  # Null values are duplicated
        assert (
            result.affected_rows == 1
        )  # 1 duplicate (2 nulls = 1 unique + 1 duplicate)

    def test_validate_ignore_null_values(self):
        """Test validating data ignoring null values."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="ignore_nulls",
            description="Check duplicates ignoring nulls",
            severity=ValidationSeverity.WARNING,
            parameters={"max_duplicates": 0, "ignore_nulls": True},
        )

        data = pd.Series([1, None, None, 2, 3], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True  # Nulls are ignored
        assert result.affected_rows == 0

    def test_validate_with_default_rules(self):
        """Test validation using validator's default rules."""
        # Arrange
        validator = DuplicatesValidator()
        data = pd.Series([1, 1, 2, 3, 4], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column")

        # Assert
        assert len(results) >= 1  # Should have at least default rule
        assert any(not r.passed for r in results)  # Should detect duplicates

    def test_detailed_results_information(self):
        """Test that results contain detailed information."""
        # Arrange
        validator = DuplicatesValidator()
        rule = ValidationRule(
            name="detailed_check",
            description="Detailed duplicates check",
            severity=ValidationSeverity.INFO,
            parameters={"max_duplicates": 1},
        )

        data = pd.Series([1, 2, 2, 3, 3, 3], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]

        details = result.details
        assert "unique_count" in details
        assert "duplicate_count" in details
        assert "duplicate_values" in details
        assert details["unique_count"] == 3  # Values: 1, 2, 3
        assert (
            details["duplicate_count"] == 3
        )  # Duplicate rows: 2nd "2", 2nd "3", 3rd "3"

    def test_rule_parameters_validation(self):
        """Test that rule parameters are validated."""
        # Arrange
        validator = DuplicatesValidator()
        invalid_rule = ValidationRule(
            name="invalid_max",
            description="Invalid max duplicates",
            severity=ValidationSeverity.ERROR,
            parameters={"max_duplicates": -1},  # Invalid: negative
        )

        data = pd.Series([1, 2, 3], name="test_column")

        # Act & Assert
        with pytest.raises(ValueError, match="max_duplicates must be >= 0"):
            validator.validate_column(data, "test_table", "test_column", [invalid_rule])
