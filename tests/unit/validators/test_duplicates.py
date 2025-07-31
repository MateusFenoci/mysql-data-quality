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

    def test_load_patterns_from_env_with_values(self):
        """Test loading patterns from environment variables."""
        import os

        # Arrange
        original_skip = os.environ.get("SKIP_DUPLICATE_PATTERNS", "")
        original_unique = os.environ.get("FORCE_UNIQUE_PATTERNS", "")
        original_force_cols = os.environ.get("FORCE_UNIQUE_COLUMNS", "")
        original_allow_cols = os.environ.get("ALLOW_DUPLICATE_COLUMNS", "")

        try:
            os.environ["SKIP_DUPLICATE_PATTERNS"] = "test_skip,custom_pattern"
            os.environ["FORCE_UNIQUE_PATTERNS"] = "test_unique,custom_unique"
            os.environ["FORCE_UNIQUE_COLUMNS"] = "force_column"
            os.environ["ALLOW_DUPLICATE_COLUMNS"] = "allow_column"

            # Act
            validator = DuplicatesValidator()

            # Assert
            assert "test_skip" in validator._skip_patterns
            assert "custom_pattern" in validator._skip_patterns
            assert "test_unique" in validator._unique_patterns
            assert "custom_unique" in validator._unique_patterns
            assert "force_column" in validator._force_unique_columns
            assert "allow_column" in validator._allow_duplicate_columns

        finally:
            # Cleanup
            os.environ["SKIP_DUPLICATE_PATTERNS"] = original_skip
            os.environ["FORCE_UNIQUE_PATTERNS"] = original_unique
            os.environ["FORCE_UNIQUE_COLUMNS"] = original_force_cols
            os.environ["ALLOW_DUPLICATE_COLUMNS"] = original_allow_cols

    def test_load_patterns_from_env_empty_values(self):
        """Test loading patterns with empty environment variables (uses defaults)."""
        import os

        # Arrange
        original_skip = os.environ.get("SKIP_DUPLICATE_PATTERNS", "")
        original_unique = os.environ.get("FORCE_UNIQUE_PATTERNS", "")

        try:
            os.environ["SKIP_DUPLICATE_PATTERNS"] = ""
            os.environ["FORCE_UNIQUE_PATTERNS"] = ""

            # Act
            validator = DuplicatesValidator()

            # Assert - should have default patterns
            assert len(validator._skip_patterns) > 0
            assert len(validator._unique_patterns) > 0
            assert "cpf" in validator._unique_patterns  # Default pattern
            assert "cliente_id" in validator._skip_patterns  # Default pattern

        finally:
            # Cleanup
            os.environ["SKIP_DUPLICATE_PATTERNS"] = original_skip
            os.environ["FORCE_UNIQUE_PATTERNS"] = original_unique

    def test_should_skip_column_for_duplicates_force_unique(self):
        """Test intelligent pattern matching for force unique columns."""
        # Arrange
        validator = DuplicatesValidator()
        validator._force_unique_columns.add("specific_column")

        # Act & Assert
        assert not validator._should_skip_column_for_duplicates("specific_column")
        assert not validator._should_skip_column_for_duplicates(
            "cpf"
        )  # Should validate CPF
        assert not validator._should_skip_column_for_duplicates(
            "cnpj"
        )  # Should validate CNPJ

    def test_should_skip_column_for_duplicates_allow_duplicates(self):
        """Test intelligent pattern matching for allow duplicate columns."""
        # Arrange
        validator = DuplicatesValidator()
        validator._allow_duplicate_columns.add("specific_column")

        # Act & Assert
        assert validator._should_skip_column_for_duplicates("specific_column")
        assert validator._should_skip_column_for_duplicates("cliente_id")  # FK pattern
        assert validator._should_skip_column_for_duplicates(
            "uuid_field"
        )  # UUID pattern
        assert validator._should_skip_column_for_duplicates(
            "status"
        )  # Categorical pattern

    def test_should_skip_column_for_duplicates_patterns(self):
        """Test intelligent pattern matching for various column types."""
        # Arrange
        validator = DuplicatesValidator()

        # Act & Assert - Columns that should be validated (unique patterns)
        assert not validator._should_skip_column_for_duplicates("cpf")
        assert not validator._should_skip_column_for_duplicates("cnpj")
        assert not validator._should_skip_column_for_duplicates("document_number")
        assert not validator._should_skip_column_for_duplicates("serial_code")
        assert not validator._should_skip_column_for_duplicates("barcode")

        # Columns that should be skipped (skip patterns)
        assert validator._should_skip_column_for_duplicates("cliente_id")
        assert validator._should_skip_column_for_duplicates("fk_user")
        assert validator._should_skip_column_for_duplicates("uuid_field")
        assert validator._should_skip_column_for_duplicates("nome")
        assert validator._should_skip_column_for_duplicates("endereco")
        assert validator._should_skip_column_for_duplicates("status")
        assert validator._should_skip_column_for_duplicates("categoria")

    def test_should_skip_column_default_behavior(self):
        """Test default behavior for columns that don't match any pattern."""
        # Arrange
        validator = DuplicatesValidator()

        # Act & Assert - Random column names should be validated by default
        assert not validator._should_skip_column_for_duplicates("random_column")
        assert not validator._should_skip_column_for_duplicates("some_field")
        assert not validator._should_skip_column_for_duplicates("unknown_col")

    def test_validate_table_with_intelligent_skipping(self):
        """Test table validation with intelligent column skipping."""
        # Arrange
        validator = DuplicatesValidator()
        data = pd.DataFrame(
            {
                "cpf": [
                    "123.456.789-01",
                    "987.654.321-02",
                    "123.456.789-01",
                ],  # Should validate
                "cliente_id": [1, 2, 1],  # Should skip (FK)
                "nome": ["João", "Maria", "João"],  # Should skip (descriptive)
                "unique_code": ["A001", "A002", "A003"],  # Should validate
            }
        )

        # Act
        results = validator.validate_table(data, "test_table")

        # Assert
        validated_columns = {r.column_name for r in results if r.column_name}

        # Should validate CPF and unique_code, skip cliente_id and nome
        assert "cpf" in validated_columns
        assert "unique_code" in validated_columns
        assert "cliente_id" not in validated_columns
        assert "nome" not in validated_columns

    def test_composite_key_validation_error_cases(self):
        """Test composite key validation error handling."""
        # Arrange
        validator = DuplicatesValidator()
        data = pd.DataFrame({"col1": [1, 2, 3], "col2": ["A", "B", "C"]})

        # Test missing parameters
        rule_no_params = ValidationRule(
            name="composite_test",
            description="Test composite key",
            severity=ValidationSeverity.ERROR,
            parameters={},  # Empty parameters instead of None
        )

        with pytest.raises(KeyError, match="columns"):
            validator._validate_composite_key(data, "test_table", rule_no_params)

        # Test missing columns
        rule_missing_cols = ValidationRule(
            name="composite_test",
            description="Test composite key",
            severity=ValidationSeverity.ERROR,
            parameters={"columns": ["col1", "missing_col"]},
        )

        with pytest.raises(ValueError, match="columns.*not found"):
            validator._validate_composite_key(data, "test_table", rule_missing_cols)

        # Test invalid max_duplicates
        rule_invalid_max = ValidationRule(
            name="composite_test",
            description="Test composite key",
            severity=ValidationSeverity.ERROR,
            parameters={"columns": ["col1", "col2"], "max_duplicates": -1},
        )

        with pytest.raises(ValueError, match="max_duplicates must be >= 0"):
            validator._validate_composite_key(data, "test_table", rule_invalid_max)

    def test_composite_key_validation_success(self):
        """Test successful composite key validation."""
        # Arrange
        validator = DuplicatesValidator()
        data = pd.DataFrame(
            {
                "col1": [1, 2, 1, 3],
                "col2": ["A", "B", "B", "C"],  # (1,A), (2,B), (1,B), (3,C) - all unique
            }
        )

        rule = ValidationRule(
            name="composite_test",
            description="Test composite key",
            severity=ValidationSeverity.ERROR,
            parameters={"columns": ["col1", "col2"], "max_duplicates": 0},
        )

        # Act
        result = validator._validate_composite_key(data, "test_table", rule)

        # Assert
        assert result.passed is True
        assert result.affected_rows == 0
        assert result.details["unique_combinations"] == 4
        assert result.details["duplicate_combinations"] == 0

    def test_composite_key_validation_with_duplicates(self):
        """Test composite key validation with duplicate combinations."""
        # Arrange
        validator = DuplicatesValidator()
        data = pd.DataFrame(
            {
                "col1": [1, 2, 1, 1],
                "col2": ["A", "B", "A", "A"],  # (1,A) appears twice - duplicate
            }
        )

        rule = ValidationRule(
            name="composite_test",
            description="Test composite key",
            severity=ValidationSeverity.ERROR,
            parameters={"columns": ["col1", "col2"], "max_duplicates": 0},
        )

        # Act
        result = validator._validate_composite_key(data, "test_table", rule)

        # Assert
        assert result.passed is False
        assert (
            result.affected_rows == 2
        )  # Two duplicate rows (3rd and 4th occurrence of (1,A))
        assert result.details["unique_combinations"] == 2  # [1,A] and [2,B]
        assert result.details["duplicate_combinations"] == 2
        assert len(result.details["sample_duplicates"]) > 0

    def test_composite_key_validation_ignore_nulls(self):
        """Test composite key validation ignoring null values."""
        # Arrange
        validator = DuplicatesValidator()
        data = pd.DataFrame({"col1": [1, 2, None, 1], "col2": ["A", "B", "C", "A"]})

        rule = ValidationRule(
            name="composite_test",
            description="Test composite key",
            severity=ValidationSeverity.ERROR,
            parameters={
                "columns": ["col1", "col2"],
                "max_duplicates": 0,
                "ignore_nulls": True,
            },
        )

        # Act
        result = validator._validate_composite_key(data, "test_table", rule)

        # Assert
        assert result.passed is False  # (1,A) appears twice
        assert result.details["total_combinations"] == 3  # Null row excluded
        assert result.details["duplicate_combinations"] == 1
