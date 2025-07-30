"""Tests for IntegrityValidator following Triple A pattern."""

import pandas as pd
from unittest.mock import Mock

from data_quality.validators.base import ValidationRule, ValidationSeverity
from data_quality.validators.integrity import IntegrityValidator


class TestIntegrityValidator:
    """Test IntegrityValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        # Arrange & Act
        validator = IntegrityValidator()

        # Assert
        assert validator.name == "integrity"
        assert "referential integrity" in validator.description.lower()

    def test_validate_foreign_key_with_valid_references(self):
        """Test validating foreign key with all valid references."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="valid_fk",
            description="All foreign keys must exist in parent table",
            severity=ValidationSeverity.ERROR,
            parameters={
                "foreign_key": "cliente_uid",
                "reference_table": "cliente",
                "reference_column": "uid",
                "reference_data": pd.DataFrame(
                    {"uid": ["client_1", "client_2", "client_3"]}
                ),
            },
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "cliente_uid": ["client_1", "client_2", "client_3"],
                "amount": [100, 200, 300],
            }
        )

        # Act
        results = validator.validate_table(data, "orders", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.rule_name == "valid_fk"
        assert result.affected_rows == 0
        assert result.total_rows == 3

    def test_validate_foreign_key_with_invalid_references(self):
        """Test validating foreign key with some invalid references."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="fk_violation",
            description="Check for orphaned records",
            severity=ValidationSeverity.CRITICAL,
            parameters={
                "foreign_key": "cliente_uid",
                "reference_table": "cliente",
                "reference_column": "uid",
                "reference_data": pd.DataFrame(
                    {"uid": ["client_1", "client_2"]}  # client_3 missing
                ),
            },
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "cliente_uid": ["client_1", "client_2", "client_3", "client_invalid"],
                "amount": [100, 200, 300, 400],
            }
        )

        # Act
        results = validator.validate_table(data, "orders", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.severity == ValidationSeverity.CRITICAL
        assert result.affected_rows == 2  # client_3 and client_invalid
        assert result.total_rows == 4
        assert "2 orphaned records" in result.message

    def test_validate_foreign_key_with_nulls_allowed(self):
        """Test validating foreign key allowing null values."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="fk_with_nulls",
            description="Foreign key with nullable reference",
            severity=ValidationSeverity.WARNING,
            parameters={
                "foreign_key": "cliente_uid",
                "reference_table": "cliente",
                "reference_column": "uid",
                "allow_nulls": True,
                "reference_data": pd.DataFrame({"uid": ["client_1", "client_2"]}),
            },
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "cliente_uid": ["client_1", None, "client_2", None],
                "amount": [100, 200, 300, 400],
            }
        )

        # Act
        results = validator.validate_table(data, "orders", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.affected_rows == 0  # Nulls are allowed

    def test_validate_foreign_key_with_nulls_not_allowed(self):
        """Test validating foreign key not allowing null values."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="fk_no_nulls",
            description="Foreign key must not be null",
            severity=ValidationSeverity.ERROR,
            parameters={
                "foreign_key": "cliente_uid",
                "reference_table": "cliente",
                "reference_column": "uid",
                "allow_nulls": False,
                "reference_data": pd.DataFrame({"uid": ["client_1", "client_2"]}),
            },
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "cliente_uid": ["client_1", None, "client_2"],
                "amount": [100, 200, 300],
            }
        )

        # Act
        results = validator.validate_table(data, "orders", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 1  # 1 null value
        assert "null values" in result.message.lower()

    def test_validate_composite_foreign_key(self):
        """Test validating composite foreign key."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="composite_fk",
            description="Composite foreign key validation",
            severity=ValidationSeverity.ERROR,
            parameters={
                "foreign_key": ["emp_id", "cliente_id"],
                "reference_table": "empresa_cliente",
                "reference_column": ["emp_id", "cliente_id"],
                "reference_data": pd.DataFrame(
                    {
                        "emp_id": ["emp_1", "emp_1", "emp_2"],
                        "cliente_id": ["client_1", "client_2", "client_1"],
                    }
                ),
            },
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "emp_id": ["emp_1", "emp_1", "emp_2", "emp_3"],
                "cliente_id": ["client_1", "client_2", "client_1", "client_1"],
                "amount": [100, 200, 300, 400],
            }
        )

        # Act
        results = validator.validate_table(data, "transactions", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 1  # (emp_3, client_1) doesn't exist

    def test_validate_with_database_connector(self):
        """Test validation using database connector to fetch reference data."""
        # Arrange
        validator = IntegrityValidator()
        mock_connector = Mock()
        mock_connector.execute_query.return_value = pd.DataFrame(
            {"uid": ["client_1", "client_2"]}
        )

        rule = ValidationRule(
            name="db_fk_check",
            description="FK validation with database lookup",
            severity=ValidationSeverity.ERROR,
            parameters={
                "foreign_key": "cliente_uid",
                "reference_table": "cliente",
                "reference_column": "uid",
                "connector": mock_connector,
            },
        )

        data = pd.DataFrame({"cliente_uid": ["client_1", "client_2", "client_invalid"]})

        # Act
        results = validator.validate_table(data, "orders", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 1
        mock_connector.execute_query.assert_called_once()

    def test_validate_circular_references(self):
        """Test handling of circular reference scenarios."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="self_reference",
            description="Self-referencing foreign key",
            severity=ValidationSeverity.WARNING,
            parameters={
                "foreign_key": "parent_id",
                "reference_table": "categories",
                "reference_column": "id",
                "allow_self_reference": True,
                "reference_data": pd.DataFrame({"id": [1, 2, 3, 4]}),
            },
        )

        data = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "parent_id": [None, 1, 1, 2],  # Self-referencing hierarchy
                "name": ["Root", "Child1", "Child2", "Grandchild"],
            }
        )

        # Act
        results = validator.validate_table(data, "categories", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.affected_rows == 0

    def test_detailed_results_information(self):
        """Test that results contain detailed information."""
        # Arrange
        validator = IntegrityValidator()
        rule = ValidationRule(
            name="detailed_fk",
            description="Detailed FK validation",
            severity=ValidationSeverity.ERROR,
            parameters={
                "foreign_key": "cliente_uid",
                "reference_table": "cliente",
                "reference_column": "uid",
                "reference_data": pd.DataFrame({"uid": ["client_1", "client_2"]}),
            },
        )

        data = pd.DataFrame(
            {"id": [1, 2, 3], "cliente_uid": ["client_1", "client_invalid", "client_2"]}
        )

        # Act
        results = validator.validate_table(data, "orders", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]

        details = result.details
        assert "total_references" in details
        assert "valid_references" in details
        assert "invalid_references" in details
        assert "orphaned_values" in details
        assert details["total_references"] == 3
        assert details["valid_references"] == 2
        assert details["invalid_references"] == 1
        assert "client_invalid" in details["orphaned_values"]

    def test_rule_parameters_validation(self):
        """Test that rule parameters are validated."""
        # Arrange
        validator = IntegrityValidator()
        invalid_rule = ValidationRule(
            name="missing_params",
            description="Rule with missing parameters",
            severity=ValidationSeverity.ERROR,
            parameters={},  # Missing required parameters
        )

        data = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        results = validator.validate_table(data, "test_table", [invalid_rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert not result.passed
        assert "foreign_key parameter is required" in result.message

    def test_validate_column_not_supported(self):
        """Test that column validation is not supported for integrity checks."""
        # Arrange
        validator = IntegrityValidator()
        data = pd.Series([1, 2, 3], name="test_column")

        # Act
        results = validator.validate_column(data, "test_table", "test_column")

        # Assert
        assert len(results) == 1
        result = results[0]
        assert not result.passed
        assert "not supported" in result.message.lower()
