"""Tests for base validator classes following Triple A pattern."""

from datetime import datetime
from unittest.mock import Mock

import pandas as pd

from data_quality.validators.base import (
    DataQualityValidator,
    ValidationEngine,
    ValidationResult,
    ValidationRule,
    ValidationSeverity,
)


class ConcreteValidator(DataQualityValidator):
    """Concrete implementation for testing abstract base class."""

    def validate_table(self, data, table_name, rules=None):
        """Test implementation."""
        return []

    def validate_column(self, data, table_name, column_name, rules=None):
        """Test implementation."""
        return []


class TestValidationResult:
    """Test ValidationResult class."""

    def test_pass_rate_calculation_with_no_errors(self):
        """Test pass rate calculation when no errors found."""
        # Arrange
        result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            column_name="test_column",
            severity=ValidationSeverity.INFO,
            passed=True,
            message="All good",
            details={},
            timestamp=datetime.now(),
            affected_rows=0,
            total_rows=100,
        )

        # Act
        pass_rate = result.pass_rate

        # Assert
        assert pass_rate == 100.0

    def test_pass_rate_calculation_with_errors(self):
        """Test pass rate calculation when errors found."""
        # Arrange
        result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            column_name="test_column",
            severity=ValidationSeverity.ERROR,
            passed=False,
            message="Errors found",
            details={},
            timestamp=datetime.now(),
            affected_rows=25,
            total_rows=100,
        )

        # Act
        pass_rate = result.pass_rate

        # Assert
        assert pass_rate == 75.0

    def test_pass_rate_with_zero_total_rows(self):
        """Test pass rate when total rows is zero."""
        # Arrange
        result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            column_name="test_column",
            severity=ValidationSeverity.INFO,
            passed=True,
            message="Empty table",
            details={},
            timestamp=datetime.now(),
            affected_rows=0,
            total_rows=0,
        )

        # Act
        pass_rate = result.pass_rate

        # Assert
        assert pass_rate == 100.0

    def test_to_dict_serialization(self):
        """Test conversion to dictionary for serialization."""
        # Arrange
        timestamp = datetime.now()
        result = ValidationResult(
            rule_name="test_rule",
            table_name="test_table",
            column_name="test_column",
            severity=ValidationSeverity.WARNING,
            passed=False,
            message="Warning message",
            details={"key": "value"},
            timestamp=timestamp,
            affected_rows=10,
            total_rows=100,
        )

        # Act
        result_dict = result.to_dict()

        # Assert
        expected_dict = {
            "rule_name": "test_rule",
            "table_name": "test_table",
            "column_name": "test_column",
            "severity": "WARNING",
            "passed": False,
            "message": "Warning message",
            "details": {"key": "value"},
            "timestamp": timestamp.isoformat(),
            "affected_rows": 10,
            "total_rows": 100,
            "pass_rate": 90.0,
        }
        assert result_dict == expected_dict


class TestValidationRule:
    """Test ValidationRule class."""

    def test_rule_creation_with_defaults(self):
        """Test creating rule with default parameters."""
        # Arrange & Act
        rule = ValidationRule(
            name="test_rule",
            description="Test description",
            severity=ValidationSeverity.ERROR,
        )

        # Assert
        assert rule.name == "test_rule"
        assert rule.description == "Test description"
        assert rule.severity == ValidationSeverity.ERROR
        assert rule.enabled is True
        assert rule.parameters == {}

    def test_rule_creation_with_custom_parameters(self):
        """Test creating rule with custom parameters."""
        # Arrange
        custom_params = {"threshold": 0.95, "ignore_nulls": True}

        # Act
        rule = ValidationRule(
            name="custom_rule",
            description="Custom rule",
            severity=ValidationSeverity.WARNING,
            enabled=False,
            parameters=custom_params,
        )

        # Assert
        assert rule.name == "custom_rule"
        assert rule.enabled is False
        assert rule.parameters == custom_params


class TestDataQualityValidator:
    """Test DataQualityValidator abstract base class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        # Arrange & Act
        validator = ConcreteValidator("test_validator", "Test description")

        # Assert
        assert validator.name == "test_validator"
        assert validator.description == "Test description"
        assert len(validator.get_rules()) == 0

    def test_add_and_get_rules(self):
        """Test adding and retrieving rules."""
        # Arrange
        validator = ConcreteValidator("test_validator", "Test description")
        rule1 = ValidationRule("rule1", "First rule", ValidationSeverity.ERROR)
        rule2 = ValidationRule("rule2", "Second rule", ValidationSeverity.WARNING)

        # Act
        validator.add_rule(rule1)
        validator.add_rule(rule2)
        rules = validator.get_rules()

        # Assert
        assert len(rules) == 2
        assert rules[0].name == "rule1"
        assert rules[1].name == "rule2"

    def test_get_rules_returns_copy(self):
        """Test that get_rules returns a copy, not reference."""
        # Arrange
        validator = ConcreteValidator("test_validator", "Test description")
        rule = ValidationRule("rule1", "First rule", ValidationSeverity.ERROR)
        validator.add_rule(rule)

        # Act
        rules1 = validator.get_rules()
        rules2 = validator.get_rules()

        # Assert
        assert rules1 is not rules2  # Different objects
        assert len(rules1) == len(rules2) == 1

    def test_create_result_helper(self):
        """Test _create_result helper method."""
        # Arrange
        validator = ConcreteValidator("test_validator", "Test description")
        rule = ValidationRule("test_rule", "Test rule", ValidationSeverity.ERROR)

        # Act
        result = validator._create_result(
            rule=rule,
            table_name="test_table",
            column_name="test_column",
            passed=False,
            message="Test message",
            details={"detail": "value"},
            affected_rows=5,
            total_rows=100,
        )

        # Assert
        assert result.rule_name == "test_rule"
        assert result.table_name == "test_table"
        assert result.column_name == "test_column"
        assert result.severity == ValidationSeverity.ERROR
        assert result.passed is False
        assert result.message == "Test message"
        assert result.details == {"detail": "value"}
        assert result.affected_rows == 5
        assert result.total_rows == 100


class TestValidationEngine:
    """Test ValidationEngine class."""

    def test_engine_initialization(self):
        """Test validation engine initialization."""
        # Arrange & Act
        engine = ValidationEngine()

        # Assert
        assert len(engine.get_all_validators()) == 0

    def test_register_and_get_validator(self):
        """Test registering and retrieving validators."""
        # Arrange
        engine = ValidationEngine()
        validator = ConcreteValidator("test_validator", "Test description")

        # Act
        engine.register_validator(validator)
        retrieved_validator = engine.get_validator("test_validator")

        # Assert
        assert retrieved_validator is validator
        assert len(engine.get_all_validators()) == 1

    def test_get_nonexistent_validator(self):
        """Test getting validator that doesn't exist."""
        # Arrange
        engine = ValidationEngine()

        # Act
        validator = engine.get_validator("nonexistent")

        # Assert
        assert validator is None

    def test_validate_data_with_all_validators(self):
        """Test validating data with all registered validators."""
        # Arrange
        engine = ValidationEngine()
        mock_validator1 = Mock(spec=DataQualityValidator)
        mock_validator1.name = "validator1"
        mock_validator1.validate_table.return_value = [Mock(spec=ValidationResult)]

        mock_validator2 = Mock(spec=DataQualityValidator)
        mock_validator2.name = "validator2"
        mock_validator2.validate_table.return_value = [
            Mock(spec=ValidationResult),
            Mock(spec=ValidationResult),
        ]

        engine.register_validator(mock_validator1)
        engine.register_validator(mock_validator2)

        test_data = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        results = engine.validate_data(test_data, "test_table")

        # Assert
        assert len(results) == 3
        mock_validator1.validate_table.assert_called_once_with(test_data, "test_table")
        mock_validator2.validate_table.assert_called_once_with(test_data, "test_table")

    def test_validate_data_with_specific_validators(self):
        """Test validating data with specific validators."""
        # Arrange
        engine = ValidationEngine()
        mock_validator1 = Mock(spec=DataQualityValidator)
        mock_validator1.name = "validator1"
        mock_validator1.validate_table.return_value = []

        mock_validator2 = Mock(spec=DataQualityValidator)
        mock_validator2.name = "validator2"
        mock_validator2.validate_table.return_value = []

        engine.register_validator(mock_validator1)
        engine.register_validator(mock_validator2)

        test_data = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        results = engine.validate_data(test_data, "test_table", ["validator1"])

        # Assert
        assert results is not None  # Ensure results are returned
        mock_validator1.validate_table.assert_called_once()
        mock_validator2.validate_table.assert_not_called()

    def test_validate_data_handles_validator_exceptions(self):
        """Test that engine handles validator exceptions gracefully."""
        # Arrange
        engine = ValidationEngine()
        mock_validator = Mock(spec=DataQualityValidator)
        mock_validator.name = "failing_validator"
        mock_validator.validate_table.side_effect = Exception("Validator error")

        engine.register_validator(mock_validator)
        test_data = pd.DataFrame({"col1": [1, 2, 3]})

        # Act
        results = engine.validate_data(test_data, "test_table")

        # Assert
        assert len(results) == 1
        assert results[0].rule_name == "failing_validator_error"
        assert results[0].severity == ValidationSeverity.CRITICAL
        assert results[0].passed is False
        assert "Validator failing_validator failed" in results[0].message
