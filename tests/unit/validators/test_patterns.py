"""Tests for PatternsValidator following Triple A pattern."""

import pandas as pd

from data_quality.validators.base import ValidationRule, ValidationSeverity
from data_quality.validators.patterns import PatternsValidator


class TestPatternsValidator:
    """Test PatternsValidator class."""

    def test_validator_initialization(self):
        """Test validator initialization."""
        # Arrange & Act
        validator = PatternsValidator()

        # Assert
        assert validator.name == "patterns"
        assert "pattern" in validator.description.lower()

    def test_validate_cnpj_valid_patterns(self):
        """Test validating valid CNPJ patterns."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="cnpj_validation",
            description="Validate CNPJ format",
            severity=ValidationSeverity.ERROR,
            parameters={"pattern_type": "cnpj"},
        )

        data = pd.Series(
            [
                "11.444.777/0001-61",  # Valid with mask
                "11444777000161",  # Valid without mask
                "11.222.333/0001-81",  # Valid with mask
            ],
            name="cnpj_column",
        )

        # Act
        results = validator.validate_column(data, "empresas", "cnpj_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.affected_rows == 0
        assert result.total_rows == 3

    def test_validate_cnpj_invalid_patterns(self):
        """Test validating invalid CNPJ patterns."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="cnpj_validation",
            description="Validate CNPJ format",
            severity=ValidationSeverity.ERROR,
            parameters={"pattern_type": "cnpj", "allow_nulls": False},
        )

        data = pd.Series(
            [
                "11.444.777/0001-61",  # Valid
                "123456789012345",  # Invalid length
                "11.444.777/0001-XX",  # Invalid characters
                "00.000.000/0000-00",  # Invalid check digits
                "",  # Empty
            ],
            name="cnpj_column",
        )

        # Act
        results = validator.validate_column(data, "empresas", "cnpj_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 4  # 4 invalid CNPJs
        assert result.total_rows == 5

    def test_validate_cpf_valid_patterns(self):
        """Test validating valid CPF patterns."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="cpf_validation",
            description="Validate CPF format",
            severity=ValidationSeverity.WARNING,
            parameters={"pattern_type": "cpf"},
        )

        data = pd.Series(
            [
                "123.456.789-09",  # Valid with mask
                "12345678909",  # Valid without mask
                "987.654.321-00",  # Valid with mask
            ],
            name="cpf_column",
        )

        # Act
        results = validator.validate_column(data, "pessoas", "cpf_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is True
        assert result.affected_rows == 0

    def test_validate_cpf_invalid_patterns(self):
        """Test validating invalid CPF patterns."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="cpf_validation",
            description="Validate CPF format",
            severity=ValidationSeverity.ERROR,
            parameters={"pattern_type": "cpf"},
        )

        data = pd.Series(
            [
                "123.456.789-09",  # Valid
                "12345678901234",  # Invalid length
                "123.456.789-XX",  # Invalid characters
                "111.111.111-11",  # Invalid (all same digits)
                "000.000.000-00",  # Invalid check digits
            ],
            name="cpf_column",
        )

        # Act
        results = validator.validate_column(data, "pessoas", "cpf_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 4  # 4 invalid CPFs

    def test_validate_email_patterns(self):
        """Test validating email patterns."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="email_validation",
            description="Validate email format",
            severity=ValidationSeverity.WARNING,
            parameters={"pattern_type": "email"},
        )

        data = pd.Series(
            [
                "user@example.com",
                "test.email@domain.co.uk",
                "invalid.email",  # Invalid - no @
                "user@",  # Invalid - no domain
                "@domain.com",  # Invalid - no user
                "valid@domain.com",
            ],
            name="email_column",
        )

        # Act
        results = validator.validate_column(data, "contacts", "email_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 3  # 3 invalid emails
        assert result.total_rows == 6

    def test_validate_phone_patterns(self):
        """Test validating phone patterns."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="phone_validation",
            description="Validate Brazilian phone format",
            severity=ValidationSeverity.INFO,
            parameters={"pattern_type": "phone_br"},
        )

        data = pd.Series(
            [
                "(11) 99999-9999",  # Valid mobile
                "(21) 3333-4444",  # Valid landline
                "11999999999",  # Valid without mask
                "123456789",  # Invalid length
                "(11) 99999-999X",  # Invalid characters
            ],
            name="phone_column",
        )

        # Act
        results = validator.validate_column(data, "contacts", "phone_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 2  # 2 invalid phones

    def test_validate_custom_regex_pattern(self):
        """Test validating custom regex pattern."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="custom_code",
            description="Validate custom code format",
            severity=ValidationSeverity.ERROR,
            parameters={
                "pattern_type": "regex",
                "regex_pattern": r"^[A-Z]{2}\d{4}$",
                "description": "Two letters followed by four digits",
            },
        )

        data = pd.Series(
            [
                "AB1234",  # Valid
                "XY5678",  # Valid
                "ab1234",  # Invalid - lowercase
                "ABC123",  # Invalid - too many letters
                "AB12345",  # Invalid - too many digits
            ],
            name="code_column",
        )

        # Act
        results = validator.validate_column(data, "codes", "code_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 3  # 3 invalid codes

    def test_validate_with_null_values(self):
        """Test validating data with null values."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="email_with_nulls",
            description="Email validation allowing nulls",
            severity=ValidationSeverity.WARNING,
            parameters={"pattern_type": "email", "allow_nulls": True},
        )

        data = pd.Series(
            ["valid@email.com", None, "invalid.email", None], name="email_column"
        )

        # Act
        results = validator.validate_column(data, "contacts", "email_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 1  # Only 1 invalid (nulls allowed)

    def test_validate_not_allowing_null_values(self):
        """Test validating data not allowing null values."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="email_no_nulls",
            description="Email validation not allowing nulls",
            severity=ValidationSeverity.ERROR,
            parameters={"pattern_type": "email", "allow_nulls": False},
        )

        data = pd.Series(
            ["valid@email.com", None, "another@valid.com"], name="email_column"
        )

        # Act
        results = validator.validate_column(data, "contacts", "email_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]
        assert result.passed is False
        assert result.affected_rows == 1  # 1 null value counted as invalid

    def test_validate_table_multiple_columns(self):
        """Test validating multiple columns in a table."""
        # Arrange
        validator = PatternsValidator()
        # Don't use default rules for this test
        validator._rules = []

        email_rule = ValidationRule(
            name="email_validation",
            description="Validate email",
            severity=ValidationSeverity.WARNING,
            parameters={"pattern_type": "email"},
        )
        phone_rule = ValidationRule(
            name="phone_validation",
            description="Validate phone",
            severity=ValidationSeverity.INFO,
            parameters={"pattern_type": "phone_br"},
        )
        validator.add_rule(email_rule)
        validator.add_rule(phone_rule)

        data = pd.DataFrame(
            {
                "email": ["valid@email.com", "invalid.email"],
                "phone": ["(11) 99999-9999", "123456789"],
                "name": ["John Doe", "Jane Smith"],  # Should be ignored
            }
        )

        # Act
        results = validator.validate_table(data, "contacts")

        # Assert
        # Should have results for email and phone columns, but not name
        email_results = [r for r in results if "email" in r.column_name]
        phone_results = [r for r in results if "phone" in r.column_name]
        name_results = [r for r in results if r.column_name == "name"]

        assert len(email_results) == 2  # 2 rules applied to email
        assert len(phone_results) == 2  # 2 rules applied to phone
        assert len(name_results) == 2  # 2 rules applied to name (no pattern matches)

    def test_detailed_results_information(self):
        """Test that results contain detailed information."""
        # Arrange
        validator = PatternsValidator()
        rule = ValidationRule(
            name="detailed_cnpj",
            description="Detailed CNPJ validation",
            severity=ValidationSeverity.ERROR,
            parameters={"pattern_type": "cnpj"},
        )

        data = pd.Series(
            [
                "11.444.777/0001-61",  # Valid
                "invalid-cnpj",  # Invalid
                "11.222.333/0001-81",  # Valid
            ],
            name="cnpj_column",
        )

        # Act
        results = validator.validate_column(data, "empresas", "cnpj_column", [rule])

        # Assert
        assert len(results) == 1
        result = results[0]

        details = result.details
        assert "pattern_type" in details
        assert "valid_count" in details
        assert "invalid_count" in details
        assert "invalid_values" in details
        assert details["pattern_type"] == "cnpj"
        assert details["valid_count"] == 2
        assert details["invalid_count"] == 1
        assert "invalid-cnpj" in details["invalid_values"]

    def test_rule_parameters_validation(self):
        """Test that rule parameters are validated."""
        # Arrange
        validator = PatternsValidator()
        invalid_rule = ValidationRule(
            name="invalid_pattern",
            description="Rule with invalid pattern type",
            severity=ValidationSeverity.ERROR,
            parameters={"pattern_type": "invalid_type"},
        )

        data = pd.Series(["test"], name="test_column")

        # Act
        results = validator.validate_column(
            data, "test_table", "test_column", [invalid_rule]
        )

        # Assert
        assert len(results) == 1
        result = results[0]
        assert not result.passed
        assert "unsupported pattern type" in result.message.lower()
