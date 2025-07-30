"""Patterns validator for checking data format patterns (CNPJ, CPF, email, etc.)."""

import re
from typing import List, Optional

import pandas as pd

from .base import (
    DataQualityValidator,
    ValidationResult,
    ValidationRule,
    ValidationSeverity,
)


class PatternsValidator(DataQualityValidator):
    """Validator for checking data format patterns.

    Follows Single Responsibility Principle - only validates format patterns.
    """

    def __init__(self):
        """Initialize patterns validator with default configuration."""
        super().__init__(
            name="patterns",
            description="Validates data format patterns (CNPJ, CPF, email, phone, etc.)",
        )

        # Define built-in patterns
        self._patterns = {
            "cnpj": {
                "regex": r"^\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2}$",
                "description": "Brazilian CNPJ format",
                "validator": self._validate_cnpj,
            },
            "cpf": {
                "regex": r"^\d{3}\.?\d{3}\.?\d{3}-?\d{2}$",
                "description": "Brazilian CPF format",
                "validator": self._validate_cpf,
            },
            "email": {
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
                "description": "Email format",
                "validator": None,
            },
            "phone_br": {
                "regex": r"^(\(\d{2}\)\s?)?\d{4,5}-?\d{4}$",
                "description": "Brazilian phone format",
                "validator": None,
            },
            "cep": {
                "regex": r"^\d{5}-?\d{3}$",
                "description": "Brazilian CEP format",
                "validator": None,
            },
        }

        # Add default rule for common patterns
        default_rule = ValidationRule(
            name="default_pattern_check",
            description="Default pattern validation",
            severity=ValidationSeverity.INFO,
            parameters={"pattern_type": "auto_detect", "allow_nulls": True},
        )
        self.add_rule(default_rule)

    def validate_table(
        self,
        data: pd.DataFrame,
        table_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate patterns for all columns in a table.

        Args:
            data: DataFrame to validate
            table_name: Name of the table being validated
            rules: Optional list of validation rules to apply

        Returns:
            List of validation results, one per column per rule
        """
        if rules is None:
            rules = self.get_rules()

        if not rules:
            return []

        results = []

        for column_name in data.columns:
            column_results = self.validate_column(
                data[column_name], table_name, column_name, rules
            )
            results.extend(column_results)

        return results

    def validate_column(
        self,
        data: pd.Series,
        table_name: str,
        column_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate patterns for a specific column.

        Args:
            data: Series to validate
            table_name: Name of the table containing the column
            column_name: Name of the column being validated
            rules: Optional list of validation rules to apply

        Returns:
            List of validation results for the column
        """
        if rules is None:
            rules = self.get_rules()

        if not rules:
            return []

        results = []

        for rule in rules:
            if not rule.enabled:
                continue

            try:
                result = self._validate_pattern(data, table_name, column_name, rule)
                results.append(result)
            except Exception as e:
                # Create error result for failed validation
                error_result = self._create_result(
                    rule=rule,
                    table_name=table_name,
                    column_name=column_name,
                    passed=False,
                    message=f"Pattern validation failed: {str(e)}",
                    details={"error": str(e)},
                    affected_rows=0,
                    total_rows=len(data),
                )
                results.append(error_result)

        return results

    def _validate_pattern(
        self, data: pd.Series, table_name: str, column_name: str, rule: ValidationRule
    ) -> ValidationResult:
        """Validate pattern for a column."""
        if rule.parameters is None:
            raise ValueError("Parameters are required for pattern validation")

        pattern_type = rule.parameters.get("pattern_type", "auto_detect")
        allow_nulls = rule.parameters.get("allow_nulls", True)

        # Handle auto-detection
        if pattern_type == "auto_detect":
            pattern_type = self._auto_detect_pattern(column_name)
            if not pattern_type:
                # No pattern detected, create info result
                return self._create_result(
                    rule=rule,
                    table_name=table_name,
                    column_name=column_name,
                    passed=True,
                    message=f"No specific pattern detected for column '{column_name}'",
                    details={
                        "pattern_type": "none",
                        "auto_detected": True,
                        "column_name": column_name,
                    },
                    affected_rows=0,
                    total_rows=len(data),
                )

        # Get pattern configuration
        if pattern_type == "regex":
            # Custom regex pattern
            if rule.parameters is None:
                raise ValueError("Parameters are required for regex pattern validation")

            regex_pattern = rule.parameters.get("regex_pattern")
            if not regex_pattern:
                raise ValueError(
                    "regex_pattern parameter is required for custom regex validation"
                )

            pattern_config = {
                "regex": regex_pattern,
                "description": rule.parameters.get(
                    "description", "Custom regex pattern"
                ),
                "validator": None,
            }
        elif pattern_type in self._patterns:
            pattern_config = self._patterns[pattern_type]
        else:
            raise ValueError(f"Unsupported pattern type: {pattern_type}")

        # Validate data against pattern
        valid_count = 0
        invalid_count = 0
        null_count = 0
        invalid_values = []

        for value in data:
            if pd.isna(value) or value == "":
                null_count += 1
                if not allow_nulls:
                    invalid_count += 1
                    invalid_values.append(str(value))
                else:
                    valid_count += 1
            else:
                # Convert to string for pattern matching
                str_value = str(value).strip()

                # Use custom validator if available, otherwise use regex
                if pattern_config["validator"]:
                    is_valid = pattern_config["validator"](str_value)
                else:
                    is_valid = bool(re.match(pattern_config["regex"], str_value))

                if is_valid:
                    valid_count += 1
                else:
                    invalid_count += 1
                    if len(invalid_values) < 10:  # Limit samples
                        invalid_values.append(str_value)

        # Determine if validation passed
        passed = invalid_count == 0

        # Create message
        if passed:
            if null_count > 0 and allow_nulls:
                message = f"All {valid_count} non-null values match {pattern_type} pattern ({null_count} nulls allowed)"
            else:
                message = f"All {valid_count} values match {pattern_type} pattern"
        else:
            issues = []
            if invalid_count > 0:
                issues.append(f"{invalid_count} invalid format")
            if null_count > 0 and not allow_nulls:
                issues.append(f"{null_count} null values")
            message = f"Pattern validation failed: {', '.join(issues)}"

        # Create detailed information
        details = {
            "pattern_type": pattern_type,
            "pattern_description": pattern_config["description"],
            "regex_pattern": pattern_config["regex"],
            "valid_count": int(valid_count),
            "invalid_count": int(invalid_count),
            "null_count": int(null_count),
            "allow_nulls": allow_nulls,
            "invalid_values": invalid_values,
            "validity_ratio": float(valid_count / len(data)) if len(data) > 0 else 1.0,
        }

        # Create validation result
        result = self._create_result(
            rule=rule,
            table_name=table_name,
            column_name=column_name,
            passed=passed,
            message=message,
            details=details,
            affected_rows=int(invalid_count),
            total_rows=int(len(data)),
        )

        return result

    def _auto_detect_pattern(self, column_name: str) -> Optional[str]:
        """Auto-detect pattern type based on column name."""
        column_lower = column_name.lower()

        if "cnpj" in column_lower:
            return "cnpj"
        elif "cpf" in column_lower:
            return "cpf"
        elif "email" in column_lower or "mail" in column_lower:
            return "email"
        elif (
            "phone" in column_lower
            or "telefone" in column_lower
            or "fone" in column_lower
        ):
            return "phone_br"
        elif "cep" in column_lower:
            return "cep"

        return None

    def _validate_cnpj(self, cnpj: str) -> bool:
        """Validate CNPJ with check digits."""
        # Remove formatting
        cnpj = re.sub(r"[^\d]", "", cnpj)

        # Check length
        if len(cnpj) != 14:
            return False

        # Check if all digits are the same
        if cnpj == cnpj[0] * 14:
            return False

        # Validate check digits
        def calculate_digit(cnpj_digits: str, weights: list[int]) -> int:
            total = sum(
                int(digit) * weight for digit, weight in zip(cnpj_digits, weights)
            )
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder

        # First check digit
        weights1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit1 = calculate_digit(cnpj[:12], weights1)

        if int(cnpj[12]) != digit1:
            return False

        # Second check digit
        weights2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
        digit2 = calculate_digit(cnpj[:13], weights2)

        return int(cnpj[13]) == digit2

    def _validate_cpf(self, cpf: str) -> bool:
        """Validate CPF with check digits."""
        # Remove formatting
        cpf = re.sub(r"[^\d]", "", cpf)

        # Check length
        if len(cpf) != 11:
            return False

        # Check if all digits are the same
        if cpf == cpf[0] * 11:
            return False

        # Validate check digits
        def calculate_digit(cpf_digits: str, weights: list[int]) -> int:
            total = sum(
                int(digit) * weight for digit, weight in zip(cpf_digits, weights)
            )
            remainder = total % 11
            return 0 if remainder < 2 else 11 - remainder

        # First check digit
        weights1 = list(range(10, 1, -1))
        digit1 = calculate_digit(cpf[:9], weights1)

        if int(cpf[9]) != digit1:
            return False

        # Second check digit
        weights2 = list(range(11, 1, -1))
        digit2 = calculate_digit(cpf[:10], weights2)

        return int(cpf[10]) == digit2
