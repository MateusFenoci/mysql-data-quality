"""Completeness validator for checking null/missing values."""

from typing import List, Optional

import pandas as pd

from .base import (
    DataQualityValidator,
    ValidationResult,
    ValidationRule,
    ValidationSeverity,
)


class CompletenessValidator(DataQualityValidator):
    """Validator for checking data completeness (null/missing values).

    Follows Single Responsibility Principle - only validates completeness.
    """

    def __init__(self):
        """Initialize completeness validator with default configuration."""
        super().__init__(
            name="completeness",
            description="Validates data completeness by checking for null/missing values",
        )

        # Add default rule (Open/Closed Principle - extensible via rule addition)
        default_rule = ValidationRule(
            name="default_completeness",
            description="Default completeness check requiring 95% non-null values",
            severity=ValidationSeverity.WARNING,
            parameters={"threshold": 0.95},
        )
        self.add_rule(default_rule)

    def validate_table(
        self,
        data: pd.DataFrame,
        table_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate completeness for all columns in a table.

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
        """Validate completeness for a specific column.

        Args:
            data: Series to validate
            table_name: Name of the table containing the column
            column_name: Name of the column being validated
            rules: Optional list of validation rules to apply

        Returns:
            List of validation results for the column

        Raises:
            ValueError: If rule parameters are invalid
        """
        if rules is None:
            rules = self.get_rules()

        if not rules:
            return []

        results = []

        # Calculate completeness metrics once
        total_rows = len(data)
        null_count = data.isnull().sum()
        non_null_count = total_rows - null_count
        completeness_ratio = non_null_count / total_rows if total_rows > 0 else 1.0

        for rule in rules:
            if not rule.enabled:
                continue

            # Ensure parameters exist
            if rule.parameters is None:
                raise ValueError("Parameters are required for completeness validation")

            # Validate rule parameters (Fail Fast principle)
            threshold = rule.parameters.get("threshold", 1.0)
            if not (0.0 <= threshold <= 1.0):
                raise ValueError(
                    f"Rule '{rule.name}': threshold must be between 0.0 and 1.0, got {threshold}"
                )

            # Determine if validation passed
            passed = bool(completeness_ratio >= threshold)

            # Create detailed message
            if passed:
                message = f"Column '{column_name}' has {completeness_ratio:.1%} completeness (>= {threshold:.1%} required)"
            else:
                message = f"Column '{column_name}' has {completeness_ratio:.1%} completeness (< {threshold:.1%} required)"

            # Create detailed information for reporting
            details = {
                "null_count": int(null_count),
                "non_null_count": int(non_null_count),
                "completeness_ratio": float(completeness_ratio),
                "threshold": float(threshold),
                "null_percentage": float(
                    (null_count / total_rows * 100) if total_rows > 0 else 0
                ),
            }

            # Create validation result
            result = self._create_result(
                rule=rule,
                table_name=table_name,
                column_name=column_name,
                passed=passed,
                message=message,
                details=details,
                affected_rows=int(null_count),
                total_rows=int(total_rows),
            )

            results.append(result)

        return results
