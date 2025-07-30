"""Duplicates validator for checking duplicate values."""

from typing import List, Optional

import pandas as pd

from .base import (
    DataQualityValidator,
    ValidationResult,
    ValidationRule,
    ValidationSeverity,
)


class DuplicatesValidator(DataQualityValidator):
    """Validator for checking duplicate values in data.

    Follows Single Responsibility Principle - only validates duplicates.
    """

    def __init__(self):
        """Initialize duplicates validator with default configuration."""
        super().__init__(
            name="duplicates",
            description="Validates data uniqueness by checking for duplicate values",
        )

        # Add default rule (Open/Closed Principle - extensible via rule addition)
        default_rule = ValidationRule(
            name="default_uniqueness",
            description="Default uniqueness check - no duplicate values allowed",
            severity=ValidationSeverity.ERROR,
            parameters={"max_duplicates": 0, "ignore_nulls": True},
        )
        self.add_rule(default_rule)

    def validate_table(
        self,
        data: pd.DataFrame,
        table_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate duplicates for table (composite keys or all columns).

        Args:
            data: DataFrame to validate
            table_name: Name of the table being validated
            rules: Optional list of validation rules to apply

        Returns:
            List of validation results
        """
        if rules is None:
            rules = self.get_rules()

        if not rules:
            return []

        results = []

        for rule in rules:
            if not rule.enabled:
                continue

            # Ensure parameters exist
            if rule.parameters is None:
                raise ValueError("Parameters are required for duplicate validation")

            # Check if rule specifies columns for composite key validation
            if "columns" in rule.parameters:
                composite_result = self._validate_composite_key(data, table_name, rule)
                results.append(composite_result)
            else:
                # Validate each column individually
                for column_name in data.columns:
                    column_results = self.validate_column(
                        data[column_name], table_name, column_name, [rule]
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
        """Validate duplicates for a specific column.

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

        for rule in rules:
            if not rule.enabled:
                continue

            # Ensure parameters exist
            if rule.parameters is None:
                raise ValueError("Parameters are required for duplicate validation")

            # Skip composite key rules in column validation
            if "columns" in rule.parameters:
                continue

            # Validate rule parameters (Fail Fast principle)
            max_duplicates = rule.parameters.get("max_duplicates", 0)
            ignore_nulls = rule.parameters.get("ignore_nulls", True)

            if not isinstance(max_duplicates, int) or max_duplicates < 0:
                raise ValueError(
                    f"Rule '{rule.name}': max_duplicates must be >= 0, got {max_duplicates}"
                )

            # Calculate duplicate metrics
            if ignore_nulls:
                non_null_data = data.dropna()
                unique_count = non_null_data.nunique()
                total_count = len(non_null_data)
            else:
                unique_count = data.nunique(
                    dropna=False
                )  # Include NaNs in unique count
                total_count = len(data)

            duplicate_count = total_count - unique_count

            # Determine if validation passed
            passed = bool(duplicate_count <= max_duplicates)

            # Create detailed message
            if passed:
                if duplicate_count == 0:
                    message = f"Column '{column_name}' has no duplicate values"
                else:
                    message = f"Column '{column_name}' has {duplicate_count} duplicate values (<= {max_duplicates} allowed)"
            else:
                message = f"Column '{column_name}' has {duplicate_count} duplicate values (> {max_duplicates} allowed)"

            # Get duplicate values for detailed reporting
            if ignore_nulls:
                working_data = data.dropna()
            else:
                working_data = data

            duplicate_values = []
            if duplicate_count > 0:
                value_counts = working_data.value_counts()
                duplicate_values = value_counts[value_counts > 1].index.tolist()
                # Convert to native Python types for JSON serialization
                duplicate_values = [
                    val.item() if hasattr(val, "item") else val
                    for val in duplicate_values
                ]

            # Create detailed information for reporting
            details = {
                "unique_count": int(unique_count),
                "duplicate_count": int(duplicate_count),
                "total_rows": int(len(data)),
                "non_null_rows": int(len(data.dropna())),
                "duplicate_values": duplicate_values[
                    :10
                ],  # Limit to first 10 for performance
                "max_duplicates": int(max_duplicates),
                "ignore_nulls": bool(ignore_nulls),
            }

            # Create validation result
            result = self._create_result(
                rule=rule,
                table_name=table_name,
                column_name=column_name,
                passed=passed,
                message=message,
                details=details,
                affected_rows=int(duplicate_count),
                total_rows=int(len(data)),
            )

            results.append(result)

        return results

    def _validate_composite_key(
        self, data: pd.DataFrame, table_name: str, rule: ValidationRule
    ) -> ValidationResult:
        """Validate composite key uniqueness."""
        if rule.parameters is None:
            raise ValueError("Parameters are required for composite key validation")

        columns = rule.parameters["columns"]
        max_duplicates = rule.parameters.get("max_duplicates", 0)
        ignore_nulls = rule.parameters.get("ignore_nulls", True)

        # Validate parameters
        if not isinstance(max_duplicates, int) or max_duplicates < 0:
            raise ValueError(
                f"Rule '{rule.name}': max_duplicates must be >= 0, got {max_duplicates}"
            )

        if not all(col in data.columns for col in columns):
            missing_cols = [col for col in columns if col not in data.columns]
            raise ValueError(
                f"Rule '{rule.name}': columns {missing_cols} not found in data"
            )

        # Get subset of data for composite key
        key_data = data[columns]

        if ignore_nulls:
            # Remove rows with any null values in the key columns
            key_data = key_data.dropna()

        # Check for duplicates
        total_rows = len(key_data)
        unique_combinations = key_data.drop_duplicates()
        unique_count = len(unique_combinations)
        duplicate_count = total_rows - unique_count

        # Determine if validation passed
        passed = bool(duplicate_count <= max_duplicates)

        # Create message
        columns_str = ", ".join(columns)
        if passed:
            if duplicate_count == 0:
                message = f"Composite key ({columns_str}) has no duplicate combinations"
            else:
                message = f"Composite key ({columns_str}) has {duplicate_count} duplicate combinations (<= {max_duplicates} allowed)"
        else:
            message = f"Composite key ({columns_str}) has {duplicate_count} duplicate combinations (> {max_duplicates} allowed)"

        # Get sample duplicate combinations
        duplicate_combinations = []
        if duplicate_count > 0:
            # Find duplicate rows
            is_duplicate = key_data.duplicated(keep=False)
            duplicate_rows = key_data[is_duplicate]

            # Get unique duplicate combinations (limit to 5 for reporting)
            for _, row in duplicate_rows.drop_duplicates().head(5).iterrows():
                combo = tuple(row.values)
                # Convert to native Python types
                combo = tuple(
                    val.item() if hasattr(val, "item") else val for val in combo
                )
                duplicate_combinations.append(combo)

        # Create detailed information
        details = {
            "composite_key_columns": columns,
            "unique_combinations": int(unique_count),
            "duplicate_combinations": int(duplicate_count),
            "total_combinations": int(total_rows),
            "sample_duplicates": duplicate_combinations,
            "max_duplicates": int(max_duplicates),
            "ignore_nulls": bool(ignore_nulls),
        }

        # Create validation result
        result = self._create_result(
            rule=rule,
            table_name=table_name,
            column_name=None,  # Composite key spans multiple columns
            passed=passed,
            message=message,
            details=details,
            affected_rows=int(duplicate_count),
            total_rows=int(len(data)),
        )

        return result
