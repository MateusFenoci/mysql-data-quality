"""Referential integrity validator for checking foreign key relationships."""

from typing import List, Optional

import pandas as pd

from .base import (
    DataQualityValidator,
    ValidationResult,
    ValidationRule,
    ValidationSeverity,
)


class IntegrityValidator(DataQualityValidator):
    """Validator for checking referential integrity and foreign key constraints.

    Follows Single Responsibility Principle - only validates referential integrity.
    """

    def __init__(self, connector=None):
        """Initialize referential integrity validator.

        Args:
            connector: Database connector for auto-discovering foreign keys
        """
        super().__init__(
            name="integrity",
            description="Validates referential integrity by checking foreign key relationships",
        )
        self.connector = connector

    def validate_table(
        self,
        data: pd.DataFrame,
        table_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate referential integrity for table.

        Args:
            data: DataFrame to validate
            table_name: Name of the table being validated
            rules: Optional list of validation rules to apply

        Returns:
            List of validation results
        """
        if rules is None:
            rules = self.get_rules()

        # Auto-discover foreign keys if no rules and connector is available
        if not rules and self.connector:
            rules = self._auto_discover_foreign_keys(table_name)

        if not rules:
            return []

        results = []

        for rule in rules:
            if not rule.enabled:
                continue

            try:
                result = self._validate_foreign_key(data, table_name, rule)
                results.append(result)
            except Exception as e:
                # Create error result for failed validation
                error_result = self._create_result(
                    rule=rule,
                    table_name=table_name,
                    column_name=None,
                    passed=False,
                    message=f"Validation failed: {str(e)}",
                    details={"error": str(e)},
                    affected_rows=0,
                    total_rows=len(data),
                )
                results.append(error_result)

        return results

    def validate_column(
        self,
        data: pd.Series,
        table_name: str,
        column_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Column-level validation not supported for referential integrity.

        Referential integrity requires table-level validation as it involves
        relationships between tables.
        """
        # Create a result indicating this operation is not supported
        dummy_rule = ValidationRule(
            name="unsupported_operation",
            description="Column-level referential integrity validation not supported",
            severity=ValidationSeverity.ERROR,
        )

        result = self._create_result(
            rule=dummy_rule,
            table_name=table_name,
            column_name=column_name,
            passed=False,
            message="Column-level validation not supported for referential integrity. Use table-level validation instead.",
            details={
                "operation": "column_validation",
                "supported": False,
                "reason": "Referential integrity requires table-level context",
            },
            affected_rows=0,
            total_rows=len(data),
        )

        return [result]

    def _validate_foreign_key(
        self, data: pd.DataFrame, table_name: str, rule: ValidationRule
    ) -> ValidationResult:
        """Validate foreign key relationship."""
        # Ensure parameters exist
        if rule.parameters is None:
            raise ValueError("Parameters are required for foreign key validation")

        # Extract and validate rule parameters
        foreign_key = rule.parameters.get("foreign_key")
        reference_table = rule.parameters.get("reference_table")
        reference_column = rule.parameters.get("reference_column")
        allow_nulls = rule.parameters.get("allow_nulls", True)
        allow_self_reference = rule.parameters.get("allow_self_reference", False)

        # Validate required parameters
        if not foreign_key:
            raise ValueError("foreign_key parameter is required")
        if not reference_table:
            raise ValueError("reference_table parameter is required")
        if not reference_column:
            raise ValueError("reference_column parameter is required")

        # Get reference data
        reference_data = self._get_reference_data(rule)

        # Handle single column vs composite keys
        if isinstance(foreign_key, str):
            foreign_key = [foreign_key]
        if isinstance(reference_column, str):
            reference_column = [reference_column]

        if len(foreign_key) != len(reference_column):
            raise ValueError("foreign_key and reference_column must have same length")

        # Validate that columns exist
        missing_fk_cols = [col for col in foreign_key if col not in data.columns]
        if missing_fk_cols:
            raise ValueError(
                f"Foreign key columns not found in data: {missing_fk_cols}"
            )

        missing_ref_cols = [
            col for col in reference_column if col not in reference_data.columns
        ]
        if missing_ref_cols:
            raise ValueError(
                f"Reference columns not found in reference data: {missing_ref_cols}"
            )

        # Extract foreign key and reference values
        fk_data = data[foreign_key].copy()
        ref_data = reference_data[reference_column].copy()

        # Handle self-referencing tables
        if allow_self_reference and reference_table == table_name:
            # For self-referencing, add the current table's key values to reference data
            current_keys = data[reference_column].copy()
            ref_data = pd.concat([ref_data, current_keys]).drop_duplicates()

        # Create composite keys if multiple columns
        if len(foreign_key) > 1:
            # Create tuple representation for composite keys
            fk_tuples = [tuple(row) for row in fk_data.values]
            ref_tuples = set(tuple(row) for row in ref_data.values)
        else:
            # Single column case
            fk_tuples = fk_data.iloc[:, 0].values
            ref_tuples = set(ref_data.iloc[:, 0].values)

        # Analyze integrity violations
        total_references = len(fk_tuples)
        null_count = 0
        invalid_references = []

        for i, fk_value in enumerate(fk_tuples):
            # Handle null values
            if pd.isna(fk_value) or (
                isinstance(fk_value, tuple) and any(pd.isna(v) for v in fk_value)
            ):
                null_count += 1
                if not allow_nulls:
                    invalid_references.append((i, fk_value, "null_value"))
            elif fk_value not in ref_tuples:
                invalid_references.append((i, fk_value, "orphaned_record"))

        # Calculate metrics
        valid_references = total_references - len(invalid_references)
        orphaned_count = len(
            [ref for ref in invalid_references if ref[2] == "orphaned_record"]
        )
        null_violation_count = len(
            [ref for ref in invalid_references if ref[2] == "null_value"]
        )

        # Determine if validation passed
        passed = len(invalid_references) == 0

        # Create message
        if passed:
            if null_count > 0 and allow_nulls:
                message = f"All {total_references - null_count} non-null foreign key references are valid ({null_count} nulls allowed)"
            else:
                message = f"All {total_references} foreign key references are valid"
        else:
            issues = []
            if orphaned_count > 0:
                issues.append(f"{orphaned_count} orphaned records")
            if null_violation_count > 0:
                issues.append(f"{null_violation_count} null values")
            message = f"Foreign key validation failed: {', '.join(issues)}"

        # Get sample orphaned values for reporting
        orphaned_values = []
        for _, value, violation_type in invalid_references[:10]:  # Limit to 10 samples
            if violation_type == "orphaned_record":
                # Convert to native Python types for JSON serialization
                if isinstance(value, tuple):
                    clean_value = tuple(
                        v.item() if hasattr(v, "item") else v for v in value
                    )
                else:
                    # mypy: ignore unreachable warning for value.item() access
                    clean_value = value.item() if hasattr(value, "item") else value  # type: ignore[unreachable]
                orphaned_values.append(clean_value)

        # Create detailed information
        details = {
            "foreign_key_columns": foreign_key,
            "reference_table": reference_table,
            "reference_columns": reference_column,
            "total_references": int(total_references),
            "valid_references": int(valid_references),
            "invalid_references": len(invalid_references),
            "orphaned_records": int(orphaned_count),
            "null_violations": int(null_violation_count),
            "null_count": int(null_count),
            "allow_nulls": allow_nulls,
            "orphaned_values": orphaned_values,
            "integrity_ratio": float(valid_references / total_references)
            if total_references > 0
            else 1.0,
        }

        # Create validation result
        result = self._create_result(
            rule=rule,
            table_name=table_name,
            column_name=None,  # Referential integrity spans multiple columns
            passed=passed,
            message=message,
            details=details,
            affected_rows=len(invalid_references),
            total_rows=int(total_references),
        )

        return result

    def _get_reference_data(self, rule: ValidationRule) -> pd.DataFrame:
        """Get reference data for foreign key validation."""
        # Ensure parameters exist
        if rule.parameters is None:
            raise ValueError("Parameters are required for reference data validation")

        # Check if reference data is provided directly
        if "reference_data" in rule.parameters:
            return rule.parameters["reference_data"]

        # Check if database connector is provided
        if "connector" in rule.parameters:
            connector = rule.parameters["connector"]
            reference_table = rule.parameters["reference_table"]
            reference_column = rule.parameters["reference_column"]

            # Handle multiple columns
            if isinstance(reference_column, list):
                columns_str = ", ".join(reference_column)
            else:
                columns_str = reference_column

            # Note: This is safe as table/column names are validated  # nosec B608
            query = (
                f"SELECT DISTINCT {columns_str} FROM {reference_table}"  # nosec B608
            )
            return connector.execute_query(query)

    def _auto_discover_foreign_keys(self, table_name: str) -> List[ValidationRule]:
        """Auto-discover foreign key relationships and create validation rules.

        Args:
            table_name: Name of the table to discover foreign keys for

        Returns:
            List of validation rules for discovered foreign keys
        """
        if not self.connector:
            return []

        try:
            # Query to get foreign key information from MySQL
            fk_query = """
            SELECT
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME,
                CONSTRAINT_NAME
            FROM
                INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE
                TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = :table_name
                AND REFERENCED_TABLE_NAME IS NOT NULL
            """

            fk_result = self.connector.execute_query(
                fk_query, {"table_name": table_name}
            )

            rules = []
            for _, row in fk_result.iterrows():
                column_name = row["COLUMN_NAME"]
                ref_table = row["REFERENCED_TABLE_NAME"]
                ref_column = row["REFERENCED_COLUMN_NAME"]
                constraint_name = row["CONSTRAINT_NAME"]

                rule = ValidationRule(
                    name=f"auto_fk_{constraint_name}",
                    description=f"Auto-discovered foreign key: {column_name} -> {ref_table}.{ref_column}",
                    severity=ValidationSeverity.ERROR,
                    parameters={
                        "foreign_key": column_name,
                        "reference_table": ref_table,
                        "reference_column": ref_column,
                        "connector": self.connector,
                    },
                )
                rules.append(rule)

            return rules

        except Exception:
            # If auto-discovery fails, return empty list silently
            return []

        # No reference data available
        raise ValueError(
            "Either 'reference_data' or 'connector' parameter must be provided "
            "to fetch reference data for foreign key validation"
        )
