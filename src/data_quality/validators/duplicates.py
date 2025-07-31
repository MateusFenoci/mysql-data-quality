"""Duplicates validator for checking duplicate values."""

import os
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

        # Initialize custom column configurations first
        self._force_unique_columns = set()  # Columns that must be unique
        self._allow_duplicate_columns = set()  # Columns that can have duplicates

        # Load configuration from environment
        self._load_patterns_from_env()

        # Add default rule (Open/Closed Principle - extensible via rule addition)
        default_rule = ValidationRule(
            name="default_uniqueness",
            description="Default uniqueness check - no duplicate values allowed",
            severity=ValidationSeverity.ERROR,
            parameters={"max_duplicates": 0, "ignore_nulls": True},
        )
        self.add_rule(default_rule)

    def _load_patterns_from_env(self):
        """Load duplicate validation patterns from environment variables."""
        # Load skip patterns (columns that can have duplicates)
        skip_patterns_env = os.getenv("SKIP_DUPLICATE_PATTERNS", "")
        self._skip_patterns = [
            p.strip() for p in skip_patterns_env.split(",") if p.strip()
        ]

        # Load force unique patterns (columns that must be unique)
        unique_patterns_env = os.getenv("FORCE_UNIQUE_PATTERNS", "")
        self._unique_patterns = [
            p.strip() for p in unique_patterns_env.split(",") if p.strip()
        ]

        # Load specific column overrides
        force_unique_cols = os.getenv("FORCE_UNIQUE_COLUMNS", "")
        if force_unique_cols:
            self._force_unique_columns.update(
                p.strip() for p in force_unique_cols.split(",") if p.strip()
            )

        allow_duplicate_cols = os.getenv("ALLOW_DUPLICATE_COLUMNS", "")
        if allow_duplicate_cols:
            self._allow_duplicate_columns.update(
                p.strip() for p in allow_duplicate_cols.split(",") if p.strip()
            )

        # Set defaults if no environment configuration
        if not self._skip_patterns:
            self._skip_patterns = [
                "_id",
                "_uid",
                "fk_",
                "_fk",
                "foreign_key",
                "_key",
                "ref_",
                "_ref",
                "emp_id",
                "empresa_id",
                "cliente_id",
                "user_id",
                "usuario_id",
                "categoria_id",
                "tipo_id",
                "status_id",
                "parent_id",
                "uuid",
                "guid",
                "_uuid",
                "_guid",
                "uid",
                "endereco",
                "rua",
                "avenida",
                "cidade",
                "estado",
                "pais",
                "cep",
                "nome",
                "sobrenome",
                "titulo",
                "descricao",
                "observacao",
                "comentario",
                "telefone",
                "celular",
                "email",
                "cor",
                "tamanho",
                "peso",
                "altura",
                "largura",
                "marca",
                "modelo",
                "versao",
                "status",
                "situacao",
                "tipo",
                "categoria",
                "classe",
                "genero",
                "sexo",
                "nacionalidade",
                "profissao",
                "ativo",
                "inativo",
                "pendente",
                "aprovado",
                "rejeitado",
            ]

        if not self._unique_patterns:
            self._unique_patterns = [
                "cpf",
                "cnpj",
                "rg",
                "passaporte",
                "documento",
                "codigo",
                "numero",
                "serial",
                "sku",
                "barcode",
                "login",
                "username",
                "email_pessoal",
            ]

    def configure_column_uniqueness(
        self,
        force_unique: Optional[List[str]] = None,
        allow_duplicates: Optional[List[str]] = None,
    ):
        """
        Configure specific columns for uniqueness validation.

        Args:
            force_unique: List of column names that must be unique (override smart detection)
            allow_duplicates: List of column names that can have duplicates (override smart detection)
        """
        if force_unique:
            self._force_unique_columns.update(force_unique)
        if allow_duplicates:
            self._allow_duplicate_columns.update(allow_duplicates)

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

        # Skip FK/UUID columns that are expected to have duplicates
        if self._should_skip_column_for_duplicates(column_name):
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

    def _should_skip_column_for_duplicates(self, column_name: str) -> bool:
        """
        Intelligent pattern matching to determine if column should skip duplicate validation.

        Args:
            column_name: The column name to evaluate

        Returns:
            bool: True if column should skip duplicate validation
        """
        column_lower = column_name.lower()

        # Check explicit configurations first
        if column_name in self._force_unique_columns:
            return False  # Must validate for duplicates
        if column_name in self._allow_duplicate_columns:
            return True  # Skip validation

        # Check for patterns that should force uniqueness validation
        for pattern in self._unique_patterns:
            if pattern in column_lower:
                return False  # Must validate for duplicates

        # Check for patterns that typically allow duplicates
        for pattern in self._skip_patterns:
            if pattern in column_lower:
                return True  # Skip validation

        # Default: validate for duplicates
        return False
