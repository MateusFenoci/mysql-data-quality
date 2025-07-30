"""Base classes for data quality validators following SOLID principles."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

import pandas as pd


class ValidationSeverity(Enum):
    """Severity levels for validation results."""

    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ValidationResult:
    """Result of a data quality validation."""

    rule_name: str
    table_name: str
    column_name: Optional[str]
    severity: ValidationSeverity
    passed: bool
    message: str
    details: Dict[str, Any]
    timestamp: datetime
    affected_rows: int = 0
    total_rows: int = 0

    @property
    def pass_rate(self) -> float:
        """Calculate pass rate as percentage."""
        if self.total_rows == 0:
            return 100.0
        return ((self.total_rows - self.affected_rows) / self.total_rows) * 100.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""

        def convert_value(value):
            """Convert numpy types to native Python types for JSON serialization."""
            import numpy as np

            if isinstance(value, (np.integer, np.int64, np.int32)):
                return int(value)
            elif isinstance(value, (np.floating, np.float64, np.float32)):
                return float(value)
            elif isinstance(value, np.bool_):
                return bool(value)
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert_value(v) for v in value]
            return value

        return {
            "rule_name": self.rule_name,
            "table_name": self.table_name,
            "column_name": self.column_name,
            "severity": self.severity.value,
            "passed": bool(self.passed),
            "message": self.message,
            "details": convert_value(self.details),
            "timestamp": self.timestamp.isoformat(),
            "affected_rows": int(self.affected_rows),
            "total_rows": int(self.total_rows),
            "pass_rate": float(self.pass_rate),
        }


@dataclass
class ValidationRule:
    """Configuration for a validation rule."""

    name: str
    description: str
    severity: ValidationSeverity
    enabled: bool = True
    parameters: Optional[Dict[str, Any]] = None

    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class DataQualityValidator(ABC):
    """Abstract base class for all data quality validators (Interface Segregation Principle)."""

    def __init__(self, name: str, description: str):
        """Initialize validator with name and description."""
        self.name = name
        self.description = description
        self._rules: List[ValidationRule] = []

    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule (Open/Closed Principle)."""
        self._rules.append(rule)

    def get_rules(self) -> List[ValidationRule]:
        """Get all validation rules."""
        return self._rules.copy()

    @abstractmethod
    def validate_table(
        self,
        data: pd.DataFrame,
        table_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate entire table (Single Responsibility Principle)."""
        pass

    @abstractmethod
    def validate_column(
        self,
        data: pd.Series,
        table_name: str,
        column_name: str,
        rules: Optional[List[ValidationRule]] = None,
    ) -> List[ValidationResult]:
        """Validate specific column (Single Responsibility Principle)."""
        pass

    def _create_result(
        self,
        rule: ValidationRule,
        table_name: str,
        column_name: Optional[str],
        passed: bool,
        message: str,
        details: Dict[str, Any],
        affected_rows: int = 0,
        total_rows: int = 0,
    ) -> ValidationResult:
        """Create validation result (DRY principle)."""
        return ValidationResult(
            rule_name=rule.name,
            table_name=table_name,
            column_name=column_name,
            severity=rule.severity,
            passed=passed,
            message=message,
            details=details,
            timestamp=datetime.now(),
            affected_rows=affected_rows,
            total_rows=total_rows,
        )


class ValidationEngine:
    """Orchestrates multiple validators (Dependency Inversion Principle)."""

    def __init__(self):
        """Initialize validation engine."""
        self._validators: Dict[str, DataQualityValidator] = {}

    def register_validator(self, validator: DataQualityValidator) -> None:
        """Register a validator."""
        self._validators[validator.name] = validator

    def get_validator(self, name: str) -> Optional[DataQualityValidator]:
        """Get validator by name."""
        return self._validators.get(name)

    def get_all_validators(self) -> Dict[str, DataQualityValidator]:
        """Get all registered validators."""
        return self._validators.copy()

    def validate_data(
        self,
        data: pd.DataFrame,
        table_name: str,
        validator_names: Optional[List[str]] = None,
    ) -> List[ValidationResult]:
        """Run validations using specified validators."""
        results = []

        validators_to_run = (
            [
                self._validators[name]
                for name in validator_names
                if name in self._validators
            ]
            if validator_names
            else list(self._validators.values())
        )

        for validator in validators_to_run:
            try:
                validator_results = validator.validate_table(data, table_name)
                results.extend(validator_results)
            except Exception as e:
                # Create error result for failed validator
                error_result = ValidationResult(
                    rule_name=f"{validator.name}_error",
                    table_name=table_name,
                    column_name=None,
                    severity=ValidationSeverity.CRITICAL,
                    passed=False,
                    message=f"Validator {validator.name} failed: {str(e)}",
                    details={"error": str(e)},
                    timestamp=datetime.now(),
                )
                results.append(error_result)

        return results
