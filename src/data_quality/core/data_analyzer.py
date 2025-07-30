"""Data analysis component following Single Responsibility Principle."""

from typing import Any, Dict, List, Optional

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..validators import ValidationEngine
from ..validators.base import ValidationResult

console = Console()


class DataAnalyzer:
    """
    Handles data analysis and validation following Single Responsibility Principle.

    Only responsible for data validation - does not handle database connections or reporting.
    """

    def __init__(self):
        """Initialize data analyzer."""
        self.engine = ValidationEngine()

    def register_validator(self, validator):
        """Register a validator with the engine."""
        self.engine.register_validator(validator)

    def analyze_dataframe(
        self,
        data: pd.DataFrame,
        table_name: str,
        validators: Optional[List[str]] = None,
    ) -> List[ValidationResult]:
        """
        Analyze a pandas DataFrame for data quality issues.

        Args:
            data: DataFrame to analyze
            table_name: Name of the table (for reporting)
            validators: Specific validators to run

        Returns:
            List of validation results
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task("Running data quality validations...", total=None)
            results: List[ValidationResult] = self.engine.validate_data(
                data, table_name, validators
            )
            progress.update(task, description="Validations completed")

        return results

    def get_analysis_summary(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """
        Generate summary statistics from validation results.

        Args:
            results: List of validation results

        Returns:
            Dictionary containing summary statistics
        """
        if not results:
            return {
                "total_validations": 0,
                "passed_validations": 0,
                "failed_validations": 0,
                "success_rate": 100.0,
                "critical_issues": 0,
                "error_issues": 0,
                "warning_issues": 0,
                "info_issues": 0,
            }

        total = len(results)
        passed = len([r for r in results if r.passed])
        failed = total - passed

        # Count by severity
        severity_counts = {"CRITICAL": 0, "ERROR": 0, "WARNING": 0, "INFO": 0}
        for result in results:
            if not result.passed:
                severity_counts[result.severity.value] += 1

        return {
            "total_validations": total,
            "passed_validations": passed,
            "failed_validations": failed,
            "success_rate": (passed / total) * 100 if total > 0 else 100.0,
            "critical_issues": severity_counts["CRITICAL"],
            "error_issues": severity_counts["ERROR"],
            "warning_issues": severity_counts["WARNING"],
            "info_issues": severity_counts["INFO"],
        }
