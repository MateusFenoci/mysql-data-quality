"""Base classes for data quality reports."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, List, Any

from ..validators.base import ValidationResult


class ReportGenerator(ABC):
    """Abstract base class for report generators."""

    def __init__(self, output_dir: Optional[Path] = None):
        """Initialize report generator."""
        self.output_dir = Path(output_dir) if output_dir is not None else Path(".")
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @abstractmethod
    def generate_report(
        self,
        results: List[ValidationResult],
        table_name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Generate report from validation results."""
        pass

    def _analyze_results(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Analyze validation results for summary statistics."""
        if not results:
            return {
                "total_checks": 0,
                "passed_checks": 0,
                "failed_checks": 0,
                "success_rate": 100.0,
                "severity_breakdown": {},
                "validator_breakdown": {},
            }

        total_checks = len(results)
        passed_checks = sum(1 for r in results if r.passed)
        failed_checks = total_checks - passed_checks
        success_rate = (
            (passed_checks / total_checks) * 100 if total_checks > 0 else 100.0
        )

        # Group by severity
        severity_counts = {}
        for result in results:
            severity = result.severity.value
            if severity not in severity_counts:
                severity_counts[severity] = {"total": 0, "passed": 0, "failed": 0}

            severity_counts[severity]["total"] += 1
            if result.passed:
                severity_counts[severity]["passed"] += 1
            else:
                severity_counts[severity]["failed"] += 1

        # Group by validator type (extracted from rule name)
        validator_counts = {}
        for result in results:
            # Extract validator type from rule name
            validator_type = "unknown"
            if "completeness" in result.rule_name.lower():
                validator_type = "completeness"
            elif (
                "uniqueness" in result.rule_name.lower()
                or "duplicate" in result.rule_name.lower()
            ):
                validator_type = "duplicates"
            elif (
                "integrity" in result.rule_name.lower()
                or "referential" in result.rule_name.lower()
                or "fk_" in result.rule_name.lower()
                or result.rule_name.lower().startswith("auto_fk")
            ):
                validator_type = "integrity"
            elif "pattern" in result.rule_name.lower() or any(
                pattern in result.rule_name.lower()
                for pattern in ["cnpj", "cpf", "email"]
            ):
                validator_type = "patterns"

            if validator_type not in validator_counts:
                validator_counts[validator_type] = {
                    "total": 0,
                    "passed": 0,
                    "failed": 0,
                }

            validator_counts[validator_type]["total"] += 1
            if result.passed:
                validator_counts[validator_type]["passed"] += 1
            else:
                validator_counts[validator_type]["failed"] += 1

        return {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "failed_checks": failed_checks,
            "success_rate": success_rate,
            "severity_breakdown": severity_counts,
            "validator_breakdown": validator_counts,
        }
