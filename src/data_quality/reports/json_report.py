"""JSON report generator for data quality results."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Any, Dict, List

from ..validators.base import ValidationResult
from .base import ReportGenerator


class JSONReportGenerator(ReportGenerator):
    """Generates JSON reports from validation results."""

    def generate_report(
        self,
        results: List[ValidationResult],
        table_name: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """Generate JSON report from validation results."""
        summary = self._analyze_results(results)

        # Convert results to dictionaries for JSON serialization
        results_data = [result.to_dict() for result in results]

        # Create report structure
        report_data = {
            "report": {
                "generated_at": datetime.now().isoformat(),
                "table_name": table_name,
                "metadata": metadata or {},
                "summary": summary,
                "results": results_data,
            }
        }

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"data_quality_report_{table_name}_{timestamp}.json"
        output_path = self.output_dir / filename

        # Write JSON report
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False, default=str)

        return output_path
