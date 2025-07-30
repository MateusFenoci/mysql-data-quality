"""Volumetry calculation following Single Responsibility Principle."""

from typing import Any, Dict

import pandas as pd


class VolumetryCalculator:
    """
    Calculates data volume metrics following Single Responsibility Principle.

    Only responsible for calculating data volume - does not handle analysis or reporting.
    """

    @staticmethod
    def calculate_volume_metrics(data: pd.DataFrame) -> Dict[str, Any]:
        """
        Calculate comprehensive volume metrics for a DataFrame.

        Args:
            data: DataFrame to analyze

        Returns:
            Dictionary containing volume metrics
        """
        # Calculate memory usage
        memory_usage_bytes = data.memory_usage(deep=True).sum()

        # Convert to different units
        memory_gb = memory_usage_bytes / (1024**3)
        memory_tb = memory_gb / 1024

        # Estimate disk size (typically 2-3x memory for CSV, varies by format)
        estimated_disk_gb = memory_gb * 2.5
        estimated_disk_tb = estimated_disk_gb / 1024

        return {
            "memory_usage_bytes": int(memory_usage_bytes),
            "memory_usage_gb": round(float(memory_gb), 6),
            "memory_usage_tb": round(float(memory_tb), 8),
            "estimated_disk_gb": round(float(estimated_disk_gb), 6),
            "estimated_disk_tb": round(float(estimated_disk_tb), 8),
            "row_count": len(data),
            "column_count": len(data.columns),
            "data_points": len(data) * len(data.columns),
        }

    @staticmethod
    def display_volume_info(volume_metrics: Dict[str, Any]):
        """Display volume information in a user-friendly format."""
        from rich.console import Console

        console = Console()

        console.print("📊 [bold]Dataset Overview[/bold]")
        console.print(f"   • Rows: {volume_metrics['row_count']:,}")
        console.print(f"   • Columns: {volume_metrics['column_count']:,}")
        console.print(f"   • Data Points: {volume_metrics['data_points']:,}")
        console.print(f"   • Memory Usage: {volume_metrics['memory_usage_gb']:.6f} GB")

        if volume_metrics["memory_usage_tb"] > 0.001:
            console.print(
                f"   • Memory Usage: {volume_metrics['memory_usage_tb']:.8f} TB"
            )

        console.print(
            f"   • Estimated Disk Size: {volume_metrics['estimated_disk_gb']:.6f} GB"
        )

        if volume_metrics["estimated_disk_tb"] > 0.001:
            console.print(
                f"   • Estimated Disk Size: {volume_metrics['estimated_disk_tb']:.8f} TB"
            )

    @staticmethod
    def get_sampling_info(total_rows: int, analyzed_rows: int) -> Dict[str, Any]:
        """
        Calculate sampling information.

        Args:
            total_rows: Total number of rows in the original dataset
            analyzed_rows: Number of rows actually analyzed

        Returns:
            Dictionary containing sampling information
        """
        return {
            "total_table_rows": total_rows,
            "analyzed_rows": analyzed_rows,
            "is_sampled": total_rows > analyzed_rows,
            "sampling_ratio": analyzed_rows / total_rows if total_rows > 0 else 1.0,
        }
