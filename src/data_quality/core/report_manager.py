"""Report management following Single Responsibility Principle."""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Type

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..reports import HTMLReportGenerator, JSONReportGenerator, SummaryReportGenerator
from ..reports.base import ReportGenerator
from ..validators.base import ValidationResult

console = Console()


class ReportManager:
    """
    Manages report generation following Single Responsibility Principle.

    Only responsible for generating reports - does not handle validation or analysis.
    """

    def __init__(self, output_dir: str = "reports"):
        """Initialize report manager."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_single_report(
        self,
        results: List[ValidationResult],
        table_name: str,
        format_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        custom_name: Optional[str] = None,
    ) -> Path:
        """
        Generate a single report in specified format.

        Args:
            results: Validation results
            table_name: Name of the table
            format_type: 'html', 'json', or 'txt'
            metadata: Additional metadata
            custom_name: Custom filename (without extension)

        Returns:
            Path to generated report
        """
        generators: Dict[str, Type[ReportGenerator]] = {
            "html": HTMLReportGenerator,
            "json": JSONReportGenerator,
            "txt": SummaryReportGenerator,
        }

        if format_type not in generators:
            raise ValueError(
                f"Unsupported format: {format_type}. Use: {list(generators.keys())}"
            )

        generator = generators[format_type](self.output_dir)
        report_path = generator.generate_report(results, table_name, metadata)

        # Rename if custom name provided
        if custom_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            extension = f".{format_type}" if format_type != "txt" else ".txt"
            new_name = f"{custom_name}_{timestamp}{extension}"
            new_path = self.output_dir / new_name
            report_path.rename(new_path)
            return new_path

        return report_path

    def generate_multiple_reports(
        self,
        results: List[ValidationResult],
        table_name: str,
        formats: List[str],
        metadata: Optional[Dict[str, Any]] = None,
        base_name: Optional[str] = None,
    ) -> Dict[str, Path]:
        """
        Generate multiple reports in different formats.

        Args:
            results: Validation results
            table_name: Name of the table
            formats: List of formats to generate ('html', 'json', 'txt')
            metadata: Additional metadata
            base_name: Base name for all reports

        Returns:
            Dictionary mapping format to file path
        """
        if not formats:
            formats = ["html", "json", "txt"]  # Default to all formats

        generated_reports = {}

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Generating {len(formats)} report(s)...", total=len(formats)
            )

            for format_type in formats:
                try:
                    report_path = self.generate_single_report(
                        results, table_name, format_type, metadata, base_name
                    )
                    generated_reports[format_type] = report_path
                    progress.advance(task)

                except Exception as e:
                    console.print(
                        f"⚠️  [yellow]{format_type.upper()} report failed: {e}[/yellow]"
                    )

        return generated_reports

    def generate_unified_report(
        self,
        results: List[ValidationResult],
        table_name: str,
        metadata: Optional[Dict[str, Any]] = None,
        report_name: Optional[str] = None,
        formats: Optional[List[str]] = None,
    ) -> Dict[str, Path]:
        """
        Generate unified report with consistent naming across specified formats.

        Args:
            results: Validation results
            table_name: Name of the table
            metadata: Additional metadata
            report_name: Custom report name
            formats: List of formats to generate ('html', 'json', 'txt'). None = all

        Returns:
            Dictionary mapping format to file path
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = report_name or f"data_quality_unified_{table_name}_{timestamp}"

        generated_reports = {}
        if formats is None:
            formats = ["html", "json", "txt"]

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            task = progress.add_task(
                "Generating unified reports...", total=len(formats)
            )

            for format_type in formats:
                try:
                    # Generate with temporary name first
                    generator_map: Dict[str, Type[ReportGenerator]] = {
                        "html": HTMLReportGenerator,
                        "json": JSONReportGenerator,
                        "txt": SummaryReportGenerator,
                    }

                    generator = generator_map[format_type](self.output_dir)
                    temp_path = generator.generate_report(results, table_name, metadata)

                    # Rename to unified naming
                    extension = f".{format_type}" if format_type != "txt" else ".txt"
                    unified_path = self.output_dir / f"{base_name}{extension}"
                    temp_path.rename(unified_path)

                    generated_reports[format_type] = unified_path
                    progress.advance(task)

                except Exception as e:
                    console.print(
                        f"⚠️  [yellow]{format_type.upper()} report failed: {e}[/yellow]"
                    )

        return generated_reports

    def display_report_summary(self, generated_reports: Dict[str, Path]):
        """Display summary of generated reports."""
        if generated_reports:
            console.print("✅ [bold green]Reports generated successfully![/bold green]")
            for format_type, file_path in generated_reports.items():
                console.print(f"  📁 {format_type.upper()}: [cyan]{file_path}[/cyan]")
        else:
            console.print(
                "❌ [bold red]No reports were generated successfully[/bold red]"
            )
