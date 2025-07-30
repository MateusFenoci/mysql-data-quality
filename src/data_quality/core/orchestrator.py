"""Core orchestrator following SOLID principles."""

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from ..config import load_config
from ..connectors.base import DatabaseConnector
from ..connectors.factory import DatabaseConnectorFactory
from ..validators import (
    CompletenessValidator,
    DuplicatesValidator,
    IntegrityValidator,
    PatternsValidator,
)
from .data_analyzer import DataAnalyzer
from .report_manager import ReportManager
from .volumetry_calculator import VolumetryCalculator

console = Console()


class DataQualityOrchestrator:
    """
    Orchestrates data quality workflow following SOLID principles.

    Single Responsibility: Coordinates different components
    Open/Closed: Extensible through dependency injection
    Liskov: Uses abstractions for components
    Interface Segregation: Each component has focused interface
    Dependency Inversion: Depends on abstractions, not concretions
    """

    def __init__(
        self,
        output_dir: str = "logs",
        analyzer: Optional[DataAnalyzer] = None,
        report_manager: Optional[ReportManager] = None,
        volumetry_calculator: Optional[VolumetryCalculator] = None,
    ):
        """
        Initialize the orchestrator with dependency injection.

        Args:
            output_dir: Output directory for reports
            analyzer: Data analyzer instance (optional, creates default if None)
            report_manager: Report manager instance (optional, creates default if None)
            volumetry_calculator: Volumetry calculator (optional, creates default if None)
        """
        # Load configuration
        try:
            self.config = load_config()
            self.db_config = self.config["database"]
        except Exception as e:
            console.print(f"❌ [bold red]Configuration error: {e}[/bold red]")
            sys.exit(1)

        # Initialize components (Dependency Inversion Principle)
        self.analyzer = analyzer or DataAnalyzer()
        self.report_manager = report_manager or ReportManager(output_dir)
        self.volumetry_calculator = volumetry_calculator or VolumetryCalculator()

        # Database connector (managed by orchestrator)
        self.connector: Optional[DatabaseConnector] = None

        # Register default validators
        self._register_validators()

    def _register_validators(self):
        """Register all available validators."""
        self.analyzer.register_validator(CompletenessValidator())
        self.analyzer.register_validator(DuplicatesValidator())
        self.analyzer.register_validator(IntegrityValidator())
        self.analyzer.register_validator(PatternsValidator())

    def _connect_database(self) -> bool:
        """Establish database connection."""
        try:
            self.connector = DatabaseConnectorFactory.create_connector(
                self.db_config.connection_string, self.db_config.driver
            )
            self.connector.connect()

            if not self.connector.test_connection():
                console.print("❌ [bold red]Database connection failed![/bold red]")
                return False

            return True

        except Exception as e:
            console.print(f"❌ [bold red]Database connection error: {e}[/bold red]")
            return False

    def _disconnect_database(self):
        """Close database connection."""
        if self.connector:
            self.connector.disconnect()

    def _build_metadata(
        self, table_name: str, data: pd.DataFrame, total_rows: int
    ) -> Dict[str, Any]:
        """Build comprehensive metadata using specialized components."""
        # Use VolumetryCalculator for volume metrics
        volume_metrics = self.volumetry_calculator.calculate_volume_metrics(data)

        # Use VolumetryCalculator for sampling info
        sampling_info = self.volumetry_calculator.get_sampling_info(
            total_rows, len(data)
        )

        return {
            "table_name": table_name,
            "analysis_timestamp": datetime.now().isoformat(),
            "database_info": {
                "host": self.db_config.host,
                "port": self.db_config.port,
                "database": self.db_config.name,
                "driver": self.db_config.driver,
            },
            "data_volume": volume_metrics,
            "sampling_info": sampling_info,
            "table_structure": {
                "columns": list(data.columns),
                "data_types": {col: str(dtype) for col, dtype in data.dtypes.items()},
            },
        }

    def analyze_table(
        self,
        table_name: str,
        sample_size: int = 10000,
        validators: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Perform comprehensive data quality analysis on a table.

        Args:
            table_name: Name of the table to analyze
            sample_size: Maximum number of rows to analyze
            validators: Specific validators to run (default: all)

        Returns:
            Dictionary containing analysis results and metadata
        """
        analysis_start = datetime.now()

        console.print(f"\n🔍 [bold blue]Data Quality Analysis: {table_name}[/bold blue]")

        # Connect to database
        if not self._connect_database():
            return {"error": "Database connection failed"}

        try:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                # Load data
                task = progress.add_task("Loading table data...", total=None)

                if self.connector is None:
                    raise RuntimeError("Database connector not initialized")

                total_rows = self.connector.get_table_count(table_name)

                if total_rows > sample_size:
                    console.print(
                        f"⚠️  [yellow]Table has {total_rows:,} rows. Using sample of {sample_size:,} rows.[/yellow]"
                    )
                    query = f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT {sample_size}"  # nosec B608
                else:
                    console.print(
                        f"ℹ️  [blue]Analyzing all {total_rows:,} rows.[/blue]"
                    )
                    query = f"SELECT * FROM {table_name}"  # nosec B608

                if self.connector is None:
                    raise RuntimeError("Database connector not initialized")

                data = self.connector.execute_query(query)
                progress.update(task, description="Data loaded successfully")

                # Get metadata using specialized components
                task = progress.add_task("Analyzing table structure...", total=None)
                metadata = self._build_metadata(table_name, data, total_rows)
                progress.update(task, description="Metadata collected")

                # Display volume information using VolumetryCalculator
                self.volumetry_calculator.display_volume_info(metadata["data_volume"])

                # Run validations using DataAnalyzer
                results = self.analyzer.analyze_dataframe(data, table_name, validators)

                analysis_duration = datetime.now() - analysis_start

                # Get analysis summary using DataAnalyzer
                summary = self.analyzer.get_analysis_summary(results)
                summary["duration_seconds"] = analysis_duration.total_seconds()

                return {
                    "table_name": table_name,
                    "metadata": metadata,
                    "validation_results": results,
                    "analysis_summary": summary,
                }

        except Exception as e:
            console.print(f"❌ [bold red]Analysis error: {e}[/bold red]")
            return {"error": str(e)}

        finally:
            self._disconnect_database()

    def generate_reports(
        self,
        analysis_results: Dict[str, Any],
        formats: Optional[List[str]] = None,
        unified: bool = True,
        report_name: Optional[str] = None,
    ) -> Dict[str, Path]:
        """
        Generate reports with flexible options.

        Args:
            analysis_results: Results from analyze_table method
            formats: List of formats to generate ('html', 'json', 'txt'). None = all formats
            unified: If True, generates with unified naming. If False, generates separate reports
            report_name: Custom name for the reports

        Returns:
            Dictionary mapping format to file path
        """
        if "error" in analysis_results:
            console.print(
                f"❌ [bold red]Cannot generate report: {analysis_results['error']}[/bold red]"
            )
            return {}

        table_name = analysis_results["table_name"]
        results = analysis_results["validation_results"]
        metadata = analysis_results["metadata"]

        # Default to all formats if none specified
        if formats is None:
            formats = ["html", "json", "txt"]

        # Generate reports using ReportManager
        if unified:
            generated_reports = self.report_manager.generate_unified_report(
                results, table_name, metadata, report_name, formats
            )
        else:
            generated_reports = self.report_manager.generate_multiple_reports(
                results, table_name, formats, metadata, report_name
            )

        # Display summary
        self.report_manager.display_report_summary(generated_reports)

        return generated_reports

    def run_complete_analysis(
        self,
        table_name: str,
        sample_size: int = 10000,
        validators: Optional[List[str]] = None,
        report_formats: Optional[List[str]] = None,
        unified_reports: bool = True,
        report_name: Optional[str] = None,
    ) -> Dict[str, Path]:
        """
        Run complete analysis and generate reports with flexible options.

        Args:
            table_name: Name of the table to analyze
            sample_size: Maximum number of rows to analyze
            validators: Specific validators to run (default: all)
            report_formats: List of report formats ('html', 'json', 'txt'). None = all
            unified_reports: If True, generates unified reports. If False, separate reports
            report_name: Custom name for the report files

        Returns:
            Dictionary mapping report format to file path
        """
        console.print(
            "🚀 [bold blue]Starting Complete Data Quality Analysis[/bold blue]"
        )

        # Perform analysis
        analysis_results = self.analyze_table(table_name, sample_size, validators)

        if "error" in analysis_results:
            return {}

        # Generate reports with flexible options
        reports = self.generate_reports(
            analysis_results,
            formats=report_formats,
            unified=unified_reports,
            report_name=report_name,
        )

        console.print("\n🎉 [bold green]Complete analysis finished![/bold green]")

        return reports
