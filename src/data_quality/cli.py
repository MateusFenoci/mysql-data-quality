"""Command line interface for data quality tool."""

import click
from rich.console import Console
from rich.table import Table

from .config import load_config
from .connectors.factory import DatabaseConnectorFactory

console = Console()


@click.group()
@click.version_option(version="0.1.0")
def main():
    """Data Quality validation and reporting tool."""
    console.print("🔍 [bold blue]Data Quality Tool[/bold blue]", style="bold")


@main.command()
def test_connection():
    """Test database connection."""
    try:
        config = load_config()
        db_config = config["database"]

        console.print(f"🔌 Testing connection to {db_config.driver}...")
        console.print(f"   Host: {db_config.host}:{db_config.port}")
        console.print(f"   Database: {db_config.name}")

        connector = DatabaseConnectorFactory.create_connector(
            db_config.connection_string, db_config.driver
        )

        with console.status("[bold green]Connecting..."):
            connector.connect()

        if connector.test_connection():
            console.print("✅ [bold green]Connection successful![/bold green]")
            connector.disconnect()
        else:
            console.print("❌ [bold red]Connection failed![/bold red]")

    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")


@main.command()
@click.option("--real-count", is_flag=True, help="Get real row counts (slower)")
def list_tables(real_count):
    """List all tables in the database."""
    try:
        config = load_config()
        db_config = config["database"]

        connector = DatabaseConnectorFactory.create_connector(
            db_config.connection_string, db_config.driver
        )

        with console.status("[bold green]Connecting and fetching tables..."):
            connector.connect()

            # Get table names first
            tables_query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
            ORDER BY table_name
            """

            tables_df = connector.execute_query(tables_query)

        if len(tables_df) > 0:
            table = Table(title="Database Tables")
            table.add_column("Table Name", style="cyan")

            if real_count:
                table.add_column("Real Row Count", style="magenta", justify="right")
                console.print(
                    "⚠️  [yellow]Getting real counts - this may take a while...[/yellow]"
                )

                with console.status("[bold green]Counting rows in each table..."):
                    table_data = []
                    for _, row in tables_df.iterrows():
                        table_name = row["table_name"]
                        try:
                            count = connector.get_table_count(table_name)
                            table_data.append((table_name, f"{count:,}"))
                        except Exception as e:
                            table_data.append((table_name, f"Error: {e}"))

                    # Sort by count (highest first)
                    table_data.sort(
                        key=lambda x: int(x[1].replace(",", ""))
                        if x[1].replace(",", "").isdigit()
                        else 0,
                        reverse=True,
                    )

                    for table_name, count in table_data:
                        table.add_row(table_name, count)
            else:
                table.add_column("Estimated Rows", style="magenta", justify="right")

                # Get estimates from information_schema
                estimates_query = """
                SELECT table_name, table_rows
                FROM information_schema.tables
                WHERE table_schema = DATABASE()
                ORDER BY table_name
                """
                estimates_df = connector.execute_query(estimates_query)

                for _, row in estimates_df.iterrows():
                    table_name = row["table_name"]
                    table_rows = str(row.get("table_rows", "N/A"))
                    table.add_row(table_name, table_rows)

            console.print(table)

            if not real_count:
                console.print(
                    "\n💡 [dim]Use --real-count flag for accurate row counts (slower)[/dim]"
                )
        else:
            console.print("No tables found.")

        connector.disconnect()

    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")


@main.command()
@click.argument("table_name")
def describe_table(table_name):
    """Describe table structure."""
    try:
        config = load_config()
        db_config = config["database"]

        connector = DatabaseConnectorFactory.create_connector(
            db_config.connection_string, db_config.driver
        )

        with console.status(f"[bold green]Fetching structure for {table_name}..."):
            connector.connect()
            columns_info = connector.get_table_info(table_name)
            count = connector.get_table_count(table_name)

        console.print(f"\n🏗️  [bold]Table: {table_name}[/bold]")
        console.print(f"📊 [bold]Records: {count:,}[/bold]\n")

        if columns_info:
            table = Table(title="Column Information")
            table.add_column("Column", style="cyan")
            table.add_column("Type", style="green")
            table.add_column("Nullable", style="yellow")
            table.add_column("Default", style="magenta")

            for col in columns_info:
                col_name = col.get("column_name", "N/A")
                col_type = col.get("data_type", "N/A")
                nullable = "Yes" if col.get("is_nullable") == "YES" else "No"
                default = str(col.get("column_default", "")) or "-"

                table.add_row(col_name, col_type, nullable, default)

            console.print(table)
        else:
            console.print("No column information found.")

        connector.disconnect()

    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")


@main.command()
@click.argument("table_name")
@click.option(
    "--validators",
    "-v",
    multiple=True,
    help="Specific validators to run (completeness, duplicates, integrity, patterns)",
)
@click.option(
    "--sample-size", "-s", type=int, default=10000, help="Sample size for large tables"
)
@click.option(
    "--report-format",
    "-r",
    multiple=True,
    type=click.Choice(["html", "json", "summary"], case_sensitive=False),
    help="Generate reports in specified formats",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default="reports",
    help="Output directory for reports",
)
def validate(table_name, validators, sample_size, report_format, output_dir):
    """Run data quality validations on a table."""
    try:
        from .validators import (
            ValidationEngine,
            CompletenessValidator,
            DuplicatesValidator,
            IntegrityValidator,
            PatternsValidator,
        )
        from .reports import (
            HTMLReportGenerator,
            JSONReportGenerator,
            SummaryReportGenerator,
        )
        from pathlib import Path

        config = load_config()
        db_config = config["database"]

        connector = DatabaseConnectorFactory.create_connector(
            db_config.connection_string, db_config.driver
        )

        with console.status(
            f"[bold green]Connecting and loading data from {table_name}..."
        ):
            connector.connect()

            # Get table row count to determine if sampling is needed
            total_rows = connector.get_table_count(table_name)

            if total_rows > sample_size:
                console.print(
                    f"⚠️  [yellow]Table has {total_rows:,} rows. Using sample of {sample_size:,} rows.[/yellow]"
                )
                # Table name is validated by connection, safe to use  # nosec B608
                query = f"SELECT * FROM {table_name} ORDER BY RAND() LIMIT {sample_size}"  # nosec B608
            else:
                console.print(f"ℹ️  [blue]Analyzing all {total_rows:,} rows.[/blue]")
                # Table name is validated by connection, safe to use  # nosec B608
                query = f"SELECT * FROM {table_name}"  # nosec B608

            data = connector.execute_query(query)
            connector.disconnect()

        console.print(
            f"\n🔍 [bold]Running data quality validations on '{table_name}'[/bold]"
        )
        console.print(f"📊 Dataset: {len(data)} rows × {len(data.columns)} columns\n")

        # Initialize validation engine
        engine = ValidationEngine()

        # Register validators
        if not validators or "completeness" in validators:
            engine.register_validator(CompletenessValidator())

        if not validators or "duplicates" in validators:
            engine.register_validator(DuplicatesValidator())

        if not validators or "integrity" in validators:
            engine.register_validator(IntegrityValidator())

        if not validators or "patterns" in validators:
            engine.register_validator(PatternsValidator())

        # Run validations
        with console.status("[bold green]Running validations..."):
            results = engine.validate_data(
                data, table_name, list(validators) if validators else None
            )

        # Display results
        if results:
            _display_validation_results(results)

            # Generate reports if requested
            if report_format:
                console.print("\n📄 [bold]Generating reports...[/bold]")
                output_path = Path(output_dir)
                generated_reports = []

                # Prepare metadata
                metadata = {
                    "sample_size": sample_size if total_rows > sample_size else None,
                    "total_table_rows": total_rows,
                    "analyzed_rows": len(data),
                    "validators_used": list(validators)
                    if validators
                    else ["completeness", "duplicates", "integrity", "patterns"],
                }

                with console.status("[bold green]Generating reports..."):
                    if "html" in report_format:
                        html_generator = HTMLReportGenerator(output_path)
                        html_file = html_generator.generate_report(
                            results, table_name, metadata
                        )
                        generated_reports.append(("HTML", html_file))

                    if "json" in report_format:
                        json_generator = JSONReportGenerator(output_path)
                        json_file = json_generator.generate_report(
                            results, table_name, metadata
                        )
                        generated_reports.append(("JSON", json_file))

                    if "summary" in report_format:
                        summary_generator = SummaryReportGenerator(output_path)
                        summary_file = summary_generator.generate_report(
                            results, table_name, metadata
                        )
                        generated_reports.append(("Summary", summary_file))

                console.print("✅ [green]Reports generated successfully![/green]")
                for report_type, file_path in generated_reports:
                    console.print(f"  📁 {report_type}: [cyan]{file_path}[/cyan]")
        else:
            console.print("✅ [green]No validation rules to run.[/green]")

    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")


@main.command()
@click.argument("table_name")
@click.option(
    "--sample-size", "-s", type=int, default=10000, help="Sample size for large tables"
)
@click.option(
    "--validators",
    "-v",
    multiple=True,
    help="Specific validators to run (completeness, duplicates, integrity, patterns)",
)
@click.option(
    "--output-dir",
    "-o",
    type=click.Path(),
    default="reports",
    help="Output directory for reports",
)
@click.option("--report-name", "-n", type=str, help="Custom name for the report files")
@click.option(
    "--formats",
    "-f",
    multiple=True,
    type=click.Choice(["html", "json", "txt"], case_sensitive=False),
    help="Report formats to generate (default: all formats)",
)
@click.option(
    "--separate-reports",
    is_flag=True,
    help="Generate separate reports instead of unified naming",
)
def analyze(
    table_name,
    sample_size,
    validators,
    output_dir,
    report_name,
    formats,
    separate_reports,
):
    """Run complete data quality analysis with unified reporting (TXT, JSON, HTML)."""
    try:
        from .core import DataQualityOrchestrator

        # Initialize orchestrator
        orchestrator = DataQualityOrchestrator(output_dir)

        # Run complete analysis with flexible options
        reports = orchestrator.run_complete_analysis(
            table_name=table_name,
            sample_size=sample_size,
            validators=list(validators) if validators else None,
            report_formats=list(formats) if formats else None,
            unified_reports=not separate_reports,
            report_name=report_name,
        )

        if reports:
            console.print(
                "\n📊 [bold green]Analysis completed successfully![/bold green]"
            )
            console.print(
                f"🎯 [bold]Generated {len(reports)} report(s) for table '{table_name}'[/bold]"
            )
        else:
            console.print(
                "❌ [bold red]Analysis failed - no reports generated[/bold red]"
            )

    except Exception as e:
        console.print(f"❌ [bold red]Error: {e}[/bold red]")


def _display_validation_results(results):
    """Display validation results in a formatted table."""
    from rich.panel import Panel

    # Group results by severity
    critical_results = [r for r in results if r.severity.value == "CRITICAL"]
    error_results = [r for r in results if r.severity.value == "ERROR"]
    warning_results = [r for r in results if r.severity.value == "WARNING"]
    info_results = [r for r in results if r.severity.value == "INFO"]

    # Summary
    total_checks = len(results)
    passed_checks = len([r for r in results if r.passed])
    failed_checks = total_checks - passed_checks

    summary_text = f"""
📊 [bold]Validation Summary[/bold]
• Total Checks: {total_checks}
• Passed: [green]{passed_checks}[/green]
• Failed: [red]{failed_checks}[/red]
• Success Rate: [{'green' if failed_checks == 0 else 'yellow' if failed_checks < 3 else 'red'}]{(passed_checks/total_checks*100):.1f}%[/]
"""

    console.print(Panel(summary_text, title="🎯 Results Summary", border_style="blue"))

    # Show failed validations by severity (properly separated)
    if critical_results:
        console.print("\n🚨 [bold red]Critical Issues[/bold red]")
        for result in critical_results:
            _display_single_result(result)

    if error_results:
        console.print("\n❌ [bold red]Errors[/bold red]")
        for result in error_results:
            _display_single_result(result)

    if warning_results:
        console.print("\n⚠️  [bold yellow]Warnings[/bold yellow]")
        for result in warning_results:
            _display_single_result(result)

    if info_results:
        console.print("\n💡 [bold blue]Information[/bold blue]")
        for result in info_results:
            _display_single_result(result)


def _display_single_result(result):
    """Display a single validation result."""
    status_icon = "✅" if result.passed else "❌"
    severity_color = {
        "CRITICAL": "red",
        "ERROR": "red",
        "WARNING": "yellow",
        "INFO": "blue",
    }.get(result.severity.value, "white")

    column_info = f" [{result.column_name}]" if result.column_name else ""

    console.print(
        f"  {status_icon} [{severity_color}]{result.rule_name}[/]{column_info}: {result.message}"
    )

    if result.affected_rows > 0:
        console.print(
            f"    📈 Affected: {result.affected_rows:,} / {result.total_rows:,} rows ({result.pass_rate:.1f}% pass rate)"
        )

    # Show key details
    if result.details:
        key_details = []
        if "completeness_ratio" in result.details:
            key_details.append(
                f"Completeness: {result.details['completeness_ratio']:.1%}"
            )
        if "duplicate_count" in result.details:
            key_details.append(f"Duplicates: {result.details['duplicate_count']}")
        if "duplicate_values" in result.details and result.details["duplicate_values"]:
            sample_vals = result.details["duplicate_values"][:3]
            key_details.append(f"Sample values: {sample_vals}")

        if key_details:
            console.print(f"    💡 {' | '.join(key_details)}")

    console.print()


if __name__ == "__main__":
    main()
