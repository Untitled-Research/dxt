"""XLT CLI - Command-line interface for the data move tool."""

import typer
from typing_extensions import Annotated
from pathlib import Path

from xlt import __version__

app = typer.Typer(
    name="xlt",
    help="XLT - The YAML-first Data Move Tool",
    add_completion=True,
)


def version_callback(value: bool) -> None:
    """Show version and exit."""
    if value:
        typer.echo(f"xlt version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-v",
            callback=version_callback,
            is_eager=True,
            help="Show version and exit",
        ),
    ] = False,
) -> None:
    """XLT - Extract and Load data with declarative YAML pipelines."""
    pass


@app.command()
def run(
    pipeline: Annotated[
        Path,
        typer.Argument(
            help="Path to the YAML pipeline file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Validate pipeline without executing"),
    ] = False,
) -> None:
    """Run a data pipeline from a YAML file."""
    typer.echo(f"Running pipeline: {pipeline}")
    if dry_run:
        typer.echo("Dry run mode - validation only")
    # TODO: Implement pipeline execution
    typer.echo("Pipeline execution not yet implemented")


@app.command()
def validate(
    pipeline: Annotated[
        Path,
        typer.Argument(
            help="Path to the YAML pipeline file",
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
) -> None:
    """Validate a pipeline YAML file."""
    typer.echo(f"Validating pipeline: {pipeline}")
    # TODO: Implement pipeline validation
    typer.echo("Validation not yet implemented")


@app.command()
def init(
    name: Annotated[
        str,
        typer.Argument(help="Name of the pipeline to create"),
    ] = "pipeline",
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output directory for the pipeline file"),
    ] = Path("."),
) -> None:
    """Create a new pipeline template."""
    pipeline_file = output / f"{name}.yaml"
    typer.echo(f"Creating pipeline template: {pipeline_file}")
    # TODO: Implement pipeline template creation
    typer.echo("Template creation not yet implemented")


if __name__ == "__main__":
    app()
