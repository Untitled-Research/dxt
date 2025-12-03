"""DXT CLI - Command-line interface for the data move tool."""

import sys

import typer
from pathlib import Path
from typing_extensions import Annotated

from dxt import __version__
from dxt.core.pipeline_executor import PipelineExecutor
from dxt.exceptions import PipelineExecutionError, ValidationError
from dxt.utils.yaml_parser import load_pipeline

app = typer.Typer(
    name="dxt",
    help="DXT - The YAML-first Data Move Tool",
    add_completion=True,
)


def version_callback(value: bool) -> None:
    """Show version and exit."""
    if value:
        typer.echo(f"dxt version {__version__}")
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
    """DXT - Extract and Load data with declarative YAML pipelines."""
    pass


@app.command()
def run(
    pipeline_path: Annotated[
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
    select: Annotated[
        str,
        typer.Option("--select", help="Stream selector (e.g., 'orders', 'tag:critical', '*')"),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Verbose output"),
    ] = False,
) -> None:
    """Run a data pipeline from a YAML file."""
    try:
        # Load and validate pipeline
        typer.echo(f"Loading pipeline: {pipeline_path}")
        pipeline = load_pipeline(pipeline_path)

        typer.echo(f"Pipeline: {pipeline.name}")
        if pipeline.description:
            typer.echo(f"Description: {pipeline.description}")
        typer.echo(f"Streams: {pipeline.stream_count}")

        if dry_run:
            typer.echo("\nDry run mode - validation only")
            typer.echo("Pipeline is valid!")
            return

        # Execute pipeline
        typer.echo("\nExecuting pipeline...")
        executor = PipelineExecutor()
        result = executor.execute(pipeline, dry_run=False, select=select)

        # Display results
        typer.echo("\n" + "="*60)
        if result.success:
            typer.secho("Pipeline execution succeeded!", fg=typer.colors.GREEN, bold=True)
        else:
            typer.secho("Pipeline execution failed!", fg=typer.colors.RED, bold=True)
            if result.error_message:
                typer.echo(f"Error: {result.error_message}")

        typer.echo(f"\nStreams processed: {result.streams_processed}")
        typer.echo(f"Streams succeeded: {result.streams_succeeded}")
        typer.echo(f"Streams failed: {result.streams_failed}")
        typer.echo(f"Total records transferred: {result.total_records_transferred:,}")
        typer.echo(f"Duration: {result.duration_seconds:.2f}s")

        if verbose and result.stream_results:
            typer.echo("\nStream details:")
            for stream_result in result.stream_results:
                status = "✓" if stream_result.success else "✗"
                typer.echo(f"  {status} {stream_result.stream_id}: {stream_result.records_transferred:,} records")
                if not stream_result.success and stream_result.error_message:
                    typer.echo(f"    Error: {stream_result.error_message}")

        # Exit with error code if failed
        if not result.success:
            raise typer.Exit(code=1)

    except ValidationError as e:
        typer.secho(f"Validation error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    except PipelineExecutionError as e:
        typer.secho(f"Execution error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"Unexpected error: {e}", fg=typer.colors.RED, err=True)
        if verbose:
            import traceback
            traceback.print_exc()
        raise typer.Exit(code=1)


@app.command()
def validate(
    pipeline_path: Annotated[
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
    try:
        typer.echo(f"Validating pipeline: {pipeline_path}")
        pipeline = load_pipeline(pipeline_path)

        typer.secho("✓ Pipeline is valid!", fg=typer.colors.GREEN, bold=True)
        typer.echo(f"\nPipeline: {pipeline.name}")
        if pipeline.description:
            typer.echo(f"Description: {pipeline.description}")
        typer.echo(f"Version: {pipeline.version}")
        typer.echo(f"Streams: {pipeline.stream_count}")
        typer.echo(f"Buffer format: {pipeline.buffer.format}")

        typer.echo("\nStreams:")
        for stream in pipeline.streams:
            typer.echo(f"  - {stream.id}")
            typer.echo(f"      Source: {stream.source.value} (type: {stream.source.type})")
            typer.echo(f"      Target: {stream.target.value} (type: {stream.target.type})")
            typer.echo(f"      Extract: {stream.extract.mode}")
            typer.echo(f"      Load: {stream.load.mode}")
            if stream.tags:
                typer.echo(f"      Tags: {', '.join(stream.tags)}")

    except ValidationError as e:
        typer.secho(f"✗ Validation failed: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    except Exception as e:
        typer.secho(f"✗ Unexpected error: {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


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
