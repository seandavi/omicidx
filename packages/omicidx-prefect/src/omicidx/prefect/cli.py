"""Operator CLI for the omicidx pipeline.

Scheduled runs go through systemd --user timers (`systemd/README.md`); this
CLI is the ad-hoc and recovery path for the same code. Every `run` subcommand
calls exactly what the corresponding timer calls, so a stage that failed
overnight is re-run by hand with `omicidx-prefect run <stage>`.
"""

import click
from omicidx.prefect.semaphore import SemaphoreStore


@click.group()
def cli() -> None:
    """omicidx-prefect operator CLI."""


@cli.group()
def semaphores() -> None:
    """Inspect and clear partition-completion semaphores."""


@semaphores.command("list")
@click.argument("namespace")
def list_semaphores(namespace: str) -> None:
    """List completed partition keys for NAMESPACE (e.g. sra/study, geo)."""
    store = SemaphoreStore(namespace)
    keys = store.list_keys()
    click.echo(f"{namespace}: {len(keys)} semaphores")
    for k in keys:
        click.echo(f"  {k}")


@semaphores.command("show")
@click.argument("namespace")
@click.argument("key")
def show_semaphore(namespace: str, key: str) -> None:
    """Print the semaphore JSON for NAMESPACE/KEY."""
    import json

    payload = SemaphoreStore(namespace).read(key)
    if payload is None:
        raise click.ClickException(f"No semaphore at {namespace}/{key}")
    click.echo(json.dumps(payload, indent=2))


@semaphores.command("clear")
@click.argument("namespace")
@click.argument("key", required=False)
@click.option("--all", "clear_all", is_flag=True, help="Clear the whole namespace")
def clear_semaphore(namespace: str, key: str | None, clear_all: bool) -> None:
    """Clear one semaphore or the whole namespace (with --all)."""
    store = SemaphoreStore(namespace)
    if clear_all:
        n = store.clear_all()
        click.echo(f"Cleared {n} semaphores in {namespace}")
        return
    if not key:
        raise click.UsageError("Pass KEY or --all")
    removed = store.clear(key)
    click.echo(f"{'Cleared' if removed else 'No semaphore at'} {namespace}/{key}")


@cli.group()
def run() -> None:
    """Run flows locally (no scheduler)."""


@run.command("sra")
@click.option("--force", is_flag=True)
def run_sra(force: bool) -> None:
    from omicidx.prefect.flows.sra import sra_extract

    sra_extract(force=force)


@run.command("geo")
@click.option("--start-month", default="2005-01")
@click.option("--end-month", default=None)
@click.option("--force", is_flag=True)
def run_geo(start_month: str, end_month: str | None, force: bool) -> None:
    from omicidx.prefect.flows.geo import geo_extract

    geo_extract(start_month=start_month, end_month=end_month, force=force)


@run.command("biosample")
@click.option("--force", is_flag=True)
def run_biosample(force: bool) -> None:
    from omicidx.prefect.flows.biosample import biosample_extract

    biosample_extract(force=force)


@run.command("bioproject")
@click.option("--force", is_flag=True)
def run_bioproject(force: bool) -> None:
    from omicidx.prefect.flows.biosample import bioproject_extract

    bioproject_extract(force=force)


@run.command("pubmed")
@click.option("--force", is_flag=True)
def run_pubmed(force: bool) -> None:
    from omicidx.prefect.flows.pubmed import pubmed_extract

    pubmed_extract(force=force)


@run.command("ebi-biosample")
@click.option("--start-day", default="2021-01-01")
@click.option("--end-day", default=None)
@click.option("--force", is_flag=True)
def run_ebi_biosample(start_day: str, end_day: str | None, force: bool) -> None:
    from omicidx.prefect.flows.ebi_biosample import ebi_biosample_extract

    ebi_biosample_extract(start_day=start_day, end_day=end_day, force=force)


@run.command("ducklake-load")
@click.option("--lake-schema", default=None, help="Override target lake schema.")
def run_ducklake_load(lake_schema: str | None) -> None:
    """Daily incremental load: upsert raw -> lake (SRA advances by watermark).

    For a full rebuild from raw instead, use `reproduce-from-raw`.
    """
    from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
    from omicidx.prefect.flows.ducklake_load import ducklake_load

    ducklake_load(lake_schema=lake_schema or LAKE_SCHEMA)


@run.command("reproduce-from-raw")
@click.option("--lake-schema", default=None, help="Override target lake schema.")
def run_reproduce_from_raw(lake_schema: str | None) -> None:
    """Rebuild the CURRENT lake tables by re-deriving them from retained raw.

    This does NOT restore a prior day's data — it reconstructs today's state by
    re-scanning every retained raw partition (SRA included). To read OLD state,
    use snapshot time travel instead (DUCKLAKE.md "Reading history":
    `... AT (VERSION => n)`).

    Idempotent: an unchanged re-run writes zero new data files (proven in
    tests/test_idempotency.py). Distinct from `ducklake-load`, the daily
    incremental load.
    """
    from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
    from omicidx.prefect.flows.ducklake_load import ducklake_load

    ducklake_load(lake_schema=lake_schema or LAKE_SCHEMA, force=True)


@run.command("ducklake-maintenance")
def run_ducklake_maintenance() -> None:
    from omicidx.prefect.flows.ducklake import ducklake_maintenance

    ducklake_maintenance()


@run.command("parquet-export")
@click.option("--lake-schema", default=None, help="Override source lake schema.")
def run_parquet_export(lake_schema: str | None) -> None:
    from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
    from omicidx.prefect.flows.parquet_export import parquet_export

    parquet_export(lake_schema=lake_schema or LAKE_SCHEMA)


@run.command("publish-bundle")
@click.option("--date", default=None, help="Bundle date (default: today UTC).")
def run_publish_bundle(date: str | None) -> None:
    """Build the external frozen bundle. Run after parquet-export."""
    from omicidx.prefect.flows.publish_bundle import publish_bundle

    publish_bundle(date=date)


@run.command("transform")
@click.option("--environment", default="prod", help="SQLMesh environment.")
def run_transform(environment: str) -> None:
    """Apply the SQLMesh marts into the lake (plan --auto-apply). Run after ducklake-load."""
    from omicidx.prefect.flows.transform import transform

    transform(environment=environment)


@run.command("postgres")
def run_postgres() -> None:
    from omicidx.prefect.flows.postgres import postgres_load

    postgres_load()


@run.command("duckdb")
def run_duckdb() -> None:
    from omicidx.prefect.flows.sql import build_omicidx_duckdb

    build_omicidx_duckdb()


@run.command("daily")
def run_daily() -> None:
    """The downstream chain: ducklake-load -> transform -> parquet-export -> postgres -> publish-bundle.

    Extraction is NOT included — each domain runs on its own timer (#149).
    """
    from omicidx.prefect.flows.main import daily_pipeline

    daily_pipeline()


if __name__ == "__main__":
    cli()
