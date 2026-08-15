"""The downstream pipeline: everything after raw extraction (#158).

    lake load → transform → parquet export → postgres load → publish bundle

Run by `systemd/omicidx-downstream.timer` as
`python -m omicidx.prefect.flows.main run`. One unit for the whole chain
rather than five: the stages are strictly sequential and each one's input is
the previous one's output, so five units would only buy `After=` ordering we
already get for free — while adding four more things that can be half-enabled.

Recovery after a mid-chain failure is per stage, not per chain:
`omicidx-prefect run transform`, `... run postgres`, and so on. Every stage is
idempotent, so re-running from an earlier stage is also safe, just slower.

Raw extraction is NOT here. Each domain is its own scheduled EL process on its
own timer (#149) — `omicidx-{sra,pubmed,biosample,ebi-biosample}-extract` — so
this chain loads whatever raw those timers have already landed on R2. GEO alone
has no timer yet (#154/#174); until it does, GEO raw goes stale unless someone
runs `omicidx-prefect run geo` by hand.
"""

import logging

import click
from omicidx.prefect.flows.ducklake_load import ducklake_load
from omicidx.prefect.flows.geo import fetch_rna_seq_counts
from omicidx.prefect.flows.parquet_export import parquet_export
from omicidx.prefect.flows.postgres import postgres_load
from omicidx.prefect.flows.publish_bundle import publish_bundle
from omicidx.prefect.flows.transform import transform

log = logging.getLogger(__name__)


def daily_pipeline() -> None:
    """Load the lake from raw, transform, export, serve, publish.

    ``parquet_export`` returns the publish date it stamped v{date}/ with, and
    ``publish_bundle`` is pinned to that same date, so the frozen bundle (file
    catalog + omicidx.duckdb + views.sql + manifest) references the Parquet just
    written rather than whatever "today" is by the time it runs.
    """
    # ponytail: the one non-extract that has to lead. `fetch_rna_seq_counts` is
    # a ~1min eutils call whose output `geo_rnaseq_counts_to_ducklake` reads
    # during the load below, so it belongs to this chain, not to a timer of its
    # own. It rode inside `raw_extract_flow` only because that flow ran first.
    fetch_rna_seq_counts()
    ducklake_load()
    transform()
    date = parquet_export()
    postgres_load()
    publish_bundle(date=date)


@click.group()
def cli() -> None:
    """Downstream pipeline (`python -m omicidx.prefect.flows.main run`)."""


@cli.command("run")
def run_command() -> None:
    """Run the whole downstream chain, in order, failing loudly."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    daily_pipeline()
    click.echo("downstream: load → transform → export → postgres → publish complete")


if __name__ == "__main__":
    cli()
