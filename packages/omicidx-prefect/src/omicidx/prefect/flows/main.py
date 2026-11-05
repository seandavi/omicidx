"""The downstream pipeline: everything after raw extraction (#158).

    lake load → transform → publish bundle

Run by `systemd/omicidx-downstream.timer` as
`python -m omicidx.prefect.flows.main run`. One unit for the whole chain
rather than three: the stages are strictly sequential and each one's input is
the previous one's output, so separate units would only buy `After=` ordering we
already get for free — while adding more things that can be half-enabled.

Recovery after a mid-chain failure is per stage, not per chain:
`omicidx-prefect run transform`, `... run publish-bundle`, and so on. Every stage is
idempotent, so re-running from an earlier stage is also safe, just slower.

Raw extraction is NOT here. Each domain is its own scheduled EL process on its
own timer (#149) — `omicidx-{sra,pubmed,biosample,ebi-biosample}-extract` — so
this chain loads whatever raw those timers have already landed on R2. GEO's
timer alone is not installed yet — it owes a 74-month backfill first (#174) —
so GEO raw goes stale unless someone runs `omicidx-prefect run geo` by hand.
"""

import logging

import click
from omicidx.prefect.flows.ducklake_load import ducklake_load
from omicidx.prefect.flows.geo import fetch_rna_seq_counts
from omicidx.prefect.flows.publish_bundle import publish_bundle
from omicidx.prefect.flows.transform import transform

log = logging.getLogger(__name__)


def daily_pipeline() -> None:
    """Load the lake from raw, transform, publish.

    ``publish_bundle`` builds the whole bundle (Parquet + omicidx.duckdb +
    views.sql + restore.sql + manifest.json) locally in one pass over the lake,
    then uploads it to v{date}/ and latest/ (cdsci-lake ADR-0027).
    """
    # ponytail: the one non-extract that has to lead. `fetch_rna_seq_counts` is
    # a ~1min eutils call whose output `geo_rnaseq_counts_to_ducklake` reads
    # during the load below, so it belongs to this chain, not to a timer of its
    # own. It rode inside `raw_extract_flow` only because that flow ran first.
    fetch_rna_seq_counts()
    ducklake_load()
    transform()
    publish_bundle()


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
    click.echo("downstream: load → transform → publish complete")


if __name__ == "__main__":
    cli()
