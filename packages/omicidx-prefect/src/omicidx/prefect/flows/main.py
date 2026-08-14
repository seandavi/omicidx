"""Top-level pipeline flows.

`daily_pipeline_flow` runs the raw extracts, then the DuckLake load
(MERGE raw → lake.omicidx.*), then the parquet export (lake → public
Parquet, the reverse-ETL), then the postgres loads (which read the lake
directly), then the duckdb build (which reads the exported Parquet). Each
step is a subflow, so failure of one stage halts the rest with full
visibility.

The old `consolidate` step (raw → consolidated parquet) is fully replaced
by `ducklake-load` + `parquet-export`: the lake is the single source of
truth, and `parquet-export` COPYs the merged tables out for public serving
and the duckdb build. `consolidate.py` is dead once `parquet-export` is
verified in prod.
"""

from omicidx.prefect.flows.biosample import (
    bioproject_extract_flow,
    biosample_extract_flow,
)
from omicidx.prefect.flows.ducklake_load import ducklake_load_flow
from omicidx.prefect.flows.ebi_biosample import ebi_biosample_extract_flow
from omicidx.prefect.flows.geo import geo_rna_seq_counts_flow
from omicidx.prefect.flows.parquet_export import parquet_export_flow
from omicidx.prefect.flows.postgres import postgres_load_flow
from omicidx.prefect.flows.publish_bundle import publish_bundle_flow
from omicidx.prefect.flows.pubmed import pubmed_extract_flow
from omicidx.prefect.flows.sra import sra_extract_flow
from omicidx.prefect.flows.transform import transform_flow

from prefect import flow


@flow(name="raw-extract")
def raw_extract_flow(force: bool = False) -> None:
    """Run every raw extractor. Mirrors `daily_extract_schedule`."""
    biosample_extract_flow(force=force)
    bioproject_extract_flow(force=force)
    sra_extract_flow(force=force)
    # ponytail: geo-extract is deliberately absent. It wedges on 2020-07 and
    # held the whole pipeline in `Running` from 2026-08-08, so every stage
    # below it — including the publish — has not run for a month. Four healthy
    # domains flowing beats five domains blocked. GEO returns as its own
    # scheduled EL process (#154), not here; ad-hoc runs still work via the
    # `geo-extract` deployment and `omicidx-prefect run geo-extract`.
    geo_rna_seq_counts_flow()
    ebi_biosample_extract_flow(force=force)
    pubmed_extract_flow(force=force)


@flow(name="daily-pipeline", timeout_seconds=36000)
def daily_pipeline_flow(force: bool = False) -> None:
    """Daily pipeline: extract → ducklake-load → transform → parquet-export → postgres → publish.

    ``parquet-export`` returns the publish date it stamped v{date}/ with;
    ``publish-bundle`` is pinned to that same date so the frozen bundle
    (file catalog + omicidx.duckdb + views.sql + manifest) references the
    Parquet just written. ``publish-bundle`` builds omicidx.duckdb itself
    (Stage B2: duckdb-build output redirected into the bundle), so the old
    standalone ``omicidx-duckdb-build`` no longer runs inside the pipeline.

    ``timeout_seconds=36000`` (10h): historical successful runs take
    6.3-7h; a stall (e.g. a network call that hangs instead of erroring)
    should fail loudly well before the 12h "stuck run" automation's
    backstop, not sit blue in the UI indefinitely.
    """
    raw_extract_flow(force=force)
    ducklake_load_flow()
    transform_flow()
    date = parquet_export_flow()
    postgres_load_flow()
    publish_bundle_flow(date=date)


if __name__ == "__main__":
    daily_pipeline_flow()
