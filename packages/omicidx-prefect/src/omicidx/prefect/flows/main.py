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

from omicidx.prefect.flows.ducklake_load import ducklake_load_flow
from omicidx.prefect.flows.geo import geo_rna_seq_counts_flow
from omicidx.prefect.flows.parquet_export import parquet_export_flow
from omicidx.prefect.flows.postgres import postgres_load_flow
from omicidx.prefect.flows.publish_bundle import publish_bundle_flow
from omicidx.prefect.flows.transform import transform_flow

from prefect import flow


@flow(name="raw-extract")
def raw_extract_flow(force: bool = False) -> None:
    """Run the raw extractors that have not yet moved to their own timer.

    Hollowing out on purpose (#149): each domain migrated to a standalone
    scheduled EL process is removed here in the same change that adds its
    timer, or it would extract twice — once on its timer, once inside
    `daily-pipeline`. When the last one goes, `daily-pipeline` is nothing but
    the downstream chain, which is what #158 replaces.
    """
    # ponytail: biosample/bioproject-extract are deliberately absent. They are
    # one standalone EL process (#155) on `systemd/omicidx-biosample-extract.timer`;
    # ad-hoc runs are `python -m omicidx.prefect.flows.biosample run` or
    # `omicidx-prefect run biosample` / `run bioproject`.
    # ponytail: sra-extract is deliberately absent. It is the pilot standalone
    # EL process (#153) and now runs on its own systemd timer
    # (`systemd/omicidx-sra-extract.timer`); ad-hoc runs are
    # `python -m omicidx.prefect.flows.sra run` or `omicidx-prefect run sra`.
    # ponytail: geo-extract is deliberately absent. It wedges on 2020-07 and
    # held the whole pipeline in `Running` from 2026-08-08, so every stage
    # below it — including the publish — has not run for a month. Four healthy
    # domains flowing beats five domains blocked. GEO returns as its own
    # scheduled EL process (#154), not here; ad-hoc runs still work via the
    # `geo-extract` deployment and `omicidx-prefect run geo-extract`.
    # ponytail: ebi-biosample-extract is deliberately absent. It now runs on its
    # own systemd timer (`systemd/omicidx-ebi-biosample-extract.timer`, daily
    # 02:00 UTC); ad-hoc runs are
    # `python -m omicidx.prefect.flows.ebi_biosample run` or
    # `omicidx-prefect run ebi-biosample`. With it goes the last *extract*:
    # `geo_rna_seq_counts_flow` below is a derived flow, not an extract, so
    # where it belongs is #158's call, not this ticket's.
    geo_rna_seq_counts_flow()
    # ponytail: pubmed-extract is deliberately absent. It now runs on its own
    # systemd timer (`systemd/omicidx-pubmed-extract.timer`, hourly, the cadence
    # its retired `pubmed-extract` deployment had); ad-hoc runs are
    # `python -m omicidx.prefect.flows.pubmed run` or `omicidx-prefect run pubmed`.


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
