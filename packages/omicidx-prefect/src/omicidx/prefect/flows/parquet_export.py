"""Parquet export flow: lake.<schema>.* → public Parquet (reverse-ETL).

The public-serving half of ADR-0004. Each core lake table is COPY'd out of
the DuckLake catalog to a flat Parquet file under the dedicated public bucket
(`PUBLIC_PARQUET_ROOT`, e.g. `r2://data-omicidx`).

Two prefixes per run (spec §2, Stage B1):

- immutable ``v{date}/<file>.parquet`` — the dated snapshot, written once,
  never overwritten. The reproducibility + provenance anchor.
- rolling ``latest/<file>.parquet`` — today's dated file re-written to the
  stable URLs ``views.sql`` / ``duckdb-build`` read.

The COPY reads the lake once (into ``v{date}/``); ``latest/`` is written by
re-reading that dated Parquet over httpfs, not a second read of the lake (and
not an s3fs server-side copy — R2 flakes on multi-GB objects). Dated folders older
than ``PUBLIC_BUNDLE_RETENTION_DAYS`` are pruned (v1 bounded window; B′ opens
the dial for retained-snapshot time travel).

The R2 secret built by ``get_ducklake_connection()`` is account-scoped, so the
COPY into a *different* bucket than the lake's ``cdsci-lake`` reuses it with no
extra credentials.
"""

from datetime import UTC, datetime, timedelta

from omicidx.prefect.config import (
    get_ducklake_connection,
    get_public_parquet_path,
    get_public_upath,
    publish_date,
    settings,
)
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA

from prefect import flow, get_run_logger, task

# (lake table, public parquet filename). Names differ: lake tables are
# singular; the public files use the plural form. Authoritative against the
# ducklake_* loaders. These base-parquet exports are a public deliverable in
# their own right (ADR-0004), separate from the marts below.
EXPORTS: list[tuple[str, str]] = [
    ("bioproject", "bioprojects.parquet"),
    ("biosample", "biosamples.parquet"),
    ("geo_series", "geo_series.parquet"),
    ("geo_sample", "geo_samples.parquet"),
    ("geo_platform", "geo_platforms.parquet"),
    ("geo_series_with_rnaseq_counts", "geo_series_with_rnaseq_counts.parquet"),
    ("sra_study", "sra_studies.parquet"),
    ("sra_sample", "sra_samples.parquet"),
    ("sra_experiment", "sra_experiments.parquet"),
    ("sra_run", "sra_runs.parquet"),
    ("sra_accessions", "sra_accessions.parquet"),
    ("pubmed_article", "pubmed_articles.parquet"),
]

# SQLMesh-materialized marts: lake.{sradb,geometadb}.* → public parquet, one file
# per mart under <schema>/. The thin public duckdb (flows/sql.py) is nothing but
# views over exactly these files — no re-derivation. Names preserve the published
# sradb.*/geometadb.* contract verbatim. Authoritative against transform/models/.
MART_EXPORTS: list[tuple[str, str]] = [
    ("sradb", "study"),
    ("sradb", "sample"),
    ("sradb", "experiment"),
    ("sradb", "run"),
    ("sradb", "run_with_study"),
    ("sradb", "sra"),
    ("sradb", "human_runs"),
    ("sradb", "mouse_runs"),
    ("sradb", "rnaseq_runs"),
    ("sradb", "wgs_runs"),
    ("geometadb", "gse"),
    ("geometadb", "gsm"),
    ("geometadb", "gpl"),
    ("geometadb", "gse_gsm"),
    ("geometadb", "gse_gpl"),
    ("geometadb", "geo_supplemental_files"),
]


@task(retries=1, retry_delay_seconds=60)
def export_table(
    lake_table: str, filename: str, date: str, lake_schema: str = LAKE_SCHEMA
) -> dict:
    """COPY lake.<schema>.<lake_table> → v{date}/<filename>, mirror to latest/.

    The lake is read once (COPY to the dated file); ``latest/`` is written by
    re-reading that dated Parquet over httpfs — not a second lake read, and not
    an s3fs server-side copy (see the inline note).
    """
    log = get_run_logger()
    dated = get_public_parquet_path(f"v{date}", filename)
    latest = get_public_parquet_path("latest", filename)
    with get_ducklake_connection() as con:
        log.info(f"Exporting lake.{lake_schema}.{lake_table} → {dated}")
        con.execute(
            f"COPY (SELECT * FROM lake.{lake_schema}.{lake_table}) "
            f"TO '{dated}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{dated}')"
        ).fetchone()[0]
        # Mirror to rolling latest/ by re-reading the dated file over httpfs.
        # ponytail: NOT an s3fs server-side copy — R2 times out / 502s copying
        # multi-GB objects (biosamples is 7.8 GB). DuckDB's httpfs multipart
        # write is the path that reliably lands the dated file, so reuse it.
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{dated}')) "
            f"TO '{latest}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )

    log.info(f"Wrote {row_count:,} rows to {dated} and mirrored to latest/")
    return {
        "table": f"{lake_schema}.{lake_table}",
        "file": filename,
        "row_count": row_count,
    }


@task
def prune_dated_folders(keep_date: str) -> list[str]:
    """Delete immutable v{date}/ folders older than the retention window.

    ``keep_date`` (today) is always kept even if retention is 0. Folders are
    identified by the ``vYYYY-MM-DD`` prefix; anything else under the public
    root (``latest/``, ``duckdb/``, ...) is left untouched.
    """
    log = get_run_logger()
    days = settings().public_bundle_retention_days
    cutoff = (
        datetime.strptime(keep_date, "%Y-%m-%d").replace(tzinfo=UTC)
        - timedelta(days=days)
    ).date()

    root = get_public_upath()
    removed: list[str] = []
    for child in root.iterdir():
        name = child.name
        if not (name.startswith("v") and len(name) == 11):
            continue
        try:
            folder_date = datetime.strptime(name[1:], "%Y-%m-%d").date()
        except ValueError:
            continue
        if name[1:] != keep_date and folder_date < cutoff:
            log.info(f"Pruning old dated folder {name}/ (< {cutoff})")
            child.fs.rm(child.path, recursive=True)
            removed.append(name)
    log.info(f"Pruned {len(removed)} dated folder(s); retention={days}d")
    return removed


@flow(name="parquet-export")
def parquet_export_flow(date: str | None = None, lake_schema: str = LAKE_SCHEMA) -> str:
    """Export every core lake table to v{date}/ + latest/, prune old folders.

    Sits between ``ducklake-load`` and ``publish-bundle`` in the daily
    pipeline: the bundle build and the public ``views.sql`` read these files.
    Returns the publish date used, so the bundle build can pin to it.
    """
    date = date or publish_date()
    for lake_table, filename in EXPORTS:
        export_table(lake_table, filename, date, lake_schema=lake_schema)
    for schema, table in MART_EXPORTS:
        export_table(table, f"{schema}/{table}.parquet", date, lake_schema=schema)
    prune_dated_folders(date)
    return date


if __name__ == "__main__":
    parquet_export_flow()
