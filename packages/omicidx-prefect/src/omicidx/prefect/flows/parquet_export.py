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
not an s3fs server-side copy — R2 flakes on multi-GB objects). Old dated folders
are pruned to the newest ``PUBLIC_BUNDLE_KEEP_DAILY`` plus one per ISO week for
the newest ``PUBLIC_BUNDLE_KEEP_WEEKLY`` weeks (``select_dated_keep``; v1 bounded
window; B′ opens the dial for retained-snapshot time travel).

The R2 secret built by ``get_ducklake_connection()`` is account-scoped, so the
COPY into a *different* bucket than the lake's ``cdsci-lake`` reuses it with no
extra credentials.
"""

import logging
from datetime import datetime

from omicidx.prefect.config import (
    get_ducklake_connection,
    get_public_parquet_path,
    get_public_upath,
    publish_date,
    settings,
)
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.run import retry

log = logging.getLogger(__name__)

# (lake table, public parquet filename). Names differ: lake tables are
# singular; the public files use the plural form. Authoritative against the
# ducklake_* loaders. These base-parquet exports are a public deliverable in
# their own right (ADR-0004), separate from the marts below.
EXPORTS: list[tuple[str, str]] = [
    ("bioproject", "bioprojects.parquet"),
    ("biosample", "biosamples.parquet"),
    ("ebi_biosample", "ebi_biosamples.parquet"),
    ("geo_series", "geo_series.parquet"),
    ("geo_sample", "geo_samples.parquet"),
    ("geo_platform", "geo_platforms.parquet"),
    ("geo_series_with_rnaseq_counts", "geo_series_with_rnaseq_counts.parquet"),
    ("sra_study", "sra_studies.parquet"),
    ("sra_sample", "sra_samples.parquet"),
    ("sra_experiment", "sra_experiments.parquet"),
    ("sra_run", "sra_runs.parquet"),
    ("sra_accessions", "sra_accessions.parquet"),
]

# Base files that carry organism names are COPY'd from their SQLMesh stg view
# rather than the lake table (#226). Each of these stg views is its lake table
# column for column, except the names are resolved by taxid against current NCBI
# Taxonomy and the submitted names ride along as extra `*_as_submitted` columns.
# GEO renames organisms without bumping last_update_date, so the lake's
# as-extracted names go stale. lake table -> stg view; the stg views always read
# the prod `omicidx` schema, whatever `lake_schema` says.
TAXONOMY_RESOLVED_EXPORTS: dict[str, str] = {
    "biosample": "biosamples",
    "geo_series": "geo_series",
    "geo_sample": "geo_samples",
    "geo_platform": "geo_platforms",
    "sra_sample": "sra_samples",
}

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


@retry
def export_table(
    lake_table: str, filename: str, date: str, lake_schema: str = LAKE_SCHEMA
) -> dict:
    """COPY lake.<schema>.<lake_table> → v{date}/<filename>, mirror to latest/.

    The lake is read once (COPY to the dated file); ``latest/`` is written by
    re-reading that dated Parquet over httpfs — not a second lake read, and not
    an s3fs server-side copy (see the inline note).
    """
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


def select_dated_keep(
    dates: list[str], keep_date: str, daily: int, weekly: int
) -> set[str]:
    """The dated folders (``YYYY-MM-DD``) to keep; everything else is pruned.

    The ``daily`` newest folders, plus the newest folder of each of the
    ``weekly`` newest ISO weeks (normally Sunday's build, but any build if that
    night failed), plus ``keep_date`` (today's, just written) always. Counts,
    not ages, so a run of failed nights never empties the window.
    """
    newest_first = sorted(set(dates), reverse=True)
    keep = set(newest_first[:daily]) | {keep_date}
    weeks: dict[tuple[int, int], str] = {}
    for d in newest_first:
        iso = datetime.strptime(d, "%Y-%m-%d").isocalendar()
        weeks.setdefault((iso.year, iso.week), d)
    keep |= set(sorted(weeks.values(), reverse=True)[:weekly])
    return keep


def prune_dated_folders(keep_date: str) -> list[str]:
    """Delete the immutable v{date}/ folders ``select_dated_keep`` doesn't keep.

    ``keep_date`` (today) is always kept. Folders are identified by the
    ``vYYYY-MM-DD`` prefix; anything else under the public root (``latest/``,
    ``duckdb/``, ...) is left untouched.
    """
    s = settings()
    root = get_public_upath()
    folders = {}
    for child in root.iterdir():
        name = child.name
        if not (name.startswith("v") and len(name) == 11):
            continue
        try:
            datetime.strptime(name[1:], "%Y-%m-%d")
        except ValueError:
            continue
        folders[name[1:]] = child
    keep = select_dated_keep(
        list(folders),
        keep_date,
        s.public_bundle_keep_daily,
        s.public_bundle_keep_weekly,
    )
    removed: list[str] = []
    for d in sorted(set(folders) - keep):
        log.info(f"Pruning dated folder v{d}/")
        folders[d].fs.rm(folders[d].path, recursive=True)
        removed.append(f"v{d}")
    log.info(
        f"Pruned {len(removed)} dated folder(s); kept {len(keep & set(folders))} "
        f"(daily={s.public_bundle_keep_daily}, weekly={s.public_bundle_keep_weekly})"
    )
    return removed


def parquet_export(date: str | None = None, lake_schema: str = LAKE_SCHEMA) -> str:
    """Export every core lake table to v{date}/ + latest/, prune old folders.

    Sits between ``ducklake-load`` and ``publish-bundle`` in the daily
    pipeline: the bundle build and the public ``views.sql`` read these files.
    Returns the publish date used, so the bundle build can pin to it.
    """
    date = date or publish_date()
    for lake_table, filename in EXPORTS:
        if lake_table in TAXONOMY_RESOLVED_EXPORTS:
            stg_view = TAXONOMY_RESOLVED_EXPORTS[lake_table]
            export_table(stg_view, filename, date, lake_schema="stg")
        else:
            export_table(lake_table, filename, date, lake_schema=lake_schema)
    for schema, table in MART_EXPORTS:
        export_table(table, f"{schema}/{table}.parquet", date, lake_schema=schema)
    prune_dated_folders(date)
    return date


if __name__ == "__main__":
    parquet_export()
