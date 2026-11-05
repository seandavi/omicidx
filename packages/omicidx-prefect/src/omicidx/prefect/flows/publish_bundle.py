"""Bundle publisher (ADR-0004 β; cdsci-lake ADR-0027): the external daily bundle.

Deep module, narrow interface: ``publish_bundle() -> bundle at v{date}/ and
latest/``. The mechanics are ``cdsci.publish``: ``build_bundle`` streams every
published table out of the lake into one local directory (flat Parquet +
``omicidx.duckdb`` + ``views.sql`` + ``restore.sql`` + ``manifest.json``) and
``cdsci.publish.upload.upload`` puts it at ``v{date}/`` then ``latest/``, each
folder's views pointing at its own Parquet, manifest.json last, then prunes the
dated folders ``keep_daily_weekly`` does not keep (ADR-0007).

What stays here is omicidx policy: which lake tables are published and under
which file names (``EXPORTS``, ``TAXONOMY_RESOLVED_EXPORTS``, ``MART_EXPORTS``),
the public layout (base tables at the folder top level, marts under
``sradb/``/``geometadb/``), the retention counts, and the omicidx manifest
fields (``publish_date``, ``dated_path``, ``transform_sha``,
``lake_snapshot_id``, ``raw_partition_counts``), kept beside the shared ones so
no existing reader breaks.

The whole bundle is staged on local disk first (``TMPDIR``, see
``systemd/README.md``), so the lake is read exactly once per table.
"""

import json
import logging
import shutil
import tempfile
from pathlib import Path

from cdsci.publish import publish
from cdsci.publish.upload import keep_daily_weekly, upload
from omicidx.prefect.config import (
    get_ducklake_connection,
    get_public_filesystem,
    public_bucket,
    settings,
)
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.semaphore import SemaphoreStore

log = logging.getLogger(__name__)

DB_FILE = "omicidx.duckdb"

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
# per mart under <schema>/. The thin public duckdb is nothing but views over
# exactly these files — no re-derivation. Names preserve the published
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

# Semaphore namespaces to summarize in the manifest (DUCKLAKE.md raw-partition
# table). SRA fans out per entity under sra/<entity>.
RAW_NAMESPACES = [
    "sra/study",
    "sra/sample",
    "sra/experiment",
    "sra/run",
    "geo",
    "biosample",
    "bioproject",
    "ebi_biosample",
]


def bundle_path(schema: str, table: str) -> str:
    """Public layout: base tables (``main``) at the top level, marts under ``<schema>/``."""
    return f"{table}.parquet" if schema == "main" else f"{schema}/{table}.parquet"


def bundle_tables(lake_schema: str = LAKE_SCHEMA) -> list[tuple[str, str, str]]:
    """``(schema, table, SELECT)`` for every public file, in publish order."""
    tables = []
    for lake_table, filename in EXPORTS:
        if lake_table in TAXONOMY_RESOLVED_EXPORTS:
            source = f"lake.stg.{TAXONOMY_RESOLVED_EXPORTS[lake_table]}"
        else:
            source = f"lake.{lake_schema}.{lake_table}"
        tables.append(
            ("main", filename.removesuffix(".parquet"), f"SELECT * FROM {source}")
        )
    for schema, table in MART_EXPORTS:
        tables.append((schema, table, f"SELECT * FROM lake.{schema}.{table}"))
    return tables


def _lake_snapshot_id(con) -> int | None:
    """Current internal-lake snapshot id — the id this publish was cut from."""
    try:
        row = con.execute("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()
    except Exception:  # not a DuckLake (an offline test's plain ATTACH)
        return None
    return int(row[0]) if row and row[0] is not None else None


def _raw_partition_counts() -> dict[str, int]:
    # v1 records partition *counts* per source; full enumeration (needed to
    # reproduce a historical snapshot exactly) is the B′ upgrade.
    return {ns: len(SemaphoreStore(ns).list_keys()) for ns in RAW_NAMESPACES}


def omicidx_manifest_fields(manifest: dict, con) -> dict:
    """omicidx's provenance fields (spec §2a), additive to the shared manifest.

    ``publish_date`` and ``transform_sha`` duplicate the shared ``release`` and
    ``git_sha``; both spellings stay within manifest_version 1 so existing
    readers (docs/releases.qmd, the versions guide, omicidxr) keep working.
    """
    return {
        "publish_date": manifest["release"],
        "dated_path": f"v{manifest['release']}/",
        "transform_sha": manifest["git_sha"],
        "lake_snapshot_id": _lake_snapshot_id(con),
        "raw_partition_counts": _raw_partition_counts(),
    }


def build_bundle(
    out: Path,
    con,
    lake_schema: str = LAKE_SCHEMA,
    only: set[str] | None = None,
) -> dict:
    """Build the bundle in ``out`` (new or empty) from ``con``, which has the lake
    attached as ``lake``. Returns the manifest, as written to ``out/manifest.json``.

    The views in this local build point at ``latest/``; ``upload`` regenerates
    them per folder. ``only`` limits the build to the named ``schema.table``s.
    """
    base = settings().public_parquet_https_base
    if not base:
        raise RuntimeError("PUBLIC_PARQUET_HTTPS_BASE is not set")
    manifest = publish(
        con,
        out,
        tables=bundle_tables(lake_schema),
        docs=lambda name: (None, {}),
        base_url=f"{base.rstrip('/')}/latest",
        db_file=DB_FILE,
        code_dir=Path(__file__).resolve().parent,
        path=bundle_path,
        only=only,
    )
    manifest.update(omicidx_manifest_fields(manifest, con))
    (out / "manifest.json.tmp").write_text(json.dumps(manifest, indent=2) + "\n")
    (out / "manifest.json.tmp").replace(out / "manifest.json")
    return manifest


def upload_bundle(bundle: Path, apply: bool = True) -> None:
    """Put ``bundle`` at ``v{release}/`` then ``latest/``, then prune (ADR-0007).

    Dry run unless ``apply``: the plan is printed, nothing is written.
    """
    s = settings()
    if not s.public_parquet_https_base:
        raise RuntimeError("PUBLIC_PARQUET_HTTPS_BASE is not set")
    upload(
        bundle,
        get_public_filesystem(),
        public_bucket(),
        public_url=s.public_parquet_https_base,
        db_file=DB_FILE,
        retention=keep_daily_weekly(
            s.public_bundle_keep_daily, s.public_bundle_keep_weekly
        ),
        apply=apply,
    )


def publish_bundle(out: Path | None = None, apply: bool = True) -> dict:
    """Build today's bundle from the lake and publish it.

    Runs after ``transform`` in the daily chain. ``out`` keeps the local build
    (default: a ``TMPDIR`` directory removed afterwards).
    """
    workdir = out or Path(tempfile.mkdtemp(prefix="omicidx-bundle-"))
    with get_ducklake_connection() as con:
        manifest = build_bundle(workdir, con)
    upload_bundle(workdir, apply=apply)
    if out is None:
        shutil.rmtree(workdir)
    log.info(f"Published bundle v{manifest['release']}/ and latest/")
    return manifest


if __name__ == "__main__":
    publish_bundle()
