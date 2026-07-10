"""DuckLake load flow: upsert raw → lake.<schema>.* (incremental).

Each entity is upserted into the DuckLake catalog by its natural key via
cdsci-lake's `ops.run` + `upsert` (ADR-0005). The upsert source is a
deduped, typed projection of the raw data; `upsert` gates UPDATEs on IS
DISTINCT FROM so unchanged rows never rewrite a data file — DuckLake is
copy-on-write, so an idempotent re-run writes no new files and only a
trivial snapshot.

This module keeps the bioproject loader plus the shared write helpers
still used by the read-consumers and the parked derived loaders
(`_stamped_txn`, `replace_to_ducklake`, `_commit_extra`).

This sits between `raw-extract` and `postgres-load`. Loaders write to
`LAKE_SCHEMA` (production `omicidx`); pass an explicit `lake_schema` to
target a development schema (e.g. `omicidx_dev`) for validation.

`cdsci-lake` (the catalog's data bucket) is ducklake-controlled
exclusively. Raw inputs are read from PUBLISH_ROOT (a different bucket)
via `get_duckdb_path`; nothing else is written into the lake bucket.
"""

from contextlib import contextmanager

import duckdb
import orjson
from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import (
    get_duckdb_path,
    get_ducklake_connection,
    get_lake_connection,
)

from prefect import get_run_logger, task
from prefect.runtime import flow_run

# Production lake schema. (Was omicidx_dev during the transition.)
LAKE_SCHEMA = "omicidx"


def _commit_extra(**fields: object) -> str:
    """JSON blob for a snapshot's commit_extra_info, tagged with run id."""
    return orjson.dumps({"prefect_run_id": flow_run.get_id(), **fields}).decode()


@contextmanager
def _stamped_txn(
    con: duckdb.DuckDBPyConnection,
    author: str,
    message: str,
    extra_info: str | None,
):
    """Wrap DML in a transaction stamped with snapshot commit metadata.

    The stamp MUST share a transaction with the DML — DuckLake clears it
    on commit, so an auto-committed statement would lose it. A no-op DML
    writes no snapshot, so the stamp simply doesn't land.
    """
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "CALL ducklake_set_commit_message('lake', ?, ?, extra_info := ?)",
            [author, message, extra_info],
        )
        yield
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def replace_to_ducklake(
    con: duckdb.DuckDBPyConnection,
    *,
    schema: str,
    table: str,
    source_sql: str,
    author: str = "prefect:ducklake-load",
    commit_message: str | None = None,
    commit_extra_info: str | None = None,
) -> int:
    """Full-replace lake.<schema>.<table> with a derived query result.

    For derived tables that are cheaper to rebuild than to merge
    (sra_accessions, geo_series_with_rnaseq_counts, the linkage table).
    Stamped like `merge_to_ducklake` so snapshots stay self-documenting.
    """
    con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
    message = commit_message or f"ducklake-load: replace {schema}.{table}"
    with _stamped_txn(con, author, message, commit_extra_info):
        con.execute(f'CREATE OR REPLACE TABLE lake.{schema}."{table}" AS {source_sql}')
    return con.execute(f'SELECT count(*) FROM lake.{schema}."{table}"').fetchone()[0]


# -- bioproject (POC) ----------------------------------------------------------

# Full-dump source: one record per accession already, but we dedup
# defensively. `upsert` gates rewrites via IS DISTINCT FROM (no hash column).
_BIOPROJECT_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(accession) AS accession,
        trim(title) AS title,
        trim(description) AS description,
        trim(name) AS name,
        publications,
        locus_tags,
        release_date,
        data_types,
        external_links,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY release_date DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto('{path}', maximum_object_size = 1000000000)
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""


@task(retries=1, retry_delay_seconds=60)
def bioproject_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Upsert raw bioproject JSONL → lake.<lake_schema>.bioproject.

    Snapshot attribution is automatic (author `omicidx:bioproject`) via the
    `ops.run` block; the MERGE gates on IS DISTINCT FROM, no `_row_hash`.
    """
    log = get_run_logger()
    raw = get_duckdb_path("bioproject", "raw", "data.jsonl.gz")
    source_sql = _BIOPROJECT_SOURCE.format(path=raw)
    target = f"lake.{lake_schema}.bioproject"
    with get_lake_connection() as con:
        log.info(f"Merging {raw} → {target}")
        with ops.run(
            con,
            source="bioproject",
            target=target,
            extra={"prefect_run_id": flow_run.get_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="accession")
        log.info(f"{target} now holds {r.rows:,} rows")
        return r.summary()


# -- maintenance ---------------------------------------------------------------


@task(retries=1, retry_delay_seconds=60)
def ducklake_maintenance(
    expire_older_than: str = "now() - INTERVAL 30 DAY",
    compact: bool = True,
) -> dict:
    """Expire old snapshots, delete their data files, and compact.

    DROP/rewrite in DuckLake only unlinks in the catalog; reclaiming R2
    space needs expire_snapshots + cleanup_old_files. Compaction
    (merge_adjacent_files) coalesces the many small parquet files that
    incremental MERGEs accumulate. Default retention is 30 days of
    snapshots (appropriate for incremental tables; full-snapshot tables
    keep little useful history, so a tighter window can be passed).
    """
    log = get_run_logger()
    with get_ducklake_connection() as con:
        con.execute(
            f"CALL ducklake_expire_snapshots('lake', older_than => {expire_older_than})"
        )
        deleted = con.execute(
            "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"
        ).fetchall()
        if compact:
            con.execute("CALL ducklake_merge_adjacent_files('lake')")
        remaining = con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0]
    log.info(
        f"Cleaned {len(deleted)} orphaned files; compact={compact}; "
        f"{remaining} snapshots remain"
    )
    return {
        "files_deleted": len(deleted),
        "compacted": compact,
        "snapshots_remaining": remaining,
    }
