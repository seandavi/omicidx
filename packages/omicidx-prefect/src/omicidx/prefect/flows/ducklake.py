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

    Dormant / transform-layer only (cdsci-lake ADR-0013): used by the parked
    derived loaders in `flows/_parked/`, not the EL write path. Stamped via
    `_stamped_txn` so snapshots stay self-documenting.
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
    `ops.run` block; `upsert` gates on IS DISTINCT FROM, no `_row_hash`.
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
    retention_days: int | None = None,
    compact: bool = True,
) -> dict:
    """Reclaim space (cleanup + compaction) and, optionally, expire snapshots.

    Retention is UNBOUNDED by default (``retention_days=None``): the internal
    lake is omicidx's primary time machine (spec §1). upsert deltas are small
    under copy-on-write — only changed rows are rewritten, and for several
    sources the raw inputs are *larger* than the lake — so keeping every
    snapshot is cheap. Raw Parquet under PUBLISH_ROOT is the re-derivation
    backstop: re-deriving from the *retained* raw reproduces lake state (it is
    not a re-fetch from NCBI/EBI, which move). It is insurance, not the query
    path.

    Space is still reclaimed safely without expiry: ``cleanup_old_files`` only
    deletes files unreferenced by ANY retained snapshot, and
    ``merge_adjacent_files`` coalesces the small parquet files that incremental
    upserts accumulate. Neither drops snapshot history — a data file pinned by a
    retained snapshot is never removed.

    ``ducklake_expire_snapshots`` is the only call that removes history. The
    DEFAULT path never runs it, so the scheduled/default invocation can only
    extend retention. Passing an explicit ``retention_days`` (operator opt-in)
    deliberately re-enables bounded expiry — the one path that shortens
    retention. ``retention_days`` is a typed int (not a raw SQL interval), so it
    cannot inject SQL.
    """
    log = get_run_logger()
    with get_ducklake_connection() as con:
        if retention_days is not None:
            con.execute(
                "CALL ducklake_expire_snapshots('lake', "
                f"older_than => now() - INTERVAL {int(retention_days)} DAY)"
            )
        deleted = con.execute(
            "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)"
        ).fetchall()
        if compact:
            con.execute("CALL ducklake_merge_adjacent_files('lake')")
        remaining = con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0]
    log.info(
        f"Cleaned {len(deleted)} orphaned files; compact={compact}; "
        f"expired={'none (unbounded)' if retention_days is None else f'>{int(retention_days)}d'}; "
        f"{remaining} snapshots remain"
    )
    return {
        "files_deleted": len(deleted),
        "compacted": compact,
        "retention_days": retention_days,
        "snapshots_remaining": remaining,
    }
