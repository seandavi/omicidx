"""DuckDB build flow (thin).

Builds omicidx.duckdb as nothing but views over every public Parquet file that
`parquet-export` wrote — base tables (`sra_runs`, `geo_series`, ...) in `main`,
marts under the published `sradb.*`/`geometadb.*` schema names — no
re-derivation. The marts are computed upstream by the SQLMesh transform
(flows/transform.py). The co-published views.sql carries concrete HTTPS URLs so
external DuckDB users can `.read` it directly against the public Parquet.
"""

import json
import logging
import shutil
from datetime import datetime
from pathlib import Path

from omicidx.prefect.config import get_duckdb_connection, get_upath, settings
from omicidx.prefect.flows.parquet_export import EXPORTS, MART_EXPORTS
from omicidx.prefect.run import retry

log = logging.getLogger(__name__)

DB_FILE = "omicidx.duckdb"
MART_SCHEMAS = sorted({schema for schema, _ in MART_EXPORTS})
# (schema, view, parquet file relative to the folder) for every public file.
PUBLIC_VIEWS: list[tuple[str, str, str]] = [
    ("main", filename.removesuffix(".parquet"), filename) for _, filename in EXPORTS
] + [(schema, table, f"{schema}/{table}.parquet") for schema, table in MART_EXPORTS]


def _folder_url(folder: str) -> str:
    """Public HTTPS URL of a bundle folder (``latest`` or ``v{date}``)."""
    base = settings().public_parquet_https_base
    if not base:
        raise RuntimeError("PUBLIC_PARQUET_HTTPS_BASE is not set")
    return f"{base.rstrip('/')}/{folder}"


def _view_statements(folder: str) -> list[str]:
    """CREATE SCHEMA + CREATE VIEW per public Parquet file under ``folder``.

    ``folder`` is ``latest`` or ``v{date}``: a dated bundle's views pin that
    folder's files, so it stays readable after ``latest/`` moves on.
    """
    base = _folder_url(folder)
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in MART_SCHEMAS]
    for schema, view, file in PUBLIC_VIEWS:
        stmts.append(
            f"CREATE OR REPLACE VIEW {schema}.{view} AS "
            f"SELECT * FROM read_parquet('{base}/{file}')"
        )
    return stmts


def views_sql(folder: str = "latest") -> str:
    """The co-published views.sql: thin CREATE VIEW over the public Parquet.

    This is the exact SQL external DuckDB users `.read` against the public
    Parquet — so it carries concrete HTTPS URLs (`_view_statements`).
    """
    return ";\n".join(_view_statements(folder)) + ";\n"


def restore_sql(folder: str = "latest") -> str:
    """The co-published restore.sql: a materialized local copy (#225).

    ``duckdb omicidx.duckdb < restore.sql`` copies every public table into a
    local database file (same schemas and names as views.sql), then fails
    unless each table's row count matches the folder's manifest.json.
    """
    base = _folder_url(folder)
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in MART_SCHEMAS]
    for schema, table, file in PUBLIC_VIEWS:
        stmts.append(
            f"CREATE OR REPLACE TABLE {schema}.{table} AS "
            f"SELECT * FROM read_parquet('{base}/{file}')"
        )
    counts = "\n    UNION ALL ".join(
        f"SELECT '{schema}' AS schema, '{table}' AS \"table\", count(*) AS n "
        f"FROM {schema}.{table}"
        for schema, table, _ in PUBLIC_VIEWS
    )
    stmts.append(
        "SELECT CASE WHEN count(*) = 0 THEN 'row counts match manifest.json'\n"
        "  ELSE error('row counts differ from manifest.json: ' || "
        "string_agg(r.schema || '.' || r.\"table\", ', ')) END AS restore_check\n"
        f"FROM (\n    {counts}\n) r\n"
        "LEFT JOIN (SELECT unnest(tables, recursive := true) "
        f"FROM read_json('{base}/manifest.json')) m\n"
        '  ON r.schema = m.schema AND r."table" = m."table"\n'
        "WHERE m.row_count IS DISTINCT FROM r.n"
    )
    return ";\n".join(stmts) + ";\n"


def build_duckdb_local(db_file: str = DB_FILE, folder: str = "latest") -> dict:
    """Build omicidx.duckdb locally from the public Parquet in ``folder``; no upload.

    Returns the local db path, per-table summaries, and the views.sql text.
    Callers own the upload target (the bundle publisher uploads it into the
    bundle).
    """
    db_path = Path(db_file)
    if db_path.exists():
        db_path.unlink()

    summaries: list[dict] = []
    con = get_duckdb_connection(database=db_file)
    try:
        stmts = _view_statements(folder)
        log.info(f"Building {db_file} from {len(PUBLIC_VIEWS)} thin views")
        for stmt in stmts:
            con.execute(stmt)

        con.execute("DROP TABLE IF EXISTS db_creation_metadata")
        con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

        for schema, table, _ in PUBLIC_VIEWS:
            try:
                count = con.execute(
                    f"SELECT COUNT(*) FROM {schema}.{table}"
                ).fetchone()[0]
            except Exception:
                count = -1
            summaries.append({"schema": schema, "table": table, "row_count": count})
    finally:
        con.close()

    for s in summaries:
        log.info(f"  {s['schema']}.{s['table']}: {s['row_count']:,} rows")

    return {
        "db_path": db_path,
        "summaries": summaries,
        "views_sql": views_sql(folder),
        "total_rows": sum(s["row_count"] for s in summaries if s["row_count"] > 0),
    }


@retry
def build_omicidx_duckdb() -> dict:
    """Build the omicidx.duckdb file and upload it to R2/S3."""
    built = build_duckdb_local()
    db_path: Path = built["db_path"]

    metadata = {
        "created_at": datetime.now().isoformat(),
        "tables": built["summaries"],
    }
    db_path.with_suffix(".metadata.json").write_text(json.dumps(metadata, indent=2))

    s3_path = get_upath("duckdb", "omicidx.duckdb")
    log.info(f"Uploading {db_path} to {s3_path}")
    with db_path.open("rb") as src, s3_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    return {
        "table_count": len(built["summaries"]),
        "total_rows": built["total_rows"],
        "s3_path": str(s3_path),
    }


if __name__ == "__main__":
    build_omicidx_duckdb()
