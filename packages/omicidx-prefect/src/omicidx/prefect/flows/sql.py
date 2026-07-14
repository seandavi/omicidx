"""DuckDB build flow (thin).

Builds omicidx.duckdb as nothing but views over the public mart Parquet that
`parquet-export` wrote (`latest/{sradb,geometadb}/*.parquet`) — no re-derivation.
The marts are computed upstream by the SQLMesh transform (flows/transform.py);
this step only exposes them behind the published `sradb.*`/`geometadb.*` schema
names. The co-published views.sql carries concrete HTTPS URLs so external DuckDB
users can `.read` it directly against the public Parquet.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

from omicidx.prefect.config import get_duckdb_connection, get_upath, settings
from omicidx.prefect.flows.parquet_export import MART_EXPORTS

from prefect import flow, get_run_logger, task

DB_FILE = "omicidx.duckdb"
MART_SCHEMAS = sorted({schema for schema, _ in MART_EXPORTS})


def _view_statements() -> list[str]:
    """CREATE SCHEMA + CREATE VIEW per mart, over the public `latest/` Parquet."""
    base = settings().public_parquet_https_base
    if not base:
        raise RuntimeError("PUBLIC_PARQUET_HTTPS_BASE is not set")
    base = base.rstrip("/")
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s}" for s in MART_SCHEMAS]
    for schema, table in MART_EXPORTS:
        url = f"{base}/latest/{schema}/{table}.parquet"
        stmts.append(
            f"CREATE OR REPLACE VIEW {schema}.{table} AS "
            f"SELECT * FROM read_parquet('{url}')"
        )
    return stmts


def views_sql() -> str:
    """The co-published views.sql: thin CREATE VIEW over the public mart Parquet.

    This is the exact SQL external DuckDB users `.read` against the public
    Parquet — so it carries concrete HTTPS URLs (`_view_statements`).
    """
    return ";\n".join(_view_statements()) + ";\n"


def build_duckdb_local(db_file: str = DB_FILE) -> dict:
    """Build omicidx.duckdb locally from the public mart Parquet; no upload.

    Returns the local db path, per-table summaries, and the views.sql text.
    Callers own the upload target (the bundle publisher uploads it into the
    frozen bundle).
    """
    log = get_run_logger()
    db_path = Path(db_file)
    if db_path.exists():
        db_path.unlink()

    summaries: list[dict] = []
    con = get_duckdb_connection(database=db_file)
    try:
        stmts = _view_statements()
        log.info(f"Building {db_file} from {len(stmts)} thin mart views")
        for stmt in stmts:
            con.execute(stmt)

        con.execute("DROP TABLE IF EXISTS db_creation_metadata")
        con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

        for schema in MART_SCHEMAS:
            tables = con.execute(
                f"SELECT table_name FROM information_schema.tables "
                f"WHERE table_schema = '{schema}'"
            ).fetchall()
            for (table,) in tables:
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
        "views_sql": views_sql(),
        "total_rows": sum(s["row_count"] for s in summaries if s["row_count"] > 0),
    }


@task(retries=1, retry_delay_seconds=60)
def build_omicidx_duckdb() -> dict:
    """Build the omicidx.duckdb file and upload it to R2/S3."""
    log = get_run_logger()
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


@flow(name="omicidx-duckdb-build")
def omicidx_duckdb_flow() -> None:
    build_omicidx_duckdb()


if __name__ == "__main__":
    omicidx_duckdb_flow()
