"""DuckDB build flow.

Builds the omicidx.duckdb file from the public Parquet snapshot produced by
`parquet-export` (020-050 SQL views over `latest/*.parquet`) and uploads it
to R2/S3. The view base URL is templated from PUBLIC_PARQUET_HTTPS_BASE.
"""

import json
import shutil
from datetime import datetime
from pathlib import Path

import duckdb
import sqlglot
from omicidx.prefect.config import get_duckdb_connection, get_upath, settings

from prefect import flow, get_run_logger, task

SQL_DIR = Path(__file__).parent.parent / "sql"
DB_FILE = "omicidx.duckdb"
# Placeholder in the view SQL for the public Parquet HTTPS base, so the
# published views.sql carries a concrete URL that agrees with the export
# write path (PUBLIC_PARQUET_ROOT). See sql/020_base_parquet_views.sql.
PUBLIC_PARQUET_BASE_PLACEHOLDER = "{{PUBLIC_PARQUET_BASE}}"


def _list_sql_files() -> list[str]:
    return sorted(p.name for p in SQL_DIR.glob("*.sql"))


def _get_sql(name: str) -> str:
    path = SQL_DIR / name
    if not path.exists():
        available = ", ".join(_list_sql_files())
        raise FileNotFoundError(f"SQL file '{name}' not found. Available: {available}")
    sql = path.read_text()
    if PUBLIC_PARQUET_BASE_PLACEHOLDER in sql:
        base = settings().public_parquet_https_base
        if not base:
            raise RuntimeError(
                "PUBLIC_PARQUET_HTTPS_BASE is not set but "
                f"{name} references {PUBLIC_PARQUET_BASE_PLACEHOLDER}"
            )
        sql = sql.replace(PUBLIC_PARQUET_BASE_PLACEHOLDER, base.rstrip("/"))
    return sql


def _run_sql_file(name: str, con: duckdb.DuckDBPyConnection) -> None:
    log = get_run_logger()
    sql = _get_sql(name)
    statements = sqlglot.transpile(sql, read="duckdb")
    log.info(f"Running {name} ({len(statements)} statements)")
    for i, stmt in enumerate(statements, 1):
        preview = stmt[:100].replace("\n", " ")
        log.info(f"  [{i}/{len(statements)}] {preview}...")
        con.execute(stmt)
    log.info(f"Completed {name}")


def _view_files() -> list[str]:
    """The 020+ view SQL files that define the published omicidx.duckdb."""
    return [f for f in _list_sql_files() if f >= "020"]


def views_sql() -> str:
    """The co-published views.sql: 020+ view definitions, base URL substituted.

    This is the exact SQL external DuckDB users `.read` against the public
    Parquet — so it must carry concrete HTTPS URLs, which `_get_sql` supplies.
    """
    return "\n\n".join(_get_sql(name) for name in _view_files())


def build_duckdb_local(db_file: str = DB_FILE) -> dict:
    """Build omicidx.duckdb locally from the public Parquet; no upload.

    Returns the local db path, per-table summaries, and the views.sql text.
    Callers own the upload target (the daily flow uploads to PUBLISH_ROOT;
    the bundle publisher uploads into the frozen bundle).
    """
    log = get_run_logger()
    db_path = Path(db_file)
    if db_path.exists():
        db_path.unlink()

    summaries: list[dict] = []
    con = get_duckdb_connection(database=db_file)
    try:
        vfiles = _view_files()
        log.info(f"Building {db_file} from {len(vfiles)} SQL files")
        for name in vfiles:
            _run_sql_file(name, con)

        con.execute("DROP TABLE IF EXISTS db_creation_metadata")
        con.execute("CREATE TABLE db_creation_metadata AS SELECT now() AS created_at")

        for schema in ["main", "geometadb", "sradb"]:
            try:
                tables = con.execute(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{schema}'"
                ).fetchall()
            except Exception:
                continue
            for (table,) in tables:
                qualified = f"{schema}.{table}" if schema != "main" else table
                try:
                    count = con.execute(
                        f"SELECT COUNT(*) FROM {qualified}"
                    ).fetchone()[0]
                except Exception:
                    count = -1
                summaries.append({"schema": schema, "table": table, "row_count": count})
    finally:
        con.close()

    for s in summaries:
        qualified = f"{s['schema']}.{s['table']}" if s["schema"] != "main" else s["table"]
        log.info(f"  {qualified}: {s['row_count']:,} rows")

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
        "sql_files": ", ".join(_list_sql_files()),
        "table_count": len(built["summaries"]),
        "total_rows": built["total_rows"],
        "s3_path": str(s3_path),
    }


@flow(name="omicidx-duckdb-build")
def omicidx_duckdb_flow() -> None:
    build_omicidx_duckdb()


if __name__ == "__main__":
    omicidx_duckdb_flow()
