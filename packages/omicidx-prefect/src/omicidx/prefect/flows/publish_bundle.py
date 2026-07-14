"""Frozen publisher (spec §2, Stage B2/B4): the external daily bundle.

Deep module, narrow interface: ``publish_bundle_flow(date) -> attested bundle
at v{date}/ and latest/``. Hides which tables ship, the read-only file-catalog
generation, the omicidx.duckdb build, views.sql co-publish, manifest emission,
and the dated/rolling layout.

Bundle contents (per spec §2):
- ``catalog.ducklake`` — a **read-only, file-based** DuckLake catalog (a plain
  DuckDB file, not Postgres) whose stored ``data_path`` is the absolute HTTPS
  URL of ``v{date}/data/``. Anonymous, credential-free clients:

      ATTACH 'ducklake:https://<base>/latest/catalog.ducklake' AS omicidx (READ_ONLY);
      SELECT * FROM omicidx.geo_platforms LIMIT 5;

  Building current-state only at v1, but on the retaining file-catalog format
  from day one (B′ opens retention without a format migration).
- ``data/`` — DuckLake-managed Parquet data files backing the catalog.
- ``omicidx.duckdb`` — built from the public Parquet (marts included).
- ``views.sql`` — co-published view definitions (ADR-0004).
- ``manifest.json`` — provenance attestation (spec §2a): raw partition set,
  transform SHA, internal lake snapshot id, publish timestamp, dated path.

Runs after ``parquet-export`` (which wrote v{date}/*.parquet + latest/*.parquet)
and reads those flat Parquet files as its source — the lake is not touched
except to read the snapshot id for the manifest.

ponytail: the file catalog's ``data/`` re-encodes the same rows already in the
flat ``*.parquet`` (≈2× the current snapshot in the bundle). Fine at v1's
bounded window; the dedup upgrade is ``ducklake_add_data_files`` registering the
flat Parquet as the catalog's data files — add when bundle storage/egress hurts.
"""

import json
import os
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from omicidx.prefect.config import (
    get_duckdb_connection,
    get_ducklake_connection,
    get_public_parquet_path,
    get_public_upath,
    publish_date,
    settings,
)
from omicidx.prefect.flows.parquet_export import EXPORTS
from omicidx.prefect.flows.sql import build_duckdb_local
from omicidx.prefect.semaphore import SemaphoreStore

from prefect import flow, get_run_logger, task

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
    "pubmed",
]


def _public_table_name(filename: str) -> str:
    """Public catalog table name = the flat Parquet basename (plural form)."""
    return filename.removesuffix(".parquet")


def _git_sha() -> str | None:
    """Transform version = the repo git SHA (spec §2a). None outside a checkout."""
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent,
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except Exception:
        return None


def _lake_snapshot_id() -> int | None:
    """Current internal-lake snapshot id — the id this publish was cut from."""
    try:
        with get_ducklake_connection() as con:
            row = con.execute(
                "SELECT max(snapshot_id) FROM lake.snapshots()"
            ).fetchone()
        return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def build_file_catalog(date: str, tables: list[tuple[str, str]], workdir: Path) -> None:
    """Build catalog.ducklake + data/ under ``workdir`` from v{date}/ Parquet.

    Data files are written under ``workdir/data/`` (relative ``DATA_PATH``);
    afterwards the stored ``data_path`` is rewritten to the absolute HTTPS URL
    of ``v{date}/data/`` so anonymous clients resolve data files over range
    reads (verified end-to-end; the client supplies no credentials).
    """
    log = get_run_logger()
    https_base = settings().public_parquet_https_base
    if not https_base:
        raise RuntimeError("PUBLIC_PARQUET_HTTPS_BASE is not set")

    cwd = os.getcwd()
    os.chdir(workdir)
    try:
        con = get_duckdb_connection()  # httpfs + account-scoped r2 secret
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(
            "ATTACH 'ducklake:catalog.ducklake' AS pub "
            "(DATA_PATH 'data/', DATA_INLINING_ROW_LIMIT 0)"
        )
        for _lake_table, filename in tables:
            src = get_public_parquet_path(f"v{date}", filename)
            name = _public_table_name(filename)
            log.info(f"Catalog table {name} ← {src}")
            con.execute(
                f"CREATE TABLE pub.\"{name}\" AS SELECT * FROM read_parquet('{src}')"
            )
        con.execute("USE memory")
        con.execute("DETACH pub")
        con.close()
    finally:
        os.chdir(cwd)

    data_url = f"{https_base.rstrip('/')}/v{date}/data/"
    meta = duckdb.connect(str(workdir / "catalog.ducklake"))
    meta.execute(
        "UPDATE ducklake_metadata SET value = ? WHERE key = 'data_path'", [data_url]
    )
    meta.close()
    log.info(f"Rewrote catalog data_path → {data_url}")


def build_manifest(date: str, summaries: list[dict]) -> dict:
    """Bundle manifest (spec §2a): pins the publish to its provenance."""
    raw_partitions = {ns: len(SemaphoreStore(ns).list_keys()) for ns in RAW_NAMESPACES}
    return {
        "publish_date": date,
        "published_at": datetime.now(UTC).isoformat(),
        "dated_path": f"v{date}/",
        "transform_sha": _git_sha(),
        "lake_snapshot_id": _lake_snapshot_id(),
        # v1 records partition *counts* per source; full enumeration (needed to
        # reproduce a historical snapshot exactly) is the B′ upgrade.
        "raw_partition_counts": raw_partitions,
        "tables": [
            {
                "schema": s["schema"],
                "table": s["table"],
                "row_count": s["row_count"],
            }
            for s in summaries
        ],
    }


@task
def upload_bundle(
    date: str, workdir: Path, manifest: dict, db_path: Path, views_sql: str
) -> dict:
    """Upload the assembled bundle to v{date}/ and mirror small files to latest/.

    The DuckLake ``data/`` dir lives only under the immutable ``v{date}/``; the
    ``latest/`` catalog references it by absolute URL, so latest carries only
    the small files (catalog, duckdb, views.sql, manifest).
    """
    log = get_run_logger()

    def _put_file(local: Path, *parts: str) -> None:
        # fs.put_file chunks the local file into uniform multipart parts; a
        # buffered ``.open("wb").write(all_bytes)`` produces unequal parts that
        # R2 rejects ("All non-trailing parts must have the same length").
        dst = get_public_upath(*parts)
        dst.fs.put_file(str(local), dst.path)

    # data/ dir — upload each file to its exact key. (fs.put(recursive=True) is
    # ambiguous when the dest prefix already exists — it nests data/data/ on a
    # re-run — so map files explicitly.) Dated folder only.
    local_data = workdir / "data"
    n_data = 0
    for f in sorted(local_data.rglob("*")):
        if f.is_file():
            rel = f.relative_to(local_data).as_posix()
            _put_file(f, f"v{date}", "data", rel)
            n_data += 1
    log.info(f"Uploaded {n_data} data file(s) → v{date}/data/")

    # small files: write to v{date}/ then mirror to latest/
    catalog = workdir / "catalog.ducklake"
    manifest_path = workdir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    views_path = workdir / "views.sql"
    views_path.write_text(views_sql)

    small = [
        (catalog, "catalog.ducklake"),
        (db_path, "omicidx.duckdb"),
        (views_path, "views.sql"),
        (manifest_path, "manifest.json"),
    ]
    for local, name in small:
        _put_file(local, f"v{date}", name)
        _put_file(local, "latest", name)
        log.info(f"Published {name} → v{date}/ + latest/")

    return {"uploaded": [n for _, n in small] + ["data/"]}


@flow(name="publish-bundle")
def publish_bundle_flow(
    date: str | None = None, tables: list[tuple[str, str]] | None = None
) -> dict:
    """Publish the external frozen bundle for ``date`` (default: today UTC).

    Depends on ``parquet-export`` having written v{date}/*.parquet. Pass
    ``tables`` (subset of EXPORTS) to publish a partial catalog for testing.
    """
    log = get_run_logger()
    date = date or publish_date()
    tables = tables or EXPORTS

    built = build_duckdb_local()  # omicidx.duckdb + views.sql from public Parquet
    manifest = build_manifest(date, built["summaries"])

    with tempfile.TemporaryDirectory(prefix="omicidx-bundle-") as tmp:
        workdir = Path(tmp)
        build_file_catalog(date, tables, workdir)
        result = upload_bundle(
            date, workdir, manifest, built["db_path"], built["views_sql"]
        )

    log.info(f"Published bundle v{date}: {result['uploaded']}")
    return {"date": date, "manifest": manifest, **result}


if __name__ == "__main__":
    publish_bundle_flow()
