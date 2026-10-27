"""Bundle publisher (spec §2, Stage B; ADR-0004 β): the external daily bundle.

Deep module, narrow interface: ``publish_bundle(date) -> bundle at v{date}/
and latest/``. Hides the omicidx.duckdb build, views.sql co-publish, manifest
emission, and the dated/rolling layout.

Bundle contents, next to the flat ``*.parquet`` that ``parquet-export`` wrote:
- ``omicidx.duckdb`` — views over every public Parquet file in the same folder
  (base tables + ``sradb``/``geometadb`` marts), by absolute HTTPS URL.
- ``views.sql`` — the same view definitions as plain SQL (ADR-0004).
- ``restore.sql`` — builds a materialized local copy of every table, then
  checks row counts against ``manifest.json`` (#225).
- ``manifest.json`` — provenance attestation (spec §2a): raw partition set,
  transform SHA, internal lake snapshot id, publish timestamp, dated path.

The dated folder's views point at ``v{date}/``, ``latest/``'s at ``latest/``,
so a pinned dated bundle keeps reading its own Parquet. The lake is not touched
except to read the snapshot id for the manifest. No frozen DuckLake catalog
ships (#201/#203): nobody ATTACHed it, and its ``data/`` copy doubled the bundle.
"""

import json
import logging
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from omicidx.prefect.config import (
    get_ducklake_connection,
    get_public_upath,
    publish_date,
)
from omicidx.prefect.flows.sql import DB_FILE, build_duckdb_local, restore_sql
from omicidx.prefect.run import retry
from omicidx.prefect.semaphore import SemaphoreStore

log = logging.getLogger(__name__)

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


@retry
def _put_file(local: Path, *parts: str) -> None:
    """Upload one file to its exact key; retried, since that is idempotent (#195)."""
    # fs.put_file chunks the local file into uniform multipart parts; a
    # buffered ``.open("wb").write(all_bytes)`` produces unequal parts that
    # R2 rejects ("All non-trailing parts must have the same length").
    dst = get_public_upath(*parts)
    dst.fs.put_file(str(local), dst.path)


def publish_bundle(date: str | None = None) -> dict:
    """Publish the external bundle for ``date`` (default: today UTC).

    Depends on ``parquet-export`` having written v{date}/ and latest/ Parquet.
    """
    date = date or publish_date()
    manifest = None
    with tempfile.TemporaryDirectory(prefix="omicidx-bundle-") as tmp:
        for folder in (f"v{date}", "latest"):
            workdir = Path(tmp) / folder
            workdir.mkdir()
            built = build_duckdb_local(str(workdir / DB_FILE), folder=folder)
            # One manifest for both folders, from the dated build's row counts.
            manifest = manifest or build_manifest(date, built["summaries"])
            (workdir / "manifest.json").write_text(json.dumps(manifest, indent=2))
            (workdir / "views.sql").write_text(built["views_sql"])
            (workdir / "restore.sql").write_text(restore_sql(folder))
            # manifest.json last: it marks the folder complete (hf_release).
            for name in (DB_FILE, "views.sql", "restore.sql", "manifest.json"):
                _put_file(workdir / name, folder, name)
            log.info(f"Published {DB_FILE}, SQL files, manifest.json → {folder}/")

    return {"date": date, "manifest": manifest}


if __name__ == "__main__":
    publish_bundle()
