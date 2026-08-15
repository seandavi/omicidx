"""Steel-thread test for the reverse-ETL export seam (ADR-0004, spec §2).

The publish path had no end-to-end guard: existing tests cover the write
primitive (test_idempotency, real ``upsert``), the mart SQL over *empty*
upstreams (test_marts_schema), and the anonymous read contract (test_bundle) —
but nothing proved that rows loaded into the lake actually survive the
``parquet-export`` COPY into the public bucket with their count and schema
intact. A silent breakage there (wrong table name, a COPY that drops rows, a
lost ``latest/`` mirror) would publish an empty or malformed artifact and no
unit test would fail.

This runs the REAL ``flows.parquet_export.export_table`` against a LOCAL
file-catalog DuckLake loaded via the REAL ``cdsci.lake.connect.upsert`` — no
R2, no cdsci-lake catalog, no credentials. The config helpers the task reaches
for (live connection + public bucket path) are the only things stubbed, pointed
at the throwaway lake and a tmp dir.
"""

import contextlib
from pathlib import Path

import duckdb
import pytest
from cdsci.lake.connect import upsert
from omicidx.prefect.flows import parquet_export


def _local_lake(tmp_path):
    """Attach a throwaway file-catalog DuckLake with a local data path."""
    data = tmp_path / "lake_data"
    data.mkdir()
    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
    except duckdb.Error as e:  # pragma: no cover - offline env only
        pytest.skip(f"ducklake extension unavailable offline: {e}")
    con.execute(
        f"ATTACH 'ducklake:{tmp_path / 'catalog.ducklake'}' AS lake "
        f"(DATA_PATH '{data}/')"
    )
    return con


# Three rows, keyed like every omicidx upsert target. SELECT * in export_table
# is schema-agnostic, so a minimal realistic shape is enough to prove fidelity.
_FIXTURE = (
    "SELECT * FROM (VALUES "
    "('GPL1','Affymetrix'),('GPL2','Illumina'),('GPL3','Nanopore')"
    ") AS t(accession, title)"
)


def test_loaded_rows_survive_export_to_parquet(tmp_path, monkeypatch):
    con = _local_lake(tmp_path)
    upsert(con, "lake.omicidx.geo_platform", _FIXTURE, key="accession")

    # Point the export task at the local lake + a tmp public root. Yield the
    # same connection without closing it on __exit__ so we can assert after.
    monkeypatch.setattr(
        parquet_export,
        "get_ducklake_connection",
        lambda: contextlib.nullcontext(con),
    )

    def _fake_public_path(*parts: str) -> str:
        p = tmp_path / "public"
        for part in parts:
            p = p / part
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)

    monkeypatch.setattr(parquet_export, "get_public_parquet_path", _fake_public_path)
    result = parquet_export.export_table(
        "geo_platform", "geo_platforms.parquet", "2026-01-01"
    )

    assert result["row_count"] == 3

    dated = Path(_fake_public_path("v2026-01-01", "geo_platforms.parquet"))
    latest = Path(_fake_public_path("latest", "geo_platforms.parquet"))
    assert dated.exists(), "dated snapshot must be written"
    assert latest.exists(), "rolling latest/ mirror must be written"

    # Both files carry the loaded rows and schema — the seam holds end to end.
    for f in (dated, latest):
        rows = con.execute(
            f"SELECT accession, title FROM read_parquet('{f}') ORDER BY accession"
        ).fetchall()
        assert rows == [
            ("GPL1", "Affymetrix"),
            ("GPL2", "Illumina"),
            ("GPL3", "Nanopore"),
        ], f"{f.name} lost or mangled rows through export"
