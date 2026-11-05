"""Steel-thread test for the reverse-ETL export seam (ADR-0004, spec §2).

The publish path had no end-to-end guard: existing tests cover the write
primitive (test_idempotency, real ``upsert``), the mart SQL over *empty*
upstreams (test_marts_schema), and the anonymous read contract (test_bundle) —
but nothing proved that rows loaded into the lake actually survive the bundle
build into public Parquet with their count and schema intact. A silent
breakage there (wrong table name, a COPY that drops rows) would publish an
empty or malformed artifact and no unit test would fail.

This runs the REAL ``build_bundle`` (``cdsci.publish.publish`` over omicidx's
table list) against a LOCAL file-catalog DuckLake loaded via the REAL
``cdsci.lake.connect.upsert`` — no R2, no cdsci-lake catalog, no credentials.
"""

from types import SimpleNamespace

import duckdb
import pytest
from cdsci.lake.connect import upsert
from omicidx.prefect.flows import publish_bundle


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


# Three rows, keyed like every omicidx upsert target. SELECT * in the bundle
# build is schema-agnostic, so a minimal realistic shape is enough to prove
# fidelity.
_FIXTURE = (
    "SELECT * FROM (VALUES "
    "('PRJNA1','Affymetrix'),('PRJNA2','Illumina'),('PRJNA3','Nanopore')"
    ") AS t(accession, title)"
)


def test_loaded_rows_survive_export_to_parquet(tmp_path, monkeypatch):
    con = _local_lake(tmp_path)
    upsert(con, "lake.omicidx.bioproject", _FIXTURE, key="accession")

    monkeypatch.setattr(
        publish_bundle,
        "settings",
        lambda: SimpleNamespace(public_parquet_https_base="https://example.test/data"),
    )
    monkeypatch.setattr(publish_bundle, "_raw_partition_counts", dict)
    out = tmp_path / "bundle"
    manifest = publish_bundle.build_bundle(out, con, only={"main.bioprojects"})

    (table,) = manifest["tables"]
    assert (table["schema"], table["table"], table["path"]) == (
        "main",
        "bioprojects",
        "bioprojects.parquet",
    )
    assert table["row_count"] == 3
    assert [(c["name"], c["type"]) for c in table["columns"]] == [
        ("accession", "VARCHAR"),
        ("title", "VARCHAR"),
    ]
    # The lake snapshot the bundle was cut from is recorded (a real DuckLake here).
    assert manifest["lake_snapshot_id"] >= 1

    rows = con.execute(
        f"SELECT accession, title FROM read_parquet('{out / 'bioprojects.parquet'}') "
        "ORDER BY accession"
    ).fetchall()
    assert rows == [
        ("PRJNA1", "Affymetrix"),
        ("PRJNA2", "Illumina"),
        ("PRJNA3", "Nanopore"),
    ], "bioprojects.parquet lost or mangled rows through export"
