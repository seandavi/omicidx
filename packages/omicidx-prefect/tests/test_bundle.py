"""Regression test for the external bundle's read contract (ADR-0004 β, #203).

The bundle's ``omicidx.duckdb`` is nothing but views over the public Parquet by
absolute URL, so an **anonymous, credential-free** client can open it and query
base tables and marts over plain HTTP range requests. ``build_bundle`` builds
that file (through ``cdsci.publish``) beside the Parquet it points at.

This proves the contract offline: build the bundle from a throwaway in-memory
``lake``, serve the folder over local HTTP (standing in for the Cloudflare
Worker, which is just an HTTPS range server), and query it from a fresh DuckDB
with NO credentials. No R2, no lake, no credentials. It also pins omicidx's
public layout (base tables at the top level, marts under ``sradb/`` and
``geometadb/``) and the manifest's two field sets (shared + omicidx).
"""

import functools
import http.server
import json
import socketserver
import threading
from types import SimpleNamespace

import duckdb
import pytest
from cdsci.publish import manifest_schema
from omicidx.prefect.flows import publish_bundle
from omicidx.prefect.flows.publish_bundle import (
    DB_FILE,
    build_bundle,
    bundle_path,
    bundle_tables,
)


def _fake_lake(rows: int) -> duckdb.DuckDBPyConnection:
    """Every published source table, ``rows`` rows each, under an attached ``lake``."""
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS lake")
    for _schema, _table, sql in bundle_tables():
        source = sql.removeprefix("SELECT * FROM ")
        _, schema, table = source.split(".")
        con.execute(f"CREATE SCHEMA IF NOT EXISTS lake.{schema}")
        con.execute(
            f"CREATE TABLE lake.{schema}.{table} AS SELECT range AS id FROM range({rows})"
        )
    return con


@pytest.fixture
def served(tmp_path, monkeypatch):
    """``tmp_path`` over local HTTP as PUBLIC_PARQUET_HTTPS_BASE; yields the base URL."""
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(tmp_path)
    )
    httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    monkeypatch.setattr(
        publish_bundle,
        "settings",
        lambda: SimpleNamespace(public_parquet_https_base=base),
    )
    monkeypatch.setattr(publish_bundle, "_raw_partition_counts", lambda: {"geo": 2})
    yield base
    httpd.shutdown()


def test_bundle_layout_manifest_and_anonymous_read(tmp_path, served):
    folder = tmp_path / "latest"
    manifest = build_bundle(folder, _fake_lake(500))

    # Public layout: base tables at the top level, marts under <schema>/.
    assert (folder / "sra_runs.parquet").exists()
    assert (folder / "sradb" / "run.parquet").exists()
    assert not (folder / "main").exists()
    assert {t["path"] for t in manifest["tables"]} == {
        bundle_path(s, t) for s, t, _ in bundle_tables()
    }
    views = (folder / "views.sql").read_text()
    assert f"{served}/latest/sra_runs.parquet" in views
    assert f"{served}/latest/sradb/run.parquet" in views
    assert (
        f"{served}/latest/geometadb/gse.parquet" in (folder / "restore.sql").read_text()
    )

    # manifest.json: the shared contract plus omicidx's own fields.
    written = json.loads((folder / "manifest.json").read_text())
    assert written == manifest
    assert set(manifest_schema()["required"]) <= set(written)
    assert written["publish_date"] == written["release"]
    assert written["dated_path"] == f"v{written['release']}/"
    assert written["transform_sha"] == written["git_sha"]
    assert written["raw_partition_counts"] == {"geo": 2}
    assert "lake_snapshot_id" in written
    assert all(t["row_count"] == 500 for t in written["tables"])
    assert all(
        {"schema", "table", "row_count", "path", "sha256", "columns"} <= set(t)
        for t in written["tables"]
    )

    # Anonymous client: fresh connection, NO secrets, read-only open.
    client = duckdb.connect(str(folder / DB_FILE), read_only=True)
    try:
        client.execute("LOAD httpfs")
        assert client.execute("SELECT count(*) FROM sra_runs").fetchone()[0] == 500
        assert (
            client.execute("SELECT count(*) FROM sradb.run WHERE id >= 250").fetchone()[
                0
            ]
            == 250
        )
    except duckdb.Error as e:  # pragma: no cover - offline env only
        pytest.skip(f"httpfs unavailable offline: {e}")
    finally:
        client.close()

    # restore.sql materializes a local copy and checks it against manifest.json.
    local = duckdb.connect(str(tmp_path / "restored.duckdb"))
    local.execute((folder / "restore.sql").read_text())
    assert local.execute("SELECT count(*) FROM sradb.run").fetchone()[0] == 500
    assert local.execute(
        "SELECT DISTINCT table_type FROM information_schema.tables"
    ).fetchall() == [("BASE TABLE",)]
    local.close()


def test_pubmed_articles_is_not_published():
    """PubMed left the public release (#236); GEO/SRA keep their PMID columns."""
    assert all("pubmed" not in table for _, table, _ in bundle_tables())


def test_manifest_does_not_count_pubmed_raw():
    assert "pubmed" not in publish_bundle.RAW_NAMESPACES


def test_retention_is_seven_dailies_and_five_weeklies():
    """ADR-0007 (#258): the settings defaults the upload's keep_daily_weekly gets."""
    from omicidx.prefect.config import Settings

    assert Settings.model_fields["public_bundle_keep_daily"].default == 7
    assert Settings.model_fields["public_bundle_keep_weekly"].default == 5
