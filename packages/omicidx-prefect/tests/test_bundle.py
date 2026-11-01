"""Regression test for the external bundle's read contract (ADR-0004 β, #203).

The bundle's ``omicidx.duckdb`` is nothing but views over the public Parquet by
absolute URL, so an **anonymous, credential-free** client can open it and query
base tables and marts over plain HTTP range requests.
``sql.build_duckdb_local`` builds exactly that file.

This proves the contract offline: write one Parquet per public file, serve the
folder over local HTTP (standing in for the Cloudflare Worker, which is just an
HTTPS range server), build the duckdb against it, and query it from a fresh
DuckDB with NO credentials. No R2, no lake, no credentials.
"""

import functools
import http.server
import json
import socketserver
import threading
from types import SimpleNamespace

import duckdb
import pytest
from omicidx.prefect.flows import publish_bundle, sql


def test_views_duckdb_reads_public_parquet_anonymously(tmp_path, monkeypatch):
    folder = tmp_path / "v2026-01-01"
    for schema in sql.MART_SCHEMAS:
        (folder / schema).mkdir(parents=True)
    con = duckdb.connect()
    for _schema, _view, file in sql.PUBLIC_VIEWS:
        con.execute(
            f"COPY (SELECT range AS id FROM range(500)) TO '{folder / file}' (FORMAT PARQUET)"
        )
    con.close()

    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(tmp_path)
    )
    httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{httpd.server_address[1]}"
    monkeypatch.setattr(
        sql, "settings", lambda: SimpleNamespace(public_parquet_https_base=base)
    )

    # Builder side: plain DuckDB stands in for the R2-secret connection.
    def _connect(database):
        c = duckdb.connect(database)
        c.execute("INSTALL httpfs; LOAD httpfs;")
        return c

    monkeypatch.setattr(sql, "get_duckdb_connection", _connect)
    try:
        try:
            built = sql.build_duckdb_local(
                str(tmp_path / sql.DB_FILE), folder="v2026-01-01"
            )
        except duckdb.Error as e:  # pragma: no cover - offline env only
            pytest.skip(f"httpfs unavailable offline: {e}")
        assert {s["row_count"] for s in built["summaries"]} == {500}
        assert len(built["summaries"]) == len(sql.PUBLIC_VIEWS)
        assert f"{base}/v2026-01-01/sra_runs.parquet" in built["views_sql"]

        # Data dictionary (#130): one entry per public file, columns + source.
        entries = {(d["schema"], d["table"]): d for d in built["dictionary"]}
        assert len(entries) == len(sql.PUBLIC_VIEWS)
        assert entries[("sradb", "run")]["columns"] == [
            {"name": "id", "type": "BIGINT"}
        ]
        assert entries[("sradb", "run")]["source"] == "lake.sradb.run"
        assert entries[("main", "sra_runs")]["source"] == "lake.omicidx.sra_run"
        assert entries[("main", "geo_samples")]["source"] == "lake.stg.geo_samples"

        # Anonymous client: fresh connection, NO secrets, read-only open.
        client = duckdb.connect(str(built["db_path"]), read_only=True)
        client.execute("LOAD httpfs")
        assert client.execute("SELECT count(*) FROM sra_runs").fetchone()[0] == 500
        assert (
            client.execute("SELECT count(*) FROM sradb.run WHERE id >= 250").fetchone()[
                0
            ]
            == 250
        )
    finally:
        httpd.shutdown()


def test_restore_sql_materializes_and_checks_manifest(tmp_path, monkeypatch):
    """restore.sql copies every table locally and fails on a row-count mismatch."""
    folder = tmp_path / "latest"
    for schema in sql.MART_SCHEMAS:
        (folder / schema).mkdir(parents=True)
    con = duckdb.connect()
    for _schema, _view, file in sql.PUBLIC_VIEWS:
        con.execute(
            f"COPY (SELECT range AS id FROM range(3)) TO '{folder / file}' (FORMAT PARQUET)"
        )
    con.close()
    tables = [{"schema": s, "table": t, "row_count": 3} for s, t, _ in sql.PUBLIC_VIEWS]
    manifest = folder / "manifest.json"
    manifest.write_text(json.dumps({"tables": tables}))
    # A local folder path stands in for the HTTPS base; the SQL is the same.
    monkeypatch.setattr(
        sql,
        "settings",
        lambda: SimpleNamespace(public_parquet_https_base=str(tmp_path)),
    )
    restore = sql.restore_sql("latest")

    local = duckdb.connect(str(tmp_path / "restored.duckdb"))
    local.execute(restore)
    assert local.execute("SELECT count(*) FROM sradb.run").fetchone()[0] == 3
    kinds = local.execute(
        "SELECT DISTINCT table_type FROM information_schema.tables"
    ).fetchall()
    assert kinds == [("BASE TABLE",)]

    tables[0]["row_count"] = 4
    manifest.write_text(json.dumps({"tables": tables}))
    with pytest.raises(
        duckdb.Error, match="differ from manifest.json: main.bioprojects"
    ):
        local.execute(restore)


def test_pubmed_articles_is_not_published():
    """PubMed left the public release (#236); GEO/SRA keep their PMID columns."""
    assert all("pubmed" not in file for _, _, file in sql.PUBLIC_VIEWS)


def test_manifest_does_not_count_pubmed_raw():
    assert "pubmed" not in publish_bundle.RAW_NAMESPACES
