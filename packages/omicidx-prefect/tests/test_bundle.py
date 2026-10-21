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
import socketserver
import threading
from types import SimpleNamespace

import duckdb
import pytest
from omicidx.prefect.flows import sql


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
