"""Regression test for the external bundle's load-bearing read contract (B2/B3).

The whole external artifact rests on one mechanism (spec §3 key risk): a
**read-only, file-based** DuckLake catalog whose stored ``data_path`` is an
absolute URL can be ``ATTACH``ed and queried by an **anonymous, credential-free**
client over plain HTTP range requests. ``publish_bundle.build_file_catalog``
produces exactly such a catalog (it rewrites ``data_path`` to the bundle's HTTPS
URL after building with a relative path).

This proves the contract offline: build a file catalog, rewrite ``data_path`` to
a localhost HTTP URL, serve the bundle, and attach it from a fresh DuckDB with
NO credentials. If DuckLake ever stops resolving an absolute ``data_path`` over
HTTP range reads, the frozen artifact breaks and this test fails.

No R2, no cdsci-lake, no credentials — a local HTTP file server stands in for
the Cloudflare Worker (which is just an HTTPS range server; §2 deep module).
"""

import functools
import http.server
import os
import socketserver
import threading

import duckdb
import pytest


def _skip_if_no_ducklake(con):
    try:
        con.execute("INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;")
    except duckdb.Error as e:  # pragma: no cover - offline env only
        pytest.skip(f"ducklake/httpfs unavailable offline: {e}")


def test_anonymous_http_file_catalog_attach(tmp_path):
    """A file catalog with an absolute-HTTP data_path attaches with no creds."""
    root = tmp_path
    bundle = root / "v2026-01-01"
    bundle.mkdir()

    # 1) Build the file catalog with a relative data_path (real parquet files,
    #    inlining disabled) — the build-time shape of build_file_catalog.
    cwd = os.getcwd()
    os.chdir(bundle)
    try:
        con = duckdb.connect()
        _skip_if_no_ducklake(con)
        con.execute(
            "ATTACH 'ducklake:catalog.ducklake' AS pub "
            "(DATA_PATH 'data/', DATA_INLINING_ROW_LIMIT 0)"
        )
        con.execute(
            'CREATE TABLE pub."geo_platforms" AS '
            "SELECT range AS id, ('GPL' || range) AS accession FROM range(500)"
        )
        con.execute("USE memory")
        con.execute("DETACH pub")
        con.close()
    finally:
        os.chdir(cwd)

    # 2) Serve the bundle root over HTTP (range-capable), then rewrite the
    #    stored data_path to the absolute served URL — what the publisher does
    #    with the HTTPS base.
    handler = functools.partial(
        http.server.SimpleHTTPRequestHandler, directory=str(root)
    )
    httpd = socketserver.TCPServer(("127.0.0.1", 0), handler)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}/v2026-01-01"
    try:
        meta = duckdb.connect(str(bundle / "catalog.ducklake"))
        meta.execute(
            "UPDATE ducklake_metadata SET value = ? WHERE key = 'data_path'",
            [f"{base}/data/"],
        )
        meta.close()

        # 3) Anonymous client: fresh connection, NO secrets, read-only attach.
        client = duckdb.connect()
        _skip_if_no_ducklake(client)
        client.execute(
            f"ATTACH 'ducklake:{base}/catalog.ducklake' AS omicidx (READ_ONLY)"
        )
        # count(*) alone reads catalog stats; a filtered scan forces data-file
        # range reads over HTTP — the real proof.
        total = client.execute(
            'SELECT count(*) FROM omicidx."geo_platforms"'
        ).fetchone()[0]
        filtered = client.execute(
            'SELECT count(*) FROM omicidx."geo_platforms" WHERE id >= 250'
        ).fetchone()[0]
        assert total == 500
        assert filtered == 250
    finally:
        httpd.shutdown()
