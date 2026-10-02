"""Offline tests for the omicidx-pubmed dataset release (local DuckLake, no network)."""

import json
from datetime import date, timedelta

import pytest
from cdsci.lake.config import Settings
from cdsci.lake.connect import lake_connect
from cdsci.lake.contracts_render import lint_contract
from cdsci.lake.publish.builder import LocalDirStore
from cdsci.lake.publish.frozen import frozen_ducklake_attach_sql
from cdsci.lake.publish.index import load_index, pin_release
from cdsci.lake.publish.verify import verify_release
from omicidx.prefect.flows.publish_release import (
    PUBMED,
    PUBMED_ARTICLE,
    publish_pubmed_release,
)

AUTHOR = "STRUCT(lastname VARCHAR, forename VARCHAR, initials VARCHAR, identifier VARCHAR, affiliation VARCHAR)"
REF = "STRUCT(citation VARCHAR, pmid VARCHAR)"
GRANT = (
    "STRUCT(grant_id VARCHAR, grant_acronym VARCHAR, country VARCHAR, agency VARCHAR)"
)
NESTED = {"authors": f"{AUTHOR}[]", "references": f"{REF}[]", "grant_ids": f"{GRANT}[]"}


@pytest.fixture
def con(tmp_path):
    c = lake_connect(
        Settings(lake_backend="local", storage_base_uri=f"file://{tmp_path}/lake")
    )
    c.execute("CREATE SCHEMA lake.omicidx")
    cols = ", ".join(
        f'"{col.name}" {NESTED.get(col.name, "VARCHAR")}'
        for col in PUBMED_ARTICLE.columns
    )
    c.execute(f"CREATE TABLE lake.omicidx.pubmed_article ({cols})")
    c.execute(
        """
        INSERT INTO lake.omicidx.pubmed_article (pmid, title, authors, "references", grant_ids)
        VALUES
          ('1', 'First',
           [{'lastname': 'Doe', 'forename': 'Jane', 'initials': 'J', 'identifier': NULL, 'affiliation': 'MIT'}],
           [{'citation': 'c', 'pmid': '9'}], NULL),
          ('2', 'Second', [], NULL, NULL),
          ('3', 'Third',
           [{'lastname': 'Roe', 'forename': 'R', 'initials': 'R', 'identifier': NULL, 'affiliation': NULL}],
           NULL, [{'grant_id': 'G1', 'grant_acronym': 'A', 'country': 'US', 'agency': 'NIH'}])
        """
    )
    yield c
    c.close()


def test_contract_lints_clean():
    assert len(PUBMED_ARTICLE.columns) == 25
    assert lint_contract(PUBMED_ARTICLE) == []


def test_release_verifies_and_frozen_round_trips(con, tmp_path):
    out = tmp_path / "pub"
    manifest = publish_pubmed_release(con, out, today=date(2026, 10, 2))
    assert manifest.release == "2026-10-02"
    assert manifest.tables[0].row_count == 3
    store = LocalDirStore(out)
    report = verify_release(store, "omicidx-pubmed", "2026-10-02", contract=PUBMED)
    assert report.passed
    latest = json.loads((out / "omicidx-pubmed" / "latest.json").read_text())
    assert latest["release"] == "2026-10-02"

    import duckdb

    c = duckdb.connect()
    c.execute("INSTALL ducklake; LOAD ducklake;")
    c.execute(frozen_ducklake_attach_sql(str(out / "omicidx-pubmed" / "2026-10-02")))
    authors = c.sql(
        "SELECT authors FROM published.pubmed_article WHERE pmid = '1'"
    ).fetchone()[0]
    assert authors == [
        {
            "lastname": "Doe",
            "forename": "Jane",
            "initials": "J",
            "identifier": None,
            "affiliation": "MIT",
        }
    ]
    assert (
        c.sql(
            "SELECT authors FROM published.pubmed_article WHERE pmid = '2'"
        ).fetchone()[0]
        == []
    )
    assert c.sql("SELECT count(*) FROM published.pubmed_article").fetchone()[0] == 3
    c.close()


def test_keep_last_prunes_and_pin_survives(con, tmp_path):
    out = tmp_path / "pub"
    start = date(2026, 10, 1)
    days = [start + timedelta(days=i) for i in range(9)]
    for i, d in enumerate(days):
        if i == 7:  # before the 8th call
            pin_release(LocalDirStore(out), "omicidx-pubmed", days[0].isoformat())
        publish_pubmed_release(con, out, today=d)
    index = load_index(LocalDirStore(out), "omicidx-pubmed")
    kept = [r.release for r in index.releases]
    expected = [d.isoformat() for d in days[2:]]
    assert kept == [days[0].isoformat(), *expected]
    dirs = {p.name for p in (out / "omicidx-pubmed").iterdir() if p.is_dir()}
    assert dirs == set(kept)
    assert days[1].isoformat() not in dirs
