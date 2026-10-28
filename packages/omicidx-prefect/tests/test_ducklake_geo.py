"""#231: geo_platform gains `taxid BIGINT[]`, appended last, on an existing lake.

`upsert`'s MERGE matches columns by position and never alters a table, so the
loader appends the column itself and the source projects it last. Runs against a
local file-catalog DuckLake, like test_export_steel_thread.py.
"""

import gzip

import orjson
from cdsci.lake.connect import upsert
from omicidx.prefect.flows.ducklake_geo import (
    _GEO_PLATFORM_ADD_TAXID,
    _GEO_PLATFORM_SOURCE,
)


def _write(path, records):
    with gzip.open(path, "wb") as f:
        for rec in records:
            f.write(orjson.dumps(rec) + b"\n")


# Every column `_GEO_PLATFORM_SOURCE` projects except taxid (union_by_name yields
# NULL for a file that lacks a key another file has, but not for a key no file has).
_PLATFORM = {
    "title": "t",
    "status": "Public",
    "submission_date": "2020-01-01",
    "last_update_date": "2020-01-01",
    "contact": {"city": "x"},
    "organism": "Homo sapiens",
    "sample_id": ["GSM1"],
    "series_id": ["GSE1"],
    "technology": "t",
    "description": "d",
    "distribution": "virtual",
    "manufacturer": ["m"],
    "data_row_count": 1,
    "contributor": [{"first": "a", "last": "b"}],
    "relation": ["r"],
    "manufacture_protocol": "p",
}


def test_geo_platform_load_adds_taxid_last(tmp_path):
    # Imported here, not at module top: under `pytest packages/` both packages'
    # `tests` collide, and a sibling import only resolves once that module has
    # been collected (as test_ncbi_taxonomy's import of test_marts_schema does).
    from .test_export_steel_thread import _local_lake

    con = _local_lake(tmp_path)
    _write(tmp_path / "old.ndjson.gz", [{**_PLATFORM, "accession": "GPL1"}])
    _write(
        tmp_path / "new.ndjson.gz",
        [
            {**_PLATFORM, "accession": "GPL2", "taxid": [9606]},
            {**_PLATFORM, "accession": "GPL3", "taxid": []},
        ],
    )
    source = _GEO_PLATFORM_SOURCE.format(path=str(tmp_path / "*.ndjson.gz"))
    target = "lake.omicidx.geo_platform"
    con.execute("CREATE SCHEMA IF NOT EXISTS lake.omicidx")

    # The pre-#231 table: today's projection minus taxid, no rows.
    con.execute(
        f"CREATE TABLE {target} AS SELECT * EXCLUDE (taxid) FROM ({source}) WHERE false"
    )
    add_taxid = _GEO_PLATFORM_ADD_TAXID.format(target=target)
    con.execute(add_taxid)
    con.execute(add_taxid)  # idempotent
    con.execute(  # a missing table is a no-op, not an error
        _GEO_PLATFORM_ADD_TAXID.format(target="lake.omicidx.no_such_table")
    )

    upsert(con, target, source, key="accession")

    assert con.execute(f"DESCRIBE {target}").fetchall()[-1][:2] == ("taxid", "BIGINT[]")
    assert con.execute(
        f"SELECT accession, taxid FROM {target} ORDER BY accession"
    ).fetchall() == [("GPL1", None), ("GPL2", [9606]), ("GPL3", [])]
