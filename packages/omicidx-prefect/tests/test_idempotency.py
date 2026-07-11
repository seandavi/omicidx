"""Idempotency smoke test for the reproduce-from-raw contract (spec §1, A3).

The reproduce-from-raw acceptance — "a re-run writes zero new data files" —
rests on TWO properties:

1. cdsci-lake's ``upsert`` is idempotent: re-running the same source writes
   ZERO new data files, because the MERGE gates UPDATEs on ``IS DISTINCT FROM``
   and DuckLake is copy-on-write. **This test proves (1)** against the REAL
   ``upsert`` (the primitive every loader routes through), not a mock.
2. each loader's source ``SELECT`` is deterministic across runs, so it feeds
   ``upsert`` identical input. **This test does NOT cover (2)** — a
   non-deterministic projection (e.g. an injected load timestamp) would feed
   different rows and break idempotency at the loader layer, not here.

Scope: exercises the shared write primitive with a toy source against a LOCAL
ephemeral DuckLake (catalog file + local data dir) — no R2, no cdsci-lake
catalog, no credentials, and it does not run the flow (hard gate #1). The flow
composes this same ``upsert`` once per loader.
"""

import glob

import duckdb
import pytest
from cdsci.lake.connect import upsert


def _local_lake(tmp_path):
    """Attach a throwaway file-catalog DuckLake with a local data path."""
    data = tmp_path / "data"
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
    return con, data


def _data_files(data) -> list[str]:
    return sorted(glob.glob(f"{data}/**/*.parquet", recursive=True))


def _snapshot_count(con) -> int:
    return con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0]


# A tiny stand-in for a loader's deduped/typed raw projection: keyed by
# `accession`, exactly like every omicidx upsert target.
_SOURCE = "SELECT * FROM (VALUES ('SRR1','a'),('SRR2','b'),('SRR3','c')) AS t(accession, val)"


def test_rerun_writes_zero_new_data_files(tmp_path):
    """The acceptance criterion: an unchanged re-run adds no data files."""
    con, data = _local_lake(tmp_path)

    upsert(con, "lake.omicidx.thing", _SOURCE, key="accession")
    files_first = _data_files(data)
    snaps_first = _snapshot_count(con)
    assert files_first, "first load must write at least one data file"

    # Reproduce from the identical source — the no-op re-run.
    upsert(con, "lake.omicidx.thing", _SOURCE, key="accession")

    assert _data_files(data) == files_first, (
        "idempotent re-run must write ZERO new data files"
    )
    assert _snapshot_count(con) == snaps_first, (
        "idempotent re-run must add no snapshot"
    )
    assert con.execute("SELECT count(*) FROM lake.omicidx.thing").fetchone()[0] == 3


def test_real_change_commits_a_snapshot(tmp_path):
    """Guard against a trivial 'never writes': a real delta must commit work.

    (For a tiny table the on-disk parquet *file count* did not change on an
    update in local testing, so file count is not a reliable change signal at
    this scale — the committed snapshot is. The no-op re-run above commits no
    snapshot; a real change commits one — the IS DISTINCT FROM gate working in
    both directions.)
    """
    con, data = _local_lake(tmp_path)

    upsert(con, "lake.omicidx.thing", _SOURCE, key="accession")
    snaps_before = _snapshot_count(con)

    changed = "SELECT * FROM (VALUES ('SRR1','CHANGED'),('SRR2','b'),('SRR3','c')) AS t(accession, val)"
    upsert(con, "lake.omicidx.thing", changed, key="accession")

    assert _snapshot_count(con) > snaps_before, (
        "a changed row must commit a new snapshot (the IS DISTINCT FROM gate fires)"
    )
    assert (
        con.execute("SELECT val FROM lake.omicidx.thing WHERE accession = 'SRR1'").fetchone()[0]
        == "CHANGED"
    )
