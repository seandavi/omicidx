#!/usr/bin/env python
"""Dev-schema validation for the cdsci-lake write-path migration (PR #118).

READ-ONLY. After you have run ``ducklake_load_flow(lake_schema="<schema>")``
against a dev schema, this asserts the migration's invariants against the lake
ledger + catalog and prints PASS/FAIL. It never writes to the catalog.

    uv run --package omicidx-prefect python scripts/validate_dev.py --schema omicidx_dev

Checks:
  * omicidx's sources are registered under ``writer='omicidx'``;
  * every table in ``lake.<schema>.*`` has NO ``_row_hash`` column and is populated;
  * the ``lake_ops.run`` ledger has a ``success``/``idempotent`` run per table;
  * SRA watermarks exist, per-schema (``sra`` / ``<schema>:<entity>``);
  * snapshot attribution (``author='omicidx:*'``) — best-effort, DuckLake shape varies.

Exit 0 iff no hard check FAILs. Run twice (after a second load) to confirm
idempotency — the second run's ledger rows should read ``idempotent``.
"""

from __future__ import annotations

import argparse
import sys

from omicidx.prefect.config import get_lake_connection

EXPECTED_SOURCES = {"sra", "geo", "biosample", "ebi_biosample", "pubmed", "bioproject"}

PASS, FAIL, WARN, INFO = "PASS", "FAIL", "WARN", "INFO"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Validate the dev-schema write path (PR #118)."
    )
    ap.add_argument(
        "--schema", default="omicidx_dev", help="lake schema that was loaded"
    )
    schema = ap.parse_args().schema

    results: list[tuple[str, str, str]] = []

    def record(level: str, check: str, detail: str = "") -> None:
        results.append((level, check, detail))

    con = get_lake_connection()  # write-mode attaches `ops`; this script only SELECTs
    try:
        # 1. sources registered under writer=omicidx
        try:
            got = {
                r[0]
                for r in con.execute(
                    "SELECT name FROM ops.lake_ops.source WHERE writer = 'omicidx'"
                ).fetchall()
            }
            missing = EXPECTED_SOURCES - got
            record(
                FAIL if missing else PASS,
                "sources registered (writer=omicidx)",
                f"missing: {sorted(missing)}" if missing else f"{sorted(got)}",
            )
        except Exception as e:  # noqa: BLE001
            record(FAIL, "sources registered (writer=omicidx)", repr(e))

        # 2. tables in the schema: exist, no `_row_hash`, populated
        try:
            tables = [
                r[0]
                for r in con.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_catalog = 'lake' AND table_schema = ? ORDER BY table_name",
                    [schema],
                ).fetchall()
            ]
        except Exception as e:  # noqa: BLE001
            tables = []
            record(FAIL, f"list tables in lake.{schema}", repr(e))
        if not tables:
            record(
                FAIL,
                f"tables present in lake.{schema}",
                "none found — did the flow run against this schema?",
            )
        for t in tables:
            cols = {
                r[0]
                for r in con.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_catalog='lake' AND table_schema=? AND table_name=?",
                    [schema, t],
                ).fetchall()
            }
            if "_row_hash" in cols:
                record(
                    FAIL,
                    f"{schema}.{t}: no _row_hash column",
                    "_row_hash still present",
                )
                continue
            n = con.execute(f'SELECT count(*) FROM lake."{schema}"."{t}"').fetchone()[0]
            record(
                PASS if n > 0 else WARN,
                f"{schema}.{t}: clean + populated",
                f"{n:,} rows, no _row_hash",
            )

        # 3. run ledger — latest run per target, status ok, snapshot recorded
        rows = con.execute(
            "SELECT source, target, status, rows_after, snapshot_before, snapshot_after "
            "FROM ops.lake_ops.run WHERE target LIKE ? ORDER BY started_at DESC",
            [f"lake.{schema}.%"],
        ).fetchall()
        latest: dict[str, tuple] = {}
        for r in rows:
            latest.setdefault(r[1], r)  # first seen = most recent
        if not latest:
            record(
                FAIL,
                f"ops.run rows for lake.{schema}.*",
                "no runs recorded — ledger not written",
            )
        for _source, target, status, rows_after, sb, sa in latest.values():
            idem = " (idempotent — no new snapshot)" if sb == sa else ""
            record(
                PASS if status in ("success", "idempotent") else FAIL,
                f"run {target}",
                f"status={status}{idem}, rows={rows_after}",
            )

        # 4. SRA watermarks, per-schema
        try:
            wms = con.execute(
                "SELECT name, value FROM ops.lake_ops.watermark WHERE source='sra' AND name LIKE ?",
                [f"{schema}:%"],
            ).fetchall()
            record(
                PASS if wms else WARN,
                "SRA watermarks set (per-schema)",
                ", ".join(f"{n}={v}" for n, v in wms)
                if wms
                else "none (expected if no SRA loader ran)",
            )
        except Exception as e:  # noqa: BLE001
            record(FAIL, "SRA watermarks", repr(e))

        # 5. snapshot attribution — best-effort (DuckLake snapshots() shape varies by version)
        try:
            n_snaps = con.execute("SELECT count(*) FROM lake.snapshots()").fetchone()[0]
            record(INFO, "catalog snapshots visible", f"{n_snaps} snapshots")
            try:
                authors = [
                    a[0]
                    for a in con.execute(
                        "SELECT DISTINCT author FROM lake.snapshots() WHERE author LIKE 'omicidx:%'"
                    ).fetchall()
                ]
                record(
                    PASS if authors else WARN,
                    "snapshots attributed author=omicidx:*",
                    ", ".join(authors)
                    if authors
                    else "none via snapshots() — verify commit metadata manually",
                )
            except Exception:  # noqa: BLE001
                record(
                    INFO,
                    "snapshot author read",
                    "this DuckLake's snapshots() has no author column; check commit metadata manually",
                )
        except Exception as e:  # noqa: BLE001
            record(WARN, "catalog snapshots", repr(e))
    finally:
        con.close()

    width = max((len(c) for _, c, _ in results), default=0)
    print(f"\nDev-schema validation — lake.{schema}\n" + "=" * (width + 22))
    for level, check, detail in results:
        print(f"[{level:4}] {check:<{width}}  {detail}")
    fails = [r for r in results if r[0] == FAIL]
    print("=" * (width + 22))
    print(f"{len(results)} checks, {len(fails)} FAIL")
    if fails:
        print("\nNOT green — resolve the FAILs before the prod cutover.")
        return 1
    print(
        "\nGreen — the dev write path is correct. Proceed per DEV_VALIDATION.md (cutover dev, then prod)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
