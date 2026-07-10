# Dev-schema validation — cdsci-lake write-path migration (PR #118)

Prove the new `upsert`/`ops` write path is correct against a **dev schema**
before the destructive prod `_row_hash` cutover. Nothing here touches prod:
dev writes land in `lake.omicidx_dev.*`, and dev SRA watermarks are keyed
`sra / omicidx_dev:<entity>` — isolated from prod's `omicidx:<entity>`.

## 0. Prereqs
- A host/worker with lake access and the usual omicidx-prefect `.env`
  (`DUCKLAKE_URI`, S3/R2 creds, `PUBLISH_ROOT`) in scope.
- `uv sync` so `cdsci-lake v0.2.1` is installed.

## 1. (optional) Bound the SRA cost
A fresh dev SRA watermark means a full scan of all raw SRA partitions (large).
To validate on just recent data, seed the four dev cursors to a recent partition
date first (use a date that matches your raw `date=` partition format):

```bash
uv run --package omicidx-prefect python - <<'PY'
from omicidx.prefect.config import get_lake_connection
from cdsci.lake import ops
with get_lake_connection() as con:
    for e in ("sra_study", "sra_sample", "sra_experiment", "sra_run"):
        ops.set_watermark(con, "sra", f"omicidx_dev:{e}", "2026-07-01")
PY
```

## 2. Run the load against the dev schema
```bash
uv run --package omicidx-prefect python - <<'PY'
from omicidx.prefect.flows.ducklake_load import ducklake_load_flow
ducklake_load_flow(lake_schema="omicidx_dev")
PY
```
(For a quick smoke, call a single small loader inside a `@flow` instead, e.g.
`bioproject_to_ducklake(lake_schema="omicidx_dev")`.)

## 3. Verify
```bash
uv run --package omicidx-prefect python packages/omicidx-prefect/scripts/validate_dev.py --schema omicidx_dev
```
Green = every table clean (no `_row_hash`) and populated, a `success` run per
table in `lake_ops.run`, SRA watermarks set, and the 6 sources registered under
`writer=omicidx`. Exit code is non-zero if any hard check fails.

## 4. Idempotency (the copy-on-write guarantee)
Re-run step 2, then step 3. The second run's ledger rows should read
**`idempotent`** (`snapshot_before == snapshot_after`) — unchanged data writes no
new snapshot. If a re-run shows `success` with a new snapshot, the change-gate is
wrong; stop and investigate before prod.

## 5. Green light → cutover → merge
1. Run `scripts/cutover_drop_row_hash.sql` against **omicidx_dev first** — it
   should be a no-op (dev tables were created clean), which confirms the SQL is
   valid against the catalog.
2. Run it against **prod** (`lake.omicidx.*`) — drops the stale `_row_hash`
   column the new projections no longer produce. Destructive, deliberate.
3. Merge PR #118 and let the daily pipeline run the new path.

## Rollback (dev only; prod untouched)
```sql
-- via a lake connection
DROP SCHEMA IF EXISTS lake.omicidx_dev CASCADE;   -- or DROP each lake.omicidx_dev.* table
DELETE FROM ops.lake_ops.watermark WHERE name LIKE 'omicidx_dev:%';
DELETE FROM ops.lake_ops.run       WHERE target LIKE 'lake.omicidx_dev.%';
```
