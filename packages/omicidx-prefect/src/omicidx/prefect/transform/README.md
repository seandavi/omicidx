# omicidx transform layer (SQLMesh over DuckLake)

SQLMesh is omicidx's **transform (T) engine**. It reads the `lake.omicidx.*`
tables and materializes the derived marts (`src` → `stg` → `geometadb`/`sradb`)
as **views inside the DuckLake catalog**. It replaces the old numbered-SQL
runner (`sql/020–050`) with a real dependency DAG + `plan`/`apply` + a prod
virtual environment.

## Hard boundaries

- **T only.** SQLMesh never touches the raw→lake incremental load. That stays
  cdsci-lake `upsert`/`ops` (see `DUCKLAKE.md`). SQLMesh owns the *derived*
  layer exclusively — it must not manage the `lake.omicidx.*` base tables, which
  are declared to it as **external models** (`external_models.yaml`).
- **Deploy-time, not daily-data-path.** All 38 marts are `kind VIEW` — live over
  the lake, nothing to backfill. SQLMesh acts at deploy (`transform` flow), not
  on every daily run.
- **prod environment only** (no dev→prod gate yet).

## Layout

```
transform/
├── config.py             # SQLMesh Config — reuses omicidx Settings for creds
├── external_models.yaml  # the 14 lake.omicidx.* base tables (auto-introspected)
└── models/
    ├── src/        # 14 thin views: the single seam onto lake tables
    ├── stg/        # 11 typed/normalized staging views
    ├── geometadb/  # 6 GEOmetadb-compatible views (public contract)
    └── sradb/      # 10 SRAdb-compatible views (public contract)
```

`config.py` pulls all credentials from omicidx-prefect's own `Settings`
(`DUCKLAKE_URI`, `S3_*`), so the same `.env` that drives the pipeline drives
SQLMesh. The DuckLake catalog attaches as `lake`; SQLMesh state lives in the
same lake Postgres under the `sqlmesh` schema (independent of DuckLake's own
`ducklake_*` metadata — verified to coexist).

## Operate

From this directory (`.env` on PATH / env vars set):

```bash
# What would change? (read-only preview; safe)
uv run sqlmesh plan

# Apply the marts into the lake (WRITES views into the shared catalog)
uv run sqlmesh plan --auto-apply          # or: omicidx-prefect run transform

# Re-introspect after an upstream lake column changes
uv run sqlmesh create_external_models

# Ad-hoc query against the applied marts
uv run sqlmesh fetchdf "SELECT count(*) FROM sradb.study"
```

In the pipeline the `omicidx-transform` Prefect flow (`flows/transform.py`) runs
`plan(prod, auto_apply=True)` after `ducklake-load`. Idempotent: an unchanged
plan applies nothing.

## Organism names (#226)

GEO renames organisms (following NCBI Taxonomy) without bumping
`last_update_date`, so names in the lake go stale while taxids stay right. The
organism-bearing `stg` views (`geo_series`, `geo_samples`, `sra_samples`,
`biosamples`) are therefore their lake table column for column, with the names
replaced by the current scientific name for the record's taxid
(`stg.ncbi_taxonomy_names`, which follows merged taxids) and the submitted names
kept in appended `*_as_submitted` columns. An unknown taxid keeps the submitted
name. `parquet-export` ships those four base files from the `stg` views for the
same reason (`TAXONOMY_RESOLVED_EXPORTS`). `geo_platforms` has no taxid in the
lake (the GPL parser drops it), so GPL names stay as submitted.

## Column lineage (Phase 2, not yet wired)

Column-level lineage is a free byproduct — the `SELECT *` src views expand to
real per-column edges because the base tables are declared as external models
with columns. Headless:

```python
from sqlmesh import Context
from sqlmesh.core.lineage import column_dependencies
ctx = Context(paths=".")
column_dependencies(ctx, '"lake"."sradb"."study"', "study_accession")
# -> {'"lake"."stg"."sra_studies"': {'accession'}}
```

Syncing these edges into `ops.lineage` is Phase 2 (gated on cdsci-lake PR #19).

## Tests

`tests/test_marts_schema.py` builds the `geometadb`/`sradb` marts over synthetic
typed `stg`/`src` tables and asserts the exact public column contract — offline,
no live catalog. Run: `uv run pytest tests/test_marts_schema.py`.
