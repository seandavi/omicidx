# omicidx transform layer (plain-SQL models over DuckLake)

The marts are plain SQL files run by cdsci-lake's transform runner
(`cdsci.lake.transform`, cdsci-lake ADR-0021; omicidx ADR-0008). The runner
reads the `lake.omicidx.*` tables and materializes the derived layer
(`src` → `stg` → `geometadb`/`sradb`) as **views inside the DuckLake catalog**,
one `CREATE OR REPLACE VIEW lake.<schema>.<name>` per model, in dependency
order.

## Hard boundaries

- **T only.** The runner never touches the raw→lake incremental load. That
  stays cdsci-lake `upsert`/`ops` (see `DUCKLAKE.md`). It owns the *derived*
  layer exclusively; `lake.omicidx.*` are inputs it reads, never targets.
- **Views only, today.** All 41 models are `-- materialized: view`, live over
  the lake, nothing to backfill. Every run rebuilds every view (cheap DDL) and
  counts its rows for `lake_ops.run`.

## Layout

```
transform/
└── models/
    ├── src/        # 14 thin views: the single seam onto lake.omicidx.* tables
    ├── stg/        # 11 typed/normalized staging views
    ├── geometadb/  # 6 GEOmetadb-compatible views (public contract)
    └── sradb/      # 10 SRAdb-compatible views (public contract)
```

A model is `models/<schema>/<name>.sql` = target `lake.<schema>.<name>`. The
file is one `SELECT` (no trailing `;`) with header directives:

```sql
-- description: one line, becomes the view comment
-- license: upstream-ncbi-ebi-terms (...)
-- materialized: view
SELECT ... FROM lake.src.sra_samples s
LEFT JOIN lake.stg.ncbi_taxonomy_names n ON ...
```

Reference other models and lake tables as `lake.<schema>.<table>`: the runner
derives the DAG from these references (anything not a model is an external
input) and a fully qualified name binds the same way from any default catalog.
An optional sibling `<name>.test.sql` holds `-- test: <name>` queries that must
return zero rows; a failing one records an `error` run. None exist yet.

Connection and credentials come from omicidx's own `Settings`
(`get_lake_connection()`, which builds cdsci-lake's `Settings` from
`DUCKLAKE_URI` + `S3_*`), so the `.env` that drives the pipeline drives the
transform. Nothing else is configured.

## Operate

From the workspace root with the `.env` loaded:

```bash
# Rebuild every mart view in the lake, dependencies first (what the timer runs)
uv run omicidx-prefect run transform

# Inspect the DAG / list models offline (no lake connection)
uv run python -m cdsci.lake.transform \
  --models-dir packages/omicidx-prefect/src/omicidx/prefect/transform/models graph

# Ad-hoc query against the built marts
uv run python -c "from omicidx.prefect.config import get_lake_connection as c; \
  print(c(read_only=True).sql('SELECT count(*) FROM lake.sradb.study'))"
```

In the pipeline `flows/transform.py` runs after `ducklake-load`
(`flows/main.py`). Idempotent: `CREATE OR REPLACE VIEW` every time.

## Organism names (#226)

GEO renames organisms (following NCBI Taxonomy) without bumping
`last_update_date`, so names in the lake go stale while taxids stay right. The
organism-bearing `stg` views (`geo_series`, `geo_samples`, `geo_platforms`,
`sra_samples`, `biosamples`) are therefore their lake table column for column, with the names
replaced by the current scientific name for the record's taxid
(`stg.ncbi_taxonomy_names`, which follows merged taxids) and the submitted names
kept in appended `*_as_submitted` columns. An unknown taxid keeps the submitted
name. `publish-bundle` ships those five base files from the `stg` views for the
same reason (`TAXONOMY_RESOLVED_EXPORTS`). `geo_platforms` is resolved the same
way from its `taxid` list (#231), once every GPL has been re-extracted with it
(`python -m omicidx.prefect.flows.geo reextract-platforms`); a NULL taxid keeps
the submitted name.

## Runs and lineage

Each model runs inside `ops.run` (one `lake_ops.run` row per model,
`source = '<schema>.<name>'`, `version = sql:<sha256[:12]>` of the file) and
the snapshot it writes is attributed. After the view is created the runner
replaces that model's table lineage (`lake_ops.lineage`, `edge_type =
'sqlglot'`) and column lineage (`lake_ops.column_lineage`) from sqlglot, with
the input schemas read from the lake so `SELECT *` expands. Lineage is
best-effort observability and never fails a run.

The runner registers models with `writer = 'cdsci'` and does not take omicidx's
`run_id` (systemd `INVOCATION_ID`) as `extra`; see ADR-0008 for what that means
when attributing a mart snapshot.

## Tests

`tests/test_marts_schema.py` loads the models with the runner's `load_models`,
builds the `geometadb`/`sradb` marts in the runner's dependency order over
synthetic typed `stg`/`src` tables (in an in-memory catalog attached as
`lake`) and asserts the exact public column contract — offline, no live
catalog. Run: `uv run pytest packages/omicidx-prefect/tests/test_marts_schema.py`.
