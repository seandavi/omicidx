# 0008. Transforms use cdsci-lake's plain-SQL runner, not SQLMesh

- **Status**: Accepted
- Date: 2026-11-04
- Related: [ADR-0005](./0005-adopt-cdsci-lake-write-path.md), cdsci-lake
  [ADR-0021](https://github.com/seandavi/cdsci-lake/blob/main/docs/adr/0021-plain-sql-transforms-and-sqlglot-lineage.md)
  (plain-SQL transforms, stdlib DAG, sqlglot lineage), cdsci-lake ADR-0011
  (multi-producer write contract), #261

## Context

Since 2026-07 (`docs/specs/omicidx-marts-adaptation.md`) the marts have been a
SQLMesh project under `omicidx-prefect/transform/`: `flows/transform.py` ran
`Context(paths=...).plan("prod", auto_apply=True)` after `ducklake-load`, with
SQLMesh state in the lake Postgres under a `sqlmesh` schema and the materialized
views in `sqlmesh__{src,stg,sradb,geometadb}` schemas behind a virtual layer of
`lake.<schema>.<model>` views.

What omicidx actually used of SQLMesh, measured on 2026-11-04:

| Capability | Used |
|---|---|
| Models | 41, every one `kind VIEW` (14 `src`, 11 `stg`, 6 `geometadb`, 10 `sradb`) |
| Audits, unit tests, macros | none |
| Incremental / FULL / SCD kinds | none |
| Environments other than `prod`, dev→prod promotion | none (`plan(prod, auto_apply=True)` only) |
| Column-level lineage | computed but never wired anywhere ("Phase 2, not yet wired") |

SQLMesh therefore contributed two things: dependency ordering and SQL parsing.
It cost a large dependency tree (`sqlmesh`, `psycopg2-binary`, pandas, ...),
a second state store in the shared lake Postgres, the `sqlmesh__*` physical
schemas and virtual-layer indirection, a `config.py` that rebuilt the lake
connection a second way, and an `external_models.yaml` that had to be
re-introspected whenever a lake column changed.

cdsci-lake, the shared substrate omicidx already depends on for the write path
(ADR-0005), made the same assessment for its own models and settled on a
plain-SQL runner (cdsci-lake ADR-0021): one `SELECT` per file, header
directives (`-- description:`, `-- license:`, `-- materialized:`, `-- column`),
optional sibling `.test.sql` assertions, `graphlib` ordering from sqlglot table
references, each model run inside `ops.run`, and table + column lineage written
to `lake_ops`. ADR-0021 left omicidx's SQLMesh as "a producer-local choice".

## Decision

omicidx's transform layer moves onto `cdsci.lake.transform` (the
`cdsci-lake[transform]` extra).

- Each of the 41 models is a runner model at
  `transform/models/<schema>/<name>.sql`: the `MODEL (...)` header is gone, a
  `-- description:`, `-- license:` and `-- materialized: view` header is added,
  and every cross-model and lake reference is written `lake.<schema>.<table>`
  (how the runner names targets and the only form that binds the same way from
  any default catalog). The `src`/`stg`/`sradb`/`geometadb` layering and every
  view name are unchanged. SQLMesh silently lowercased unquoted identifiers, so
  the six mixed-case aliases in the `sradb` models (`study_ID`, `sample_ID`,
  `experiment_ID`, `run_ID`, `sra_ID`, `BioProject`) are now written lowercase
  explicitly; the published Parquet already carried the lowercase names, so the
  public column contract is unchanged (verified against production, see the PR).
- `flows/transform.py` is `load_models(MODELS_DIR)` + `run_all(con, models)`
  over omicidx's own `get_lake_connection()` (write mode, which attaches
  `lake_ops`), the same way cdsci-lake's own systemd units call the runner.
  Each model is a `CREATE OR REPLACE VIEW lake.<schema>.<name>` in dependency
  order, inside `ops.run`, with lineage replaced in `lake_ops.lineage` /
  `lake_ops.column_lineage`.
- `transform/config.py`, `external_models.yaml`, the `sqlmesh` and
  `psycopg2-binary` dependencies and the `--environment` flag of
  `omicidx-prefect run transform` are removed.
- The daily chain (`ducklake-load → transform → publish-bundle`) and the
  `publish-bundle` inputs (`lake.stg.*`, `lake.sradb.*`, `lake.geometadb.*`)
  do not change.

## Consequences

- **Cutover.** On the first production run the runner `CREATE OR REPLACE
  VIEW`s `lake.<schema>.<model>` directly, replacing SQLMesh's virtual-layer
  views of the same names in place. The `sqlmesh` state schema and the
  `sqlmesh__{src,stg,sradb,geometadb}` lake schemas become orphaned. They are
  **not** dropped by this change: the supervisor drops them only after a
  verified production run (views present, `publish-bundle` green). Dropping
  the `sqlmesh__*` schemas is a `DROP VIEW` per entry, no data files involved
  (every model was a view), so no `ducklake_expire_snapshots`/cleanup pass is
  owed beyond the usual weekly maintenance.
- **Observability.** Every mart build now leaves a `lake_ops.run` row
  (`source = '<schema>.<name>'`, `version = sql:<sha256[:12]>`) and an
  attributed DuckLake snapshot, plus current-state table and column lineage in
  `lake_ops` (cdsci-lake ADR-0021 §3–4). A view has no data, so every run is a
  "changed" snapshot. The runner does not count views (cdsci-lake #168), so
  `rows` is null for every mart.
- **Attribution.** Models run with `writer="omicidx"` (cdsci-lake #168), so mart
  runs, snapshots and assets are `omicidx:<schema>.<name>` (cdsci-lake ADR-0011).
  The runner takes no `extra`, so `run_id` is the `lake_ops` uuid, not systemd's
  `INVOCATION_ID` as with the loaders: find a mart build through `lake_ops.run`,
  not journald.
- **What is gone.** Virtual environments, `plan` previews, audits, the
  SQLMesh column-lineage API, and the SQLMesh-vs-DuckLake coexistence
  question. `docs/research/sqlmesh-on-ducklake.md` and
  `docs/specs/omicidx-marts-adaptation.md` stay as history.

## Revisit when (per cdsci-lake ADR-0021)

- A derived table is too large to rebuild as a view or a full table each run
  and needs `-- materialized: incremental` with a watermark.
- Real dev/prod environments are needed: a pre-production build in a scratch
  schema with tests, then a swap, because another producer's derived tables
  depend on omicidx's.
- A full run is too slow and only a family's models should rebuild
  (`run TARGET... --if-stale`, which the runner already has).

## Alternatives considered

- **Keep SQLMesh.** Rejected: nothing omicidx uses is SQLMesh-specific, and the
  shared substrate dropped it, so omicidx would be the last repo paying the
  version-lockstep and state-schema cost alone.
- **A second in-repo runner.** Rejected: ADR-0005's "depend, don't fork". The
  runner is ~700 lines in a package omicidx already pins.
