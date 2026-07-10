# 0005. Adopt the cdsci-lake shared write path

- Status: accepted
- Date: 2026-07-10

## Context

omicidx and cdsci-lake are peer producers into the one shared DuckLake catalog, but
omicidx hand-rolled its write path (`get_ducklake_connection`, `merge_to_ducklake`
with a `_row_hash` gate, `replace_to_ducklake`, `HighWaterMark` + semaphore-file
watermarks, a `prefect:ducklake-load` snapshot shape) parallel to cdsci-lake's
(`lake_connect`, `upsert` with `IS DISTINCT FROM`, `ops.run`/`ops.watermark`, the
`cdsci:<source>` snapshot shape). The parallel copy carried a double-list
`_row_hash` drift hazard and duplicated, already-drifted connection code.

## Decision

omicidx depends on `cdsci-lake` (base install) and adopts its write path, per
**cdsci-lake ADR-0011** (the full contract and its seven decisions). Concretely:

- Delete `merge_to_ducklake` + `HighWaterMark`; add `get_lake_connection`
  (env-cred `lake_connect`) + `ops.run` + `upsert` as the write path.
  `get_ducklake_connection` is **retained** (staged migration) for the
  read-consumers (`parquet_export`, `postgres`) and the parked loaders until a
  later pass converts them — it is simply no longer used for writes. The EL write
  path is **`upsert`-only** (cdsci-lake ADR-0013); `sra_accessions` becomes
  `upsert`-on-`accession`.
- Drop `_row_hash` from every projection and stored table.
- **Park the derived loaders** — `publication_accession_linkage` and
  `geo_series_with_rnaseq_counts` are transform-layer artifacts (ADR-0013), left
  un-wired (`replace_to_ducklake` retired from the load path) until the transform
  layer lands.
- Keep `SemaphoreStore` for raw-extract **crawl gating only**; the lake-load
  watermark + run history move to `ops`.
- Declare `OMICIDX_SOURCES`; register at the top of `ducklake_load_flow` via
  `ops.register_sources(con, writer="omicidx", sources=OMICIDX_SOURCES)`
  (idempotent, self-healing).
- Snapshots stamp `author="omicidx:<source>"`, canonical `commit_extra_info` + a
  `prefect_run_id` extra.

## Consequences

- The `_row_hash` drift hazard is deleted; one MERGE/connection/attribution path
  across both producers.
- omicidx gains a queryable run ledger (last loaded / changed / errored / which
  snapshot) it previously lacked, and becomes the reference second producer that
  proves the shared seam.
- omicidx's Prefect workers keep env-injected credentials via `lake_connect`'s
  pluggable cred source — no GSM dependency in the workers.
- **Resolved:** the derived `linkage` table is a transform-layer artifact
  (cdsci-lake ADR-0013), parked until the transform layer lands — not wired into the
  lake load. omicidx ADR-0001 still commits to publishing it; that happens from the
  transform layer, not the EL path.
