# SQLMesh on DuckLake — feasibility for the OmicIDX transform + lineage layer

Research note. Primary sources only (SQLMesh docs on `sqlmesh.readthedocs.io`,
DuckLake docs on `ducklake.select`, SQLGlot source, GitHub issues/PRs on
`TobikoData/sqlmesh` and `duckdb/ducklake`). Dates/versions flagged inline.
No code was changed; this is a decision aid.

Location note: `docs/research/` did not previously exist (the repo had
`docs/specs/` and `docs/adrs/`). Created `docs/research/` for this and future
investigation notes, since it is neither a finalized decision (ADR) nor a
durable spec.

Key dates: PR that landed remote DuckLake `data_path` in SQLMesh merged
**2025-06-02**. DuckLake ATTACH docs read as **v1.0 stable** at time of writing
(2026-07). SQLMesh `sqlmesh.core.lineage` API is in the `stable` docs.

---

## Bottom-line verdict

**The SQLMesh-on-DuckLake bet is materially de-risked — it is not speculative
territory.** SQLMesh ships a first-party DuckLake catalog type in its DuckDB
engine adapter (`catalogs: { name: { type: ducklake } }`), and there is a public
reference project that runs the *exact* OmicIDX topology: DuckLake catalog with
**Postgres metadata + Cloudflare R2 data**, SQLMesh **state in that same
Postgres**, R2 secrets, encryption — the works
([mattiasthalen/ducklake config.yaml](https://raw.githubusercontent.com/mattiasthalen/ducklake/main/config.yaml)).
That single repo answers questions 1 and 2 in the affirmative with a working
config, not a promise.

The four things that MUST be verified by a spike, in priority order:

1. **Attach + plan + apply against OUR live catalog (P0, make-or-break).**
   Stand up SQLMesh against the real cdsci-lake Postgres catalog + `r2://cdsci-lake/`
   data path and get one trivial model to `plan`/`apply`. Confirm SQLMesh's
   physical-table creation and its virtual-view layer both land correctly inside
   a DuckLake-attached catalog (DuckLake mediates all writes; SQLMesh assumes it
   can `CREATE TABLE`/`CREATE VIEW` freely). This is the load-bearing unknown —
   the reference repo proves the *shape* works, not that it works against OUR
   catalog/version.
2. **State-vs-catalog coexistence in one Postgres (P0).** SQLMesh state schema
   (`sqlmesh` by default) sharing the same Postgres instance as the DuckLake
   catalog metadata tables. Confirm no table-name/schema collision and that
   `sqlmesh run` snapshotting does not interact with DuckLake's own snapshot
   metadata. They are independent snapshot systems living in the same database —
   verify they stay independent.
3. **Column-level lineage headless (P1).** Call `sqlmesh.core.lineage` from a
   plain Python process (no UI) and dump column edges to a table. Confirm the
   `SELECT *` views expand to real column edges once upstream Parquet is declared
   as external models — otherwise ADR-0014's lineage contract gets `*` holes.
4. **Version pinning + the "custom fork" ambiguity (P1).** The reference repo's
   README still points at a *custom SQLMesh fork* for `data_path`, yet its
   `main` `config.yaml` uses stock keys and upstream PR #4600 merged `data_path`
   on 2025-06-02. Pin a specific SQLMesh release ≥ that merge and verify
   `DuckDBAttachOptions` accepts `data_path: r2://…` in stock SQLMesh.

If those four pass, the numbered-SQL runner can be retired for the derived/marts
layer with high confidence. The reverse-ETL publish step (Q5) and the A/B-slot
publishing semantics (Q4) are the parts SQLMesh does *not* fully subsume — plan
to keep those in Prefect.

---

## Q1 — SQLMesh × DuckLake (LOAD-BEARING) — **PROVEN**

SQLMesh's DuckDB engine adapter has a first-party `catalogs` block, and
`ducklake` is an explicitly supported catalog `type`. From the DuckDB engine
docs ([sqlmesh.readthedocs.io/en/latest/integrations/engines/duckdb/](https://sqlmesh.readthedocs.io/en/latest/integrations/engines/duckdb/)),
verbatim config shape:

```yaml
gateways:
  my_gateway:
    connection:
      type: duckdb
      catalogs:
        ducklake:
          type: ducklake
          path: 'catalog.ducklake'
          data_path: data/ducklake
          override_data_path: true
          encrypted: True
          data_inlining_row_limit: 10
          metadata_schema: main
```

`DuckDBAttachOptions` for `type: ducklake` supports `path` (catalog location —
can be a `postgres:` DSN), `data_path` (where Parquet lands — can be remote),
`override_data_path`, `encrypted`, `data_inlining_row_limit`, `metadata_schema`.

**The clincher — a working repo in OUR exact topology.** [mattiasthalen/ducklake
`config.yaml`](https://raw.githubusercontent.com/mattiasthalen/ducklake/main/config.yaml)
(DuckLake + SQLMesh + Neon Postgres + Cloudflare R2) runs:

```yaml
gateways:
  ducklake:
    connection:
      type: duckdb
      catalogs:
        ducklake:
          type: ducklake
          path: postgres:host={{ env_var('PG__HOST') }} dbname={{ env_var('PG__DATABASE') }} user=... password=...
          data_path: r2://ducklake            # <-- remote R2 data path
          encrypted: true
      extensions:
        - ducklake
        - httpfs
      secrets:
        - type: r2
          account_id: {{ env_var('R2__ACCOUNT_ID') }}
          key_id: {{ env_var('R2__ACCESS_KEY_ID') }}
          secret: {{ env_var('R2__SECRET_ACCESS_KEY') }}
```

This is Postgres-backed catalog metadata + R2 Parquet data + encryption + R2
secrets + the `ducklake`/`httpfs` extensions auto-loaded — precisely the
cdsci-lake substrate. The catalog `path` is a raw `postgres:` DSN, mirroring
DuckLake's own ATTACH form
`ATTACH 'ducklake:postgres:dbname=postgres' (DATA_PATH 's3://my-bucket/my-data/')`
([ducklake.select/docs/stable/duckdb/usage/connecting](https://ducklake.select/docs/stable/duckdb/usage/connecting), v1.0 stable).

Remote `data_path` was a real gap that got fixed: issue
[#4590 "Specify data_path for DuckLake"](https://github.com/TobikoData/sqlmesh/issues/4590)
(opened 2025-05-30 by mattiasthalen) → resolved by
[PR #4600](https://github.com/TobikoData/sqlmesh/pull/4600) (merged **2025-06-02**
by @izeigerman), which added `DATA_PATH` + `ENCRYPTED` support to the `ducklake`
catalog type. So any SQLMesh release after 2025-06-02 has it upstream.

**Residual risk (why it's PROVEN not blind-trust):** the reference repo's README
still references a *custom fork* (`mattiasthalen/sqlmesh@add_data_path_to_duckdb_catalog`)
for `data_path`, which predates the upstream merge. Pin a stock release ≥ the
2025-06-02 merge and confirm `data_path: r2://…` works without the fork. Also the
reference proves attach + `plan`; it does not prove *our* catalog version /
DuckLake version behave identically under heavy `CREATE TABLE` from SQLMesh.
Spike item #1.

---

## Q2 — SQLMesh state backend — **PROVEN**

SQLMesh persists its own state (snapshots, environments, versions) in a dedicated
**`state_connection`**, separate from the data warehouse
([configuration guide](https://sqlmesh.readthedocs.io/en/stable/guides/configuration/)).
Postgres is a first-class, production-recommended state engine; DuckDB is
explicitly warned against for state ("single-user database … will not scale to
production usage … simultaneous writes … corrupted data").

Yes, it can be the **same Postgres as the DuckLake catalog, isolated by schema**.
Config:

```yaml
gateways:
  my_gateway:
    state_connection:
      type: postgres
      host: <host>
      port: <port>
      user: <username>
      password: <password>
      database: <database>
    state_schema: custom_name     # default schema is `sqlmesh`
```

The reference repo does exactly this — `state_connection: { type: postgres … }`
pointed at the same Neon Postgres that backs the DuckLake catalog.

**No inherent conflict, but confirm independence.** SQLMesh state (schema
`sqlmesh` by default) and DuckLake's catalog metadata tables (`ducklake_*` under
`metadata_schema`, default `main`) are two independent snapshot/versioning
systems that happen to share a Postgres instance. They do not know about each
other — SQLMesh snapshots track *model* versions; DuckLake snapshots track
*table-file* versions. Keep them in distinct schemas (they default to different
ones already) and there is no collision. Spike item #2 verifies this against the
live catalog rather than trusting defaults.

---

## Q3 — Column-level lineage via Python API — **PROVEN**

Module: **`sqlmesh.core.lineage`**
([API docs](https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/lineage.html)).
Exact signatures:

```python
def lineage(column: str | exp.Column, model: Model,
            trim_selects: bool = True, **kwargs) -> Node        # sqlglot.lineage.Node
def column_dependencies(context: Context, model_name: str,
            column: str | exp.Column) -> dict[str, set[str]]     # {parent_model: {cols}}
def column_description(context: Context, model_name: str, column: str,
            quote_column: bool = False) -> str | None
```

- **Granularity: column-level.** `column_dependencies(context, "sradb.study",
  "study_accession")` returns `{parent_model_name: {upstream_column, …}}` — the
  exact edge shape ADR-0014 needs.
- **Relationship to SQLGlot:** SQLMesh's `lineage()` wraps SQLGlot's
  `sqlglot.lineage.lineage()` — it qualifies the query with SQLGlot's optimizer,
  builds a scope, and returns a `sqlglot.lineage.Node` graph
  ([sqlglot.com/sqlglot/lineage.html](https://sqlglot.com/sqlglot/lineage.html)).
  `column_dependencies` walks that Node graph to build the dict.
- **Headless:** these are plain functions taking a `Context` (built from the
  project dir) and model name — no UI, no server. A Prefect task can load the
  `Context`, iterate `context.models`, iterate each model's output columns, call
  `column_dependencies`, and write edges to external metadata tables. This is the
  intended headless path.

**Caveat that becomes a spike item:** SQLMesh docs state `SELECT *` "will prevent
SQLMesh from determining upstream column-level lineage unless you use an external
model kind." OmicIDX's `src_*` and `stg_*` views are `SELECT *` over Parquet (see
`020_base_parquet_views.sql`). To get real column edges (not `*` blobs), the
upstream Parquet must be declared as **external models with columns** (Q6) so
SQLMesh can expand the stars. Verify the expansion produces per-column edges, not
a single `*` node — spike item #3.

---

## Q4 — Virtual environments / blue-green — **PROVEN mechanism, PARTIAL fit**

Mechanism ([environments concept](https://sqlmesh.readthedocs.io/en/stable/concepts/environments/)):
SQLMesh materializes each model version as a **physical table keyed by a
fingerprint** (content hash of the model). An **environment is a collection of
references (a thin layer of views) pointing at those physical tables.** A model
`db.model_a` in env `my_dev` is exposed as a view `db__my_dev.model_a`; prod keeps
the bare name. `plan`/`apply` computes changed fingerprints, backfills only the
gaps into new physical tables, then the **virtual update** swaps the view layer to
point at the new physical tables. Changes in one environment "do not impact"
others because unchanged fingerprints reuse existing physical tables.

This is genuine blue-green: the view swap is atomic-ish and the old physical
tables remain until cleaned up, so promotion is cheap and reversible.

**Fit for OmicIDX — partial, and worth being honest about:**
- ✅ For the **derived/marts layer** (`stg_*`, `geometadb.*`, `sradb.*`), the
  view-swap-over-physical-tables model is a clean, native replacement for a
  hand-rolled A/B-slot swap. Build the new marts as fingerprinted physical tables,
  virtual-update the views — that IS the A/B swap, for free.
- ⚠️ It does **not** subsume the **public R2 publishing** semantics. SQLMesh's
  virtual layer is views inside the *catalog it manages*. The OmicIDX contract
  (rolling `latest/*.parquet` + immutable `v{date}/*.parquet` on R2, consumed
  anonymously over HTTPS by external DuckDB) is a *file-artifact* publish, not a
  view swap. SQLMesh has no concept of "immutable dated Parquet snapshot on R2 for
  anonymous HTTPS consumers." That publishing stays a reverse-ETL COPY (Q5),
  triggered by SQLMesh hooks but owned as an explicit step.
- ⚠️ Environments/fingerprints assume SQLMesh *owns* the physical tables. If the
  same DuckLake tables are also written by the Prefect `ducklake-load` MERGE flows,
  do not let SQLMesh manage those — SQLMesh manages the *derived* layer only; the
  raw→lake MERGE stays outside (declared to SQLMesh as external models).

---

## Q5 — Reverse-ETL / export to Parquet — **PROVEN (hooks exist), fit is a design choice**

SQLMesh has concrete hook points that can run arbitrary SQL, including
`COPY … TO 'r2://…' (FORMAT PARQUET)`:

- **`before_all` / `after_all`** — lists of SQL statements / SQLMesh macros run at
  the start/end of `sqlmesh plan` and `sqlmesh run`. Macro vars available:
  `@this_env`, `@schemas`, `@views`
  ([SQL models docs](https://sqlmesh.readthedocs.io/en/stable/concepts/models/sql_models/)).
- **`on_virtual_update`** — SQL run *after* the virtual update completes (docs'
  canonical use: `GRANT` on the swapped views). Definable per-model or
  project-wide via `model_defaults`. This is the natural place to trigger a
  publish once the new marts are live.
- **Per-model pre/post-statements** — SQL before/after a single model's query;
  `@IF(@runtime_stage = …)` gates them by stage.

So a "COPY marts → R2 Parquet `latest/` + `v{date}/`" step *can* live in an
`after_all` or `on_virtual_update` hook. **Recommended fit, though:** keep the R2
publish (with its `latest/` rolling + immutable `v{date}/` + manifest semantics,
per ADR-0004) in **Prefect**, invoked after `sqlmesh run` succeeds. Reasons: the
dated-snapshot + provenance-manifest logic is bespoke and already lives in
Prefect (`parquet_export.py`, `publish_bundle.py`); embedding multi-target COPY +
manifest generation inside a SQL hook buys nothing and couples publishing to
SQLMesh's run lifecycle. Hooks are the fallback if you later want SQLMesh to be
the single entry point.

Verdict: **PROVEN** that SQLMesh *can* own the export; **recommended UNPROVEN-as-
default** — export should stay a Prefect step calling COPY, not a SQLMesh hook,
unless you deliberately want SQLMesh as sole orchestrator.

---

## Q6 — Migration mechanics — **PROVEN, mechanical, ~1–2 day port**

The ~38 statements across `020`–`050` are plain `CREATE OR REPLACE VIEW` defs.
Mapping to SQLMesh:

| Current | SQLMesh |
|---|---|
| `src_*` = `SELECT * FROM read_parquet('…/latest/x.parquet')` (020, 12 views) | **External models** (`external_models.yaml`, one entry per Parquet with columns) OR thin `VIEW` models. Declaring them external is what unlocks column lineage through the `SELECT *` (Q3). |
| `stg_*` dedup views (030) | `MODEL (kind VIEW)` — one file per model |
| `geometadb.*`, `sradb.*` schema views (040/050) | `MODEL (kind VIEW)` with the schema in the model name (`sradb.study`) |

**Model kinds** ([models overview](https://sqlmesh.readthedocs.io/en/stable/concepts/models/overview/)):
- `VIEW` — recreated as a database view; **direct 1:1 for every current view.**
  Start here for all 38 — lowest-effort, preserves current semantics.
- `FULL` — full table rebuild each run; use if you later want the marts
  materialized as DuckLake tables (needed for the Q4 fingerprint/A-B swap benefit).
- `INCREMENTAL_*` — only for append/merge-by-time-or-key; **not** applicable to
  these stateless derived views. Skip.

**External sources** ([external models docs](https://sqlmesh.readthedocs.io/en/stable/concepts/models/external_models/)):
`sqlmesh create_external_models` introspects referenced tables and writes their
columns to `external_models.yaml`. Since `read_parquet('…')` has a resolvable
schema, this can auto-populate the 12 upstream Parquet definitions — but confirm
it introspects a `read_parquet(url)` expression and not just catalog tables
(spike). If not, hand-write the 12 YAML entries (cheap).

**Concrete gotchas for OmicIDX's SQL specifically:**
1. **`SELECT *` everywhere** (020, 030) → declare upstream as external models or
   lineage returns `*` nodes. This is the single most important porting rule.
2. **Imperative `CREATE SCHEMA sradb; USE sradb;`** (050) → **delete these.**
   SQLMesh owns schema/view creation; you encode the schema in the model name
   (`MODEL (name sradb.study, kind VIEW)`). Do not issue `USE`/`CREATE SCHEMA`.
3. **One model per file, and a rename per model.** Each `CREATE VIEW x` becomes a
   `MODEL (name schema.x …)` file. ~38 files. Mechanical, not clever.
4. **`{{PUBLIC_PARQUET_BASE}}` build-time substitution** → replace with a SQLMesh
   macro var / `@gateway`-aware value, or point models at the DuckLake catalog
   tables directly instead of the published HTTPS Parquet. Decision: do the marts
   read the *published* Parquet (current behavior) or the *lake* tables? SQLMesh
   nudges toward reading lake/catalog tables; that's a semantic change to weigh.
5. **`ROW_NUMBER() OVER (ORDER BY …)` surrogate keys, `CAST(attributes AS VARCHAR)`**
   → plain SQL, port as-is. Set `model_defaults: { dialect: duckdb }`.
6. **Audits/macros are optional.** No `@each`/`@EACH` or audits are *required* to
   port; they're upside (add `NOT NULL`/uniqueness audits on surrogate keys later).

Effort: the transforms are simple and stateless, so this is a rote 1:1 port
(~38 small files + ~12 external-model entries), on the order of a focused day or
two, dominated by the `SELECT *`/external-model decision and the
published-Parquet-vs-lake-table decision, not by SQLMesh mechanics.

---

## Q7 — Integration signal + scheduler — **PROVEN**

**First-party signal:** Tobiko markets a "DuckLake + SQLMesh" lakehouse story and
ships the `ducklake` catalog type in stock docs; the DuckDB engine adapter page
documents it directly
([duckdb engine docs](https://sqlmesh.readthedocs.io/en/latest/integrations/engines/duckdb/)).
Issue #4590 → PR #4600 shows Tobiko maintainers (@izeigerman) actively landing
DuckLake remote-data support. Community reference implementation in our exact
topology exists ([mattiasthalen/ducklake](https://github.com/mattiasthalen/ducklake)).
This is not a fringe/unsupported combination.

**Scheduler — Prefect can just invoke `sqlmesh run`:**
([scheduling guide](https://sqlmesh.readthedocs.io/en/stable/guides/scheduling/))
`sqlmesh run` "automatically detects missing intervals for all models … and then
evaluates them," runs to completion, and **exits** ("does not run continuously").
It is explicitly designed to be triggered by "a cron job, a CI/CD tool … or in a
similar fashion" — i.e. **Prefect calls `sqlmesh run` on its schedule.** How it
decides what to run: SQLMesh is **interval/cron-aware per model** (each model has a
`cron`; `run` fills the intervals that are due) — it's not a naive "run
everything," it computes the due/missing intervals and evaluates only those. For
the OmicIDX derived layer (stateless VIEW models), `run` is effectively "rebuild
whatever changed," and Prefect retiring the transform-half of orchestration to a
single `sqlmesh run` call is viable. Prefect stays for extraction, ducklake-load
MERGE, and the R2 publish.

---

## Spike checklist (do these before committing)

1. **P0 — Attach + plan + apply against live cdsci-lake.** Stock SQLMesh (pinned
   release ≥ 2025-06-02) → `catalogs: {lake: {type: ducklake, path: postgres:…,
   data_path: r2://cdsci-lake/…, encrypted: true}}` + R2 secret + `ducklake`/`httpfs`
   extensions. Get one throwaway `MODEL (kind VIEW)` to `sqlmesh plan` and
   `sqlmesh apply`. Confirm both the physical table and the `__env` view land in
   the DuckLake catalog and are queryable. **If this fails, the whole bet fails.**
2. **P0 — State + catalog in one Postgres.** `state_connection: postgres`,
   `state_schema: sqlmesh`, same DB as the DuckLake catalog. Run two `plan`
   cycles; confirm the `sqlmesh` schema and `ducklake_*` metadata coexist with no
   collision and DuckLake's own snapshots are untouched.
3. **P1 — Headless column lineage.** From Python: build `Context`, declare the 12
   upstream Parquet as external models, port 2–3 real views (`src_ → stg_ →
   sradb.study`), call `column_dependencies` and confirm it returns per-column
   edges (not `*`). Write edges to a scratch table — proves the ADR-0014 feed.
4. **P1 — `data_path` on stock SQLMesh (no fork).** Confirm `data_path: r2://…`
   is accepted by stock `DuckDBAttachOptions`; resolve the reference repo's stale
   "custom fork" reference.
5. **P2 — `create_external_models` on `read_parquet(url)`.** Does it introspect a
   `read_parquet('https://…')` source, or must the 12 be hand-written?
6. **P2 — Publish hook vs Prefect step.** Prototype an `after_all`/`on_virtual_update`
   `COPY … TO 'r2://…' (FORMAT PARQUET)` to feel the ergonomics, then decide
   hook-vs-Prefect for the `latest/` + `v{date}/` publish.

---

## What could kill or reshape the plan

- **KILL — SQLMesh can't create/manage tables inside a DuckLake-attached catalog
  at our version.** DuckLake mediates all writes; if SQLMesh's DDL assumptions
  (temp tables, `CREATE TABLE AS`, view swaps, clustering hints) clash with what
  DuckLake permits, physical materialization breaks. The reference repo de-risks
  this but does not eliminate it for our catalog/DuckLake version. Spike #1 is the
  gate.
- **RESHAPE — the marts read *published* Parquet, not lake tables.** Current
  `src_*` views read `read_parquet('…/latest/*.parquet')` (the reverse-ETL
  output), not the DuckLake `lake.omicidx.*` tables. SQLMesh's lineage/environment
  value is highest when models read *catalog tables* it can fingerprint. Keeping
  the read-from-published-Parquet pattern makes the upstream 12 pure external
  models (fine for lineage) but forfeits SQLMesh's blue-green benefit on the
  source side. Decide deliberately.
- **RESHAPE — dual writers on the same DuckLake tables.** Prefect `ducklake-load`
  MERGE writes raw→lake; SQLMesh must NOT manage those tables (only the derived
  layer). Clear ownership boundary or SQLMesh fingerprinting fights the MERGE flow.
- **RESHAPE — `SELECT *` lineage holes.** If external-model declaration is skipped,
  ADR-0014's column contract ships with `*` nodes at every source boundary —
  defeating the whole "lineage for free" rationale. Non-negotiable to declare
  sources.
- **DEGRADE — the "custom fork" trap.** If a spike accidentally validates against
  the reference repo's forked SQLMesh instead of a stock pinned release, the whole
  assessment is built on a non-shippable dependency. Pin stock; verify.
- **RESHAPE — publish semantics stay in Prefect.** SQLMesh does not model
  immutable dated R2 snapshots for anonymous HTTPS consumers; that (ADR-0004
  contract) remains a Prefect-owned COPY step. SQLMesh replaces the *transform*
  runner, not the *publisher*.
