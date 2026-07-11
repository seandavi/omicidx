# OmicIDX deliverables spec

Durable statement of intent. Endpoints are fixed; current code is the other
boundary. Written backward from the finished products, then re-ordered into a
valid forward build sequence. Plain-text canonical — inspect and diff this file,
not a rendered view.

Status: draft, 2026-07-11. Supersedes ad-hoc intent scattered across ADRs and
handoff notes; ADRs remain the record for individual decisions (0001–0005).

---

## 1. Deliverables as endpoints

Dependency structure (not a flat list):

```
Steel thread:   (1) internal lake ──► (2) external frozen artifact
Derived:                                       └─► (3) marts ──► (4) docs site
Optional/gated:                                          (5) R/Python client
                                                         (6) performant API
```

Build order is forced: 1 → 2 → 3 → 4; 5 and 6 are gated behind explicit opt-in.

### (1) Internal reproducible, time-travel-capable pipeline — STEEL THREAD

Five sources (SRA, BioSample, BioProject, GEO, PubMed) → raw extraction →
internal DuckLake.

- Substrate: the shared **cdsci-lake** Postgres catalog (`lake` DB), `omicidx`
  schema, R2 data at `r2://cdsci-lake/`. Write path is cdsci-lake's
  `lake_connect` + `upsert` (`IS DISTINCT FROM` change gate, copy-on-write) +
  `ops.run` run-ledger + watermarks (ADR-0005, already adopted).
- **Time travel = extended internal-lake snapshot retention.** The lake is the
  time machine, not raw. Rationale (owner): MERGE deltas are small — for several
  sources the raw inputs are *larger* than the lake — so long/unbounded snapshot
  retention is cheap. Raw Parquet is retained as the **re-derivation backstop**:
  a from-source re-run reproduces lake state, but is not the primary query path.
- Reproducible = re-running extraction + load from retained raw reproduces the
  lake tables (idempotent upsert makes re-runs no-ops). Acceptance: a smoke test
  proving a re-run writes zero new data files.

### (2) External daily frozen artifact — STEEL THREAD

A daily publication bundle, `omicidx` tables only. Read path is anonymous plain
HTTPS (no `--no-sign-request` R2, no client credentials, no signed URLs) via the
Cloudflare Worker / custom domain.

The external artifact is built on a **retaining, file-catalog DuckLake** from day
one, but ships in two capability tiers:

- **v1 (current-state):** the bundle exposes the current snapshot only. Honest
  promise: "the data as of today." This is all the pipeline can *attest* today
  (see §2a on faithfulness), so it is all v1 promises.
- **B′ (time-travel unlock):** the same file-catalog DuckLake retains N
  snapshots and exposes `AS OF` queries. This is the differentiated artifact —
  **no one else publishes queryable historical SRA/GEO/BioProject state** — and
  it is gated behind the faithfulness machinery (§2a), not behind a v1 deadline.

Building the catalog as *retaining* from the start (rather than exporting a
single snapshot) costs little now and avoids a format migration later: it leaves
the killer-app door open cheaply. Retention (which snapshots persist, how long)
is a policy dial, defaulted conservatively at v1 and opened at B′.

Each bundle contains:

- Parquet copies of the omicidx lake tables. Under copy-on-write, snapshots share
  unchanged data files, so retaining history costs *retained files + growing
  catalog metadata*, not a full copy per day — the same economics that make
  internal retention cheap (§1).
- `omicidx.duckdb` — built from those Parquet files, including the marts (§3).
- `views.sql` — co-published view definitions (ADR-0004).
- A **read-only file-based DuckLake catalog** (SQLite or DuckDB file, **not**
  Postgres) so anonymous, credential-free clients can `ATTACH` the frozen
  snapshot — and, at B′, query it `AS OF` a past date — without touching the
  internal Postgres catalog.
- A **bundle manifest** (§2a) pinning the publish to its provenance. This is the
  enabling primitive for trustworthy time travel, not a nice-to-have.
- Read path: **Cloudflare Worker / custom-domain over plain HTTPS** with range
  requests for remote DuckDB reads.
- Retention: rolling `latest/` **plus** immutable dated `v{date}/` folders. At
  v1, dated folders are a bounded window (yesterday stays readable). At B′, the
  file catalog itself retains the snapshot lineage, making `v{date}/` the
  physical backing for `AS OF`.

### (2a) Snapshot faithfulness — the gate on time travel

Time travel is only *valuable* if a retained snapshot is a *faithful* record of
the state on its date. A plausible-but-unattested historical answer is worse than
no answer, because users trust the `AS OF` result. Today the pipeline cannot
attest faithfulness: transforms are a numbered-SQL runner with no lineage,
publishes overwrite `latest/`, and nothing pins a snapshot to its inputs.

The **bundle manifest** closes this. Every publish records, at minimum:

- the raw partition set consumed (which source partitions were live),
- the transform version (git SHA of the SQL / SQLMesh models that built the marts),
- the internal lake snapshot id the publish was cut from,
- the publish timestamp and the `v{date}/` path.

Once every published snapshot is manifest-attested, retained snapshots become
trustworthy and B′ becomes shippable. The manifest also makes the *internal*
reproduce-from-raw claim (§1) real rather than aspirational — so it pays for
itself on both sides of the steel thread.

**Decision bound to this:** the transform engine + lineage choice (SQLMesh vs the
numbered-SQL runner) is *not* a Stage-C implementation detail. Lineage is what
makes a historical snapshot attestable ("this mart column on that date derived
from these sources via this transform"). SQLMesh-or-equivalent is therefore on
the **critical path to external time travel**. If the killer app matters,
lineage is required infrastructure; if time travel stays deferred, lineage can
stay deferred. The killer app decides it (§5).

### (3) Marts approximating legacy SRAdb + GEOmetadb — DERIVED

- Fidelity: **rough schema resemblance, modernized/adapted** to the current
  data. Recognizable table/column names from legacy SRAdb/GEOmetadb, but adapted
  where the modern data warrants — not an exact drop-in, not merely a query
  corpus.
- Lives in the `sradb.*` / `geometadb.*` view namespaces already scaffolded in
  `sql/040_geometadb_views.sql` and `sql/050_sradb_views.sql`, materialized into
  the published `omicidx.duckdb`.
- Acceptance: schema-resemblance check against the legacy schemas + a documented
  adaptation/compatibility note (what maps, what changed, what was dropped).

### (4) Public website with Diátaxis docs — DERIVED

- The existing Astro/Starlight site under `docs/`, filled out along Diátaxis
  axes (tutorial / how-to / reference / explanation).
- Must document the real access story: how to read the frozen artifact, the
  `views.sql` contract, the mart schemas, and — at B′ — how to run `AS OF`
  historical queries.

### (5) R/Python client package — OPTIONAL, GATED

Thin client over the published `omicidx.duckdb` / Parquet. Gate: (2) and (3)
stable. Greenfield.

### (6) Performant API — OPTIONAL, GATED

FastAPI over DuckDB or Postgres/ClickHouse. Gate: explicit request + a backend
decision. `omicidx-api` (read-only FastAPI, Postgres-backed) is the existing
base to build on, not a rewrite.

---

## 2. Design philosophy — boundaries as deep modules

Ousterhout: a module is deep when a narrow interface hides substantial
complexity. Each boundary below is justified on that basis (what it hides vs how
narrow its interface is).

- **Source extractor (one deep module per source).** Hides: NCBI/EBI crawl
  quirks, mirror-file Full/Incremental detection (SRA), per-entity XML→row
  schemas, output format, partitioning scheme, and the incrementality cursor.
  Narrow interface: `list_partitions() -> keys` and `extract(key, force)`. The
  interface is narrow relative to the large, per-source crawl complexity it
  buries. This is precedent-setting (§4).

- **cdsci-lake write path (externally owned deep module).** Hides: MERGE change-
  gating, copy-on-write snapshotting, the run-ledger, watermark storage, and
  snapshot attribution. Narrow interface: `lake_connect`, `upsert(con, target,
  source_sql, key)`, `ops.run(...)`, `get/set_watermark`. omicidx *consumes*
  this. Where omicidx's needs should drive cdsci-lake's interface: (a) the
  frozen file-catalog export for external publishing, (b) the transform-engine
  slot (SQLMesh) and lineage — both are shared-substrate concerns omicidx is
  first to hit.

- **Frozen publisher (deep module).** Hides: which tables ship, snapshot copy,
  `omicidx.duckdb` build, retaining file-catalog generation, manifest emission,
  dated-folder retention, and the Worker path layout. Narrow interface:
  `publish(date) -> attested bundle at latest/ and v{date}/`.

- **Public read surface / Worker (deep module).** Hides: R2 auth, HTTP range
  requests, directory listing, CORS, custom-domain routing. Narrow interface:
  anonymous HTTPS GET by URL.

- **Mart layer (deep module, SQL views 020–050).** Hides: reconstructing the
  legacy SRAdb/GEOmetadb shapes from modern lake tables. Narrow interface: the
  `sradb.*` / `geometadb.*` view names + `views.sql`.

Information-leakage watch: the extractor protocol must not leak format or
partitioning upward (today it does — downstream loaders know each source's
layout). The frozen publisher must not leak the internal Postgres catalog to
external clients (the file catalog exists precisely to prevent this).

---

## 3. Gap analysis (current code vs each endpoint)

Grounded in the Phase-0 inspection, not assumptions. Live path is
`packages/omicidx-prefect`; `omicidx-dagster` and `omicidx-etl` are superseded.

### Endpoint (1) internal pipeline

- **Reusable:** all five extractors (live, working); `SemaphoreStore` crawl
  gating (`semaphore.py`); cdsci-lake write path via `get_lake_connection`
  (`config.py:250`) + per-entity `ducklake_*.py` loaders; SRA high-water-mark
  (`ducklake_sra.py:139-187`); `ops.run` run-ledger.
- **Needs reshaping:** five bespoke flows → the narrow Source protocol (they
  already duplicate a `list → gate → write → mark` template by hand); snapshot
  retention config (currently 30-day expiry in the weekly `ducklake-maintenance`
  flow → extend to long/unbounded); stale `DUCKLAKE.md` (still documents the
  retired `_row_hash`/MERGE gate).
- **Missing:** the Source protocol itself; the long-retention maintenance
  policy; an explicit reproduce-from-raw entrypoint + the idempotency smoke
  test.

### Endpoint (2) external frozen artifact

- **Reusable:** `parquet_export` COPY to `latest/` (`parquet_export.py:29-55`);
  `get_public_parquet_path`; `duckdb-build` (`sql.py:60-114`); the Cloudflare
  Worker pattern (`omicidx-etl/worker/`, built for the legacy bucket); ADR-0004.
- **Needs reshaping:** `parquet_export` is `latest/`-only, overwritten in place →
  add immutable dated `v{date}/` + retention pruning; `duckdb-build` uploads to
  `PUBLISH_ROOT`, not the public bucket → redirect into the published bundle; the
  Worker is wired to the legacy `omicidx` bucket, not `data-omicidx`.
- **Missing:** retaining file-catalog generation (read-only file-based DuckLake
  catalog carrying snapshot lineage); the bundle manifest (§2a — the primitive
  that makes time travel attestable); dated-folder retention/pruning;
  `data-omicidx` Worker/custom-domain config committed to the repo (only the
  HTTPS base string exists today).
- **Key risk to retire early:** anonymous, credential-free `ATTACH` of a
  file-based DuckLake catalog over plain HTTPS range requests through the Worker
  is the load-bearing assumption for the whole external artifact — and for the
  B′ killer app. Sharp edges: SQLite/DuckDB catalog-file read patterns over
  range requests, relative vs absolute data-file paths recorded in the catalog,
  and CORS on the range GETs. Verify end-to-end at B3 **before** building
  retention on top of it. If it fails, the artifact degrades gracefully to
  "current-state Parquet + duckdb" and the time-travel bet is off — but you learn
  that at B3, not at B′.

### Endpoint (3) marts

- **Reusable:** `sql/040_geometadb_views.sql`, `sql/050_sradb_views.sql`
  namespaces already scaffolded; the `sqlglot` SQL-file runner (`sql.py`).
- **Needs reshaping:** audit current views against the legacy SRAdb/GEOmetadb
  schemas; modernize/adapt.
- **Missing:** the two parked derived loaders (`publication_accession_linkage`,
  `geo_series_with_rnaseq_counts` under `flows/_parked/`) need un-parking; the
  schema-resemblance acceptance tests; the adaptation/compatibility note.

### Endpoint (4) docs

- **Reusable:** Astro/Starlight site exists with `overview/`, `api/`,
  `contributing/`, `guides/`.
- **Needs reshaping:** docs describe Dagster as orchestrator (stale) → Prefect.
- **Missing:** Diátaxis tutorial + how-to for reading the frozen artifact; mart
  reference; the versioning/frozen-artifact explanation; (at B′) the `AS OF`
  historical-query how-to.

### Endpoints (5)/(6)

- (5): greenfield.
- (6): `omicidx-api` (Postgres-backed FastAPI) is the base; backend choice
  (DuckDB vs Postgres/ClickHouse) is an open decision deferred until the gate
  opens.

---

## 4. Staged plan (forward execution order)

Derived backward from the endpoints, re-ordered forward to respect path
dependencies. Steel thread (A → B) first, then C, then D; E/F gated. B′ is an
earned unlock, gated behind faithfulness (§2a), not behind a deadline.

### Stage A — solidify the internal steel thread (endpoint 1)

- **A1.** Extract the narrow Source protocol; refactor the five flows behind it.
  *(precedent — §5)*
- **A2.** Extend internal-lake snapshot retention (config + `ducklake-
  maintenance` policy); document raw Parquet as the re-derivation backstop.
- **A3.** Add a reproduce-from-raw entrypoint + an idempotency smoke test
  (re-run writes zero new data files).
- **A4.** Fix stale `DUCKLAKE.md` (`_row_hash`/MERGE → `upsert`/`IS DISTINCT
  FROM`).

### Stage B — external frozen artifact, current-state (endpoint 2; depends on A)

- **B1.** Reshape `parquet-export`: `latest/` + immutable `v{date}/` dated
  folders + retention-window pruning.
- **B2.** Generate the **retaining** file-based DuckLake catalog (built to carry
  snapshots from day one, publishing current-state only at v1); co-publish
  `views.sql`; redirect `duckdb-build` output into the published bundle.
- **B3.** Commit `data-omicidx` Worker/custom-domain config; **verify anonymous,
  credential-free HTTPS range reads + a DuckDB `ATTACH` of the file catalog
  end-to-end.** This is the gate on the whole time-travel bet (§3 key risk) —
  retire it here before B′ builds on it.
- **B4.** Emit the **bundle manifest** (§2a): raw partition set, transform SHA,
  internal snapshot id, publish timestamp, `v{date}/` path. Enables attested
  provenance for every publish from v1 onward.

### Stage B′ — time-travel unlock (endpoint 2 killer app; gated behind §2a)

Gate: (i) B3 proves anonymous file-catalog attach works; (ii) the manifest (B4)
plus lineage (transform engine decision, §5) make each snapshot attestable.

- **B′1.** Retain N snapshots in the file catalog rather than pruning to current;
  set the retention policy dial.
- **B′2.** Expose `AS OF` queries over the published catalog; verify a
  credential-free historical query end-to-end through the Worker.
- **B′3.** Ship the differentiated promise: queryable historical
  SRA/GEO/BioProject state, attested by manifest + lineage.

### Stage C — marts (endpoint 3; depends on B artifact as the read source)

- **C1.** Audit `sradb.*` / `geometadb.*` views vs the legacy schemas;
  modernize/adapt.
- **C2.** Un-park the derived loaders; wire them into the published bundle.
- **C3.** Schema-resemblance acceptance tests + the adaptation/compatibility
  note.
- **Transform-engine / lineage decision (bound to B′, not local to C):** SQLMesh
  is absent today; transforms are a numbered SQL-file runner. Lineage is what
  makes B′ snapshots attestable (§2a), so this decision is on the critical path
  to the killer app, not a Stage-C nicety. Decide, with the killer app as the
  forcing function: (a) if external time travel is wanted, adopt SQLMesh-or-
  equivalent lineage — and decide whether it lives in cdsci-lake (shared) or
  omicidx (local first, promoted later); (b) if time travel stays deferred,
  lineage can stay deferred and the numbered runner persists. Do not drift into
  SQLMesh because a handoff note aspired to it — pick it because B′ needs it.

### Stage D — docs site (endpoint 4; depends on B/C so it documents the real story)

- **D1.** De-Dagster the docs; document the Prefect architecture.
- **D2.** Diátaxis fill: data-access tutorial, mart reference, versioning/frozen-
  artifact guide; (if B′ ships) the `AS OF` historical-query how-to.

### Gated E/F (opt-in only)

- **E.** R/Python client over the DuckDB artifact. Gate: B + C stable.
- **F.** Performant API. Gate: explicit request + backend decision; build on
  `omicidx-api`.

---

## 5. Precedent-setting decisions (what inherits each)

omicidx is the single steel thread whose implementation informs a family of
related projects on the shared cdsci-lake substrate. Each decision below is
inherited by something downstream.

| Decision | Inherited by |
|---|---|
| Narrow Source extraction protocol (`list_partitions` / `extract`) | Every future omicidx source (EBI BioSample already pending); sibling producers (cmgd, bugsigdb) modeling their own extractors |
| External publish = retaining file-catalog DuckLake (Parquet + duckdb + views.sql + read-only **file-based** catalog + manifest), **not** a Postgres-catalog live lake; omicidx-only | The cdsci-lake external-publishing pattern for all producers |
| Cloudflare Worker / custom-domain as the anonymous read surface (no `--no-sign-request`, no client creds); anonymous file-catalog `ATTACH` verified over HTTPS range | All public data serving on the shared substrate |
| Time travel = extended internal-lake snapshot retention; raw is the re-derivation backstop (not the primary query path) | The retention/maintenance convention in `DUCKLAKE.md` for all producers |
| **External time travel = an earned capability gated on snapshot faithfulness (manifest + lineage), not a v1 feature nor a closed door** | Whether any producer may publish `AS OF` history — and the faithfulness bar they must clear to do so |
| **Bundle manifest (raw set + transform SHA + snapshot id) as the attestation primitive** | The provenance contract for every published artifact; the thing that makes reproduce-from-raw and time travel trustworthy |
| Mart fidelity = schema-resemblance, modernized (not drop-in) | The compatibility-promise bar for any future legacy-DB approximation |
| Transform engine slot (SQLMesh) + lineage — **on the critical path to external time travel**, decided by whether the killer app is wanted | Where transforms + lineage live across cdsci-lake |
| `ops.run` run-ledger as the canonical run-logging surface | Already cdsci-lake-shared; omicidx is the reference consumer |
