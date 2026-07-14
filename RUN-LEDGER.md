# RUN-LEDGER — omicidx Stage A

Append-only. One block per work unit per round. Builder writes; reviewers
are cited, not authors.

Template:

```
## Work unit: <A1|A2|A3|A4> — round <n>
### Changed / Tests / Persona findings / Resolution / Dissents / Gate events
```

---

## Work unit: A1 — round 1

Spec: §2 "Source extractor" deep module; §4 A1 ("Extract the narrow Source
protocol; refactor the five flows behind it"); §5 (precedent-setting).

### Changed
- `src/omicidx/prefect/source.py` (new) — the `Source` Protocol
  (`name`, `list_partitions(force) -> list[str]`, `extract: Task` with
  `(key, force) -> dict`) + generic `run_extraction(source, force)` driver that
  replaces the hand-rolled `list -> gate -> submit -> collect` loop in every
  flow. Driver sees only opaque string keys — no source cursor/format leaks up.
- `flows/sra.py` — added `SraSource`; replaced `extract_mirror_file` +
  `get_mirror_listing` + the bespoke flow loop with `extract_sra_partition(key,
  force)` and a `run_extraction(SraSource())` one-liner. Composite key
  `{entity}/{date}_{stage}` keeps the per-entity semaphore layout unchanged; a
  new `@lru_cache`d `_mirror_index()` resolves key→mirror-entry so extract needs
  no URL arg (threads share the cache → one glob/run).
- `flows/geo.py` — added `GeoSource`; flow body → `run_extraction`. `extract_month`
  gate now always re-runs the current month (was flow-level `force_this`),
  keeping the volatility as source-internal knowledge.
- `flows/ebi_biosample.py` — added `EbiBiosampleSource`; same current-day
  always-rerun move into `extract_ebi_biosample`; flow → `run_extraction`
  (consolidate step unchanged, still called after).
- `flows/biosample.py` — added `_DailyDumpSource` + `BiosampleSource`/
  `BioprojectSource`; `extract_biosample`/`extract_bioproject` now take
  `(key, force)`; flows → `run_extraction` (bioproject_to_parquet unchanged).
- `flows/pubmed.py` — added `PubmedSource`; `_list_pubmed_files` now
  `@lru_cache`d; `extract_pubmed_file` dropped its `url` arg and resolves it from
  the cached listing (process pool → each worker lists once).
- `tests/test_flows.py` — `test_real_sources_conform_to_protocol` (all six
  source classes `isinstance(Source)` with a submittable task) +
  `test_run_extraction_drives_every_key_with_force` (driver lists→extracts each
  key, threads `force` to both `list_partitions` and `extract`).

### Decisions / deviations
- Spec writes `list_partitions() -> keys` and `extract(key, force)`. I added an
  optional `force` to `list_partitions` so the source owns force-backfill
  without leaking its cursor. Faithful realization of "the source owns the
  cursor", not a silent patch. Recorded here for the skeptic/ousterhout personas.
- Gating authority moved to `list_partitions` (the source's cursor). `extract`
  keeps a self-gate (`skip if semaphore exists unless force / volatile`) as a
  belt-and-suspenders for direct/duplicate invocation. Volatile "current"
  partitions (GEO month, EBI day) are handled inside each `extract`, not by the
  driver — driver stays uniform.
- Prefect binds `self` (`__prefect_self__`) when a `@task` is a plain class
  attribute, breaking `(key, force)`. Fixed with `extract = staticmethod(task)`;
  verified via the driver test.

### Tests
- `uv run pytest tests/` — 7 passed (5 pre-existing + 2 new). Proves: protocol
  conformance of all six sources; driver fans out one extract per pending key and
  threads `force`.
- `uv run ruff check` on all changed files — clean.

### Persona findings this round (all four PASS)
- skeptic: PASS — every "preserves/reuses" claim grounded (SRA semaphore layout
  byte-identical; current-partition re-run preserved; lru_cache/force claims
  test-backed). Non-blocking note: lru_cache "listed once per process" means a
  long-lived process would serve a stale listing across runs.
- ousterhout: PASS — `Source` is a deep boundary (driver sees only opaque keys,
  no source-type switches; added `force` and in-source volatility are faithful
  narrow realizations; loader raw-layout leak correctly out of A1 scope). Nit:
  volatility encoded in two paired sites per source (`always=[current]` +
  `key != today`) — worth a comment.
- operability: PASS, no CRITICAL, no gate violation. Confirmed worker is
  `--type process` (Dockerfile:23 → fresh subprocess per run), so the lru_cache
  is correct today and only a *latent* trap under `.serve()`. ORDINARY: PubMed
  re-list KeyError window (same net outcome as the old 404 — task fails,
  retries, no data loss); first-failure propagation in `[f.result() ...]`
  (harmless, Prefect tracks per-task state).
- bioinformatician: PASS — behavior preserved, CLI intact. Friction: (F1)
  "partition" means 5 granularities, no operator-facing table; (F2) SRA
  composite `{entity}/{date}_{stage}` appears in task names but isn't directly
  typeable into `semaphores clear`; (F3) `--force` has no CLI `help=`
  (pre-existing).

### Resolution
- lru_cache cross-run staleness (skeptic + operability + bioinfo F5) → FIXED:
  `_mirror_index.cache_clear()` / `_list_pubmed_files.cache_clear()` at flow
  entry (sra.py `sra_extract_flow`, pubmed.py `pubmed_extract_flow`). Converts
  the implicit process-per-run dependency into an explicit "fresh per run,
  shared within run" invariant; robust to `.serve()`.
- ousterhout dual-encoding nit → FIXED: paired comments in `extract_month`
  (geo.py) and `extract_ebi_biosample` (ebi_biosample.py) tying the guard to
  `list_partitions`' `always=[current]`.
- bioinfo F2 (SRA composite-key clearability) → FIXED: `SraSource` docstring now
  documents `composite {entity}/{date}_{stage}` → `semaphores clear sra/{entity}
  {date}_{stage}`.
- operability PubMed KeyError window → NO CHANGE: net outcome identical to prior
  behavior (loud task failure + retry, no data loss); a silent-skip would be a
  worse contract. Acknowledged.
- operability first-failure propagation → NO CHANGE: harmless per reviewer;
  Prefect tracks each task's state.

### Dissents carried forward / deferrals (not silently dropped)
- bioinfo F1 (per-source "partition granularity" table) → DEFERRED to A4:
  DUCKLAKE.md is edited in A4; the granularity table lands there rather than
  bloating A1's code diff.
- bioinfo F3 (`--force` CLI `help=`) → DEFERRED: pre-existing; cli.py untouched
  by A1. The `--force` "current always re-runs" nuance will be documented in the
  A4 DUCKLAKE.md note; CLI help text is a docs-stage (D) concern.
- ousterhout: `Source` is really `PrefectSource` (extract typed as `Task`,
  `.submit()` in the driver) — accepted as an honest coupling to the one
  substrate (package IS omicidx-prefect; sibling producers are Prefect too). Not
  overruled, recorded as a known intentional coupling; YAGNI on substrate
  independence.

### Gate events
- none (no R2 write, no CLAUDE.md, no Worker/creds, no push, no SQLMesh; the
  extract refactor is pure-local code).

---

## Work unit: A2 — round 1

Spec: §1 ("Time travel = extended internal-lake snapshot retention"; raw is the
re-derivation backstop); §4 A2 ("Extend internal-lake snapshot retention (config
+ ducklake-maintenance policy); document raw Parquet as the re-derivation
backstop"). RUN-SCOPE hard gate #3: extend-only; HALT if any path shortens.

### Changed
- `flows/ducklake.py` `ducklake_maintenance` — default `expire_older_than`
  `"now() - INTERVAL 30 DAY"` → `None`; the `ducklake_expire_snapshots` call is
  now guarded by `if expire_older_than is not None`. Cleanup + compaction still
  run (they never drop a snapshot-pinned file). Result dict + log gained
  `expired_older_than`. Docstring rewritten: unbounded-by-default, raw backstop,
  and an explicit "this task can only extend retention, never shorten it."
- `flows/ducklake_load.py` `ducklake_maintenance_flow` — default
  `expire_older_than` → `None`; docstring documents unbounded default + opt-in
  expiry.
- `DUCKLAKE.md` Maintenance section — rewritten to unbounded-default retention,
  raw-as-backstop, cleanup/compaction safe-without-expiry, expiry opt-in only.

### Gate verification (hard gate #3 — extend only)
- The 30-day window lived ONLY in the two function defaults (grep across .py +
  .yaml). The weekly `ducklake-maintenance` deployment (prefect.yaml:53) passes
  NO `parameters:`, so it uses defaults. Changing defaults 30d → None means the
  scheduled flow stops expiring → strictly extends. No caller passes a shorter
  explicit value.
- `expire_snapshots` is the only call that removes snapshots; it is now skipped
  unless an explicit interval is passed. Default path expires nothing.
- `cleanup_old_files` deletes only files unreferenced by ANY retained snapshot;
  under unbounded retention old snapshots pin their files, so nothing retained is
  dropped. `merge_adjacent_files` rewrites for the current snapshot; old files
  stay pinned by history. No path drops retained data.
- Deliberately did NOT add a new env knob (e.g. SNAPSHOT_RETENTION_DAYS) — that
  would introduce a new env-driven path that COULD shorten. The existing
  `expire_older_than` param (already present) remains the sole opt-in.

### Tests
- `uv run pytest tests/test_flows.py::test_flow_modules_import` — passes
  (signature change imports clean). `ruff check` — clean. No test asserted the
  30-day default.

### Persona findings — round 1
- operability: PASS. Hard gate respected (expiry unreachable by default; cleanup
  + compaction history-preserving; no new knob). ORDINARY: stale prefect.yaml
  prose still says "expire snapshots."
- ousterhout: PASS. Coherent policy change; the `@flow` wrapper is a justified
  deployment entrypoint. Nit: `expire_older_than: str` (raw SQL interval) is an
  injection sink + dialect leak — a typed `days: int | None` would be narrower.
- skeptic: FAIL. (F7) docstring "this task can only extend retention, never
  shorten it" contradicts the guarded code (an explicit interval DOES shorten) —
  conflates default path with task capability. (F3) prefect.yaml:11 + deployment
  description still advertise "expire snapshots." (F4, note) cleanup/compaction
  safety is a DuckLake-external contract.
- bioinformatician: FAIL. Maintenance-safety story clear, but (F1/F3) "primary
  time machine" with no retrieval recipe; (F2) "from-source re-run reproduces
  lake state" misleads (implies re-fetch from NCBI); (F4) no internal-only
  caveat; (F5) "copy-on-write" unglossed.

### Resolution (round-1 findings)
- ousterhout nit (typed retention) → ADOPTED: `expire_older_than: str` →
  `retention_days: int | None`; expiry built as `now() - INTERVAL
  {int(retention_days)} DAY`. Closes the injection sink and the dialect leak.
- skeptic F7 → FIXED: docstring reworded to distinguish the default path
  (extend-only) from the task's opt-in capability to shorten.
- skeptic F3 + operability ORDINARY → FIXED: prefect.yaml:11 comment and the
  deployment description rewritten (cleanup + compact; unbounded; no expiry
  unless `retention_days` passed).
- bioinfo F1/F3 → FIXED: added a "Reading history (time travel)" recipe to
  DUCKLAKE.md (`lake.snapshots()` → `AT (VERSION => n)` / `AT (TIMESTAMP => ...)`
  + a materialize example). F2 → FIXED (retained-raw wording). F4 → FIXED
  (internal-only caveat blockquote). F5 → FIXED (copy-on-write glossed).
- skeptic F4 (external DuckLake contract) → NOTED, not code-fixable; the A3
  local smoke test empirically grounds the upsert-idempotency half of it.

### Grounding note (skeptic round-2 request)
- The DUCKLAKE.md `AT (VERSION => n)` and `AT (TIMESTAMP => TIMESTAMP '...')`
  examples were verified against a LOCAL ephemeral DuckLake (no R2) before being
  written: `AT (VERSION => old_snapshot)` returned the pre-change value and
  `AT (TIMESTAMP => ...)` returned rows. `lake.snapshots()` and
  `lake.omicidx.sra_study` are pre-existing real repo surfaces.

### Persona findings — round 2 (re-review of the three that had blocking/nit items)
- operability: PASS. Re-traced every caller under the typed `retention_days`;
  default path still never expires; `int()` closes injection; no HALT.
- skeptic: PASS. F7 + F3 resolved; new time-travel example internally consistent
  (`snapshots()` + table name real); `AT` syntax external-unverifiable but
  builder-verified locally (recorded above).
- bioinformatician: PASS. Retrieval recipe + wording now serve the end-user.
  Minor non-blocking nits (materialize line; snapshot_id==VERSION) → both added
  to the doc.
- ousterhout: not re-dispatched — its sole nit (typed retention param) was
  adopted verbatim; round-1 PASS stands.

### Dissents carried forward
- none. All findings fixed or (skeptic F4) noted as an external-library contract
  with the upsert half grounded by A3.

### Gate events
- none. Hard gate #3 (extend-only) verified by operability across both rounds:
  default/scheduled path never expires; the only shortening path is explicit
  operator opt-in via `retention_days`, which mirrors the pre-existing param.
  No R2/Worker/creds/push/SQLMesh touched.

---

## Work unit: A3 — round 1

Spec: §1 ("Reproducible = re-running extraction + load from retained raw
reproduces the lake tables (idempotent upsert makes re-runs no-ops). Acceptance:
a smoke test proving a re-run writes zero new data files"); §4 A3. RUN-SCOPE
hard gate #1: no R2 writes — the smoke test runs against a LOCAL ephemeral
DuckLake, never cdsci-lake.

### Changed
- `flows/ducklake_load.py` `ducklake_load_flow` — added `force: bool = False`;
  threads `force` to the four SRA loaders (the only high-water-mark incremental
  ones). `force=True` is reproduce-from-raw: SRA drops its `date >= watermark`
  filter and re-scans all retained raw; full-snapshot loaders already read all
  raw each run. Docstring documents the mode + idempotency.
- `cli.py` — new `omicidx-prefect run reproduce-from-raw [--lake-schema]`
  entrypoint → `ducklake_load_flow(force=True)`. Named/discoverable, distinct
  from the daily incremental `ducklake-load`.
- `tests/test_idempotency.py` (new) — the acceptance smoke test. Attaches a
  LOCAL file-catalog DuckLake (catalog file + local data dir, no R2), calls the
  REAL cdsci-lake `upsert` (the primitive every loader uses):
  - `test_rerun_writes_zero_new_data_files`: load a keyed fixture, re-run the
    identical source, assert the on-disk parquet count is unchanged AND no new
    snapshot — the spec's "zero new data files" acceptance.
  - `test_real_change_commits_a_snapshot`: a changed row commits a new snapshot
    (guards against a trivial "never writes"); the value updates. Uses snapshot
    count, not file count, because a tiny table's UPDATE rewrites its single
    file in place (verified empirically).

### Design note (reproduce-from-raw entrypoint)
- Reproduce-from-raw = `ducklake_load_flow(force=True)`, not a duplicate flow.
  Full-snapshot loaders (geo/biosample/bioproject/pubmed/ebi) are already
  reproduce-from-raw every run (no watermark). Only SRA is incremental, so
  `force` (which the SRA loaders already accepted) is the whole delta. The CLI
  command names the operation.
- Idempotency is proven at the `upsert` layer (shared by every loader) rather
  than by running the flow, because running the flow writes to cdsci-lake R2
  (hard gate #1). The local DuckLake exercises the exact same `upsert` code path
  and the same copy-on-write + IS DISTINCT FROM semantics.

### Tests (smoke-test output)
```
tests/test_idempotency.py::test_rerun_writes_zero_new_data_files PASSED  [ 50%]
tests/test_idempotency.py::test_real_change_commits_a_snapshot    PASSED [100%]
2 passed
```
Full suite: `uv run pytest tests/` — 9 passed. `ruff check` — clean.

### Persona findings — round 1
- operability: PASS, no CRITICAL. Idempotency test real/non-circular (real
  `upsert`, real on-disk file measurement); gate #1 respected (local DuckLake,
  CLI command defined-not-executed); `force=True` genuinely drops the SRA
  watermark filter; no daily-pipeline regression. ORDINARY: offline
  `pytest.skip` could let a green run mask the acceptance test being skipped in
  a network-less CI.
- skeptic: PASS. All six primary claims grounded (real upsert same as prod path;
  local-only no R2; asserts on-disk file equality; force drops watermark;
  full-snapshot loaders need no force; CLI = `ducklake_load_flow(force=True)`).
  Soft spot: the test comment's "rewrites its single file in place" is an
  external-unverifiable mechanism claim (non-load-bearing; snapshot count is the
  asserted signal).
- ousterhout: PASS. `force`-flag reuse (not a duplicate flow) is correct/lazy;
  CLI is a thin honest entrypoint. Real minor finding: the test docstring claims
  the guarantee rests on "one property" of `upsert`, but it rests on TWO —
  upsert idempotency (tested) + per-loader source-SELECT determinism (untested).
- bioinformatician: FAIL. F1 (MAJOR): `reproduce-from-raw` maps to the "want my
  old data back" instinct but rebuilds CURRENT state, and its `--help` had no
  pointer to time travel. F2: `ducklake-load`/`daily` had no docstrings, so the
  trio wasn't comparable via `--help`. F3: help leaked "high-water-marks
  bypassed (force=True)" jargon. F4: test uses a toy source and never runs the
  flow — worth a note so coverage isn't over-read.

### Resolution (round-1 findings)
- bioinfo F1 → FIXED: `reproduce-from-raw` docstring now says it rebuilds the
  CURRENT state (not a prior day) and points to snapshot time travel
  (`AT (VERSION => n)`, DUCKLAKE.md) for reading old state.
- bioinfo F2 → FIXED: added docstrings to `run_ducklake_load` (daily
  incremental; SRA by watermark) and `run_daily` (full pipeline).
- bioinfo F3 → FIXED: operator help no longer mentions high-water-marks/force;
  that detail stays in the flow docstring.
- ousterhout two-property + bioinfo F4 + skeptic mechanism → FIXED in the test
  module/change-test docstrings: scoped to "proves upsert idempotency (1);
  assumes-but-does-not-test per-loader source determinism (2)"; noted it
  exercises the shared primitive with a toy source, not the flow; softened the
  file-in-place wording to the observed fact (file count didn't change locally).
- operability offline-skip ORDINARY → ACKNOWLEDGED, no code change: the skip is
  intentional portability; CI that must attest the acceptance should ensure the
  ducklake extension is available (network) rather than rely on the skip.

### Persona findings — round 2
- bioinformatician: PASS. F1/F2/F4 resolved; F3 mostly resolved with one MINOR
  jargon residue ("IS DISTINCT FROM gate + copy-on-write") in the CLI help →
  FIXED (cut to "an unchanged re-run writes zero new data files (proven in
  tests/test_idempotency.py)").
- operability/skeptic/ousterhout: not re-dispatched — all PASSed round 1; their
  nits were addressed with docstring-only softenings (no new failure surface).

### Gate events
- none. No R2 write (smoke test local-only; CLI command defined, never run);
  no CLAUDE.md/Worker/creds/push/SQLMesh.

---

## Work unit: A4 — round 1

Spec: §4 A4 ("Fix stale `DUCKLAKE.md` (`_row_hash`/MERGE → `upsert`/`IS DISTINCT
FROM`)"). Also folds in the A1-deferred bioinfo F1 (per-source partition
granularity table).

### Changed (all in `packages/omicidx-prefect/DUCKLAKE.md`)
- "Merge strategy" → "Upsert strategy": removed the `_row_hash = md5(to_json(
  ...))` change-gate description and the `WHEN MATCHED AND tgt._row_hash <>
  src._row_hash` shape; replaced with cdsci-lake `upsert`'s column-wise
  `IS DISTINCT FROM` gate (no hash column), the actual MERGE shape it builds,
  and `exclude_change_cols` for per-load stamps. Incremental/full-snapshot table
  reworded (MERGE→upsert, hash-gated→IS-DISTINCT-FROM-gated).
- Watermark storage line: was "stored as semaphore files under namespace
  `ducklake/<entity>` (key `latest`)" — STALE/WRONG. Now: lake ops-ledger (`ops`
  schema `watermark` table) via `ops.get_watermark`/`set_watermark`, keyed
  source `sra`, name `<lake_schema>:<entity>`; backfill = `force=True`
  (reproduce-from-raw). Verified against `ducklake_sra.py:155,177` +
  `cdsci/lake/ops.py:429,438`.
- "Commit metadata": was "Stamp every write" with a manual `BEGIN; set_commit_
  message; MERGE; COMMIT`. Now: primary path attributes via `ops.run(...)`
  automatically; manual `_stamped_txn` + `CREATE OR REPLACE TABLE` is the
  transform-layer (`flows/_parked/`) path only.
- SQL gotcha: "a `LIMIT` view is re-evaluated per MERGE pass" (MERGE-era) →
  `upsert` materializes the source once, so the real risk is a `LIMIT` without
  stable order returning different rows *across runs*, failing the re-run
  idempotency check.
- New "Raw extraction partitions" table (closes A1-deferred bioinfo F1):
  per-source raw partition granularity + semaphore namespace/key, generalizing
  the A1 SRA-clearability note.

### Tests
- Doc-only change; no code. `grep -i _row_hash/merge_to_ducklake/'stored as
  semaphore'` on DUCKLAKE.md → clean (the one remaining `_row_hash` is the new
  "there is **no** `_row_hash` column" statement).

### Persona findings — round 1 (all four PASS)
- skeptic: PASS — every new factual assertion grounded against the code (upsert
  IS DISTINCT FROM gate + no `_row_hash`; `exclude_change_cols`; watermark in the
  ops ledger; `ops.run` attribution; `_stamped_txn`/CREATE OR REPLACE for the
  parked transform layer; the extraction-partition table). Nit: "`ops` schema" is
  imprecise — `ops` is the attach alias; the qualified path is
  `ops.lake_ops.watermark`.
- ousterhout: PASS — doc presents `upsert` as the narrow interface with
  complexity hidden; the raw-partition table is correctly framed as operator
  reference, consistent with the A1 `Source` boundary; no internal
  contradictions. Nit: the transform helpers live in `ducklake.py`, not
  `_parked/` (parked loaders only call them).
- operability: PASS — operationally accurate; backfill=`force=True` matches the
  code; no-op-no-snapshot consistent with A3; no destructive instruction; no
  stale 30-day/expiry text reintroduced (aligned with A2 unbounded default).
- bioinformatician: PASS — coherent, current description. F1 (moderate): the
  "namespace / key" separator collided with SRA's own slash. F2 (moderate):
  BioSample/BioProject row read as one namespace+key, not two namespaces. F3
  (minor): upsert/MERGE used interchangeably without stating equivalence. F4
  (minor): transform path via `_parked/` not labelled dormant.

### Resolution
- bioinfo F1 + F2 → FIXED: partition table split into `Semaphore namespace` /
  `Key` columns (no more " / " ambiguity), BioSample and BioProject as separate
  rows, plus a worked `semaphores clear sra/study 2026-01-01_Full` example noting
  SRA's embedded slash.
- bioinfo F3 → FIXED: "`upsert` builds a DuckDB `MERGE` under the hood" added to
  the intro.
- bioinfo F4 + ousterhout nit → FIXED: transform layer labelled **dormant**;
  helpers noted as "defined in `ducklake.py`" (parked loaders call them).
- skeptic nit → FIXED: "`ops` schema `watermark` table" → `ops.lake_ops.watermark`
  (verified: `_t()` builds `ops.lake_ops.<table>` in `cdsci/lake/ops.py:54-56`).

### Dissents carried forward
- none.

### Gate events
- none. DUCKLAKE.md is not CLAUDE.md; doc-only change, no
  R2/Worker/creds/push/SQLMesh. (A1-deferred bioinfo F1 partition table now
  landed here as promised.)
