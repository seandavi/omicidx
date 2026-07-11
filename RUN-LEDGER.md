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
