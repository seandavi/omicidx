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
