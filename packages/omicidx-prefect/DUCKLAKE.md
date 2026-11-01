# DuckLake conventions (OmicIDX)

Standards adopted for landing OmicIDX data into the shared DuckLake
catalog. These emerged while building the `ducklake-load` flow and are
meant to be portable to other projects writing to the same lake (CMGD,
BugSigDB, ontologies — see omicidx#64). The general/cross-project subset
should be mirrored into `monode/infrastructure`.

## Catalog topology

- **Catalog metadata:** Postgres database `lake` (role `postgres`), host
  `pg_main` inside the docker network (`127.0.0.1` from the host),
  port 5432. Metadata tables are `ducklake_*` in the `public` schema.
- **Data files:** `r2://cdsci-lake/`. This bucket is **ducklake-only** —
  no raw extracts, consolidated parquet, or public exports ever land
  here. Raw inputs are read from `PUBLISH_ROOT` (`omicidx-test`) and
  public exports (if any) are written there, never to `cdsci-lake`.
- **Attach name:** `lake`. Connect with `ATTACH 'ducklake:lake'` once the
  `lake` secret exists, then reference `lake.<schema>.<table>`.
- **Schemas:** `omicidx` (production) per omicidx#64; siblings `cmgd`,
  `bugsigdb`, `ontologies`. New work lands in `<schema>_dev` first and is
  promoted to the production schema once validated.

## DuckDB / catalog version

The catalog uses the **1.5.x DuckLake format**. duckdb **>= 1.5.3**
(latest PyPI stable) is required; 1.4.x refuses to attach (`Only DuckLake
versions 0.1, 0.2, 0.3-dev1 and 0.3 are supported`). Pin it in
`pyproject.toml` and keep workers in sync with the catalog version.

## Connection / secrets

`get_ducklake_connection()` (in `config.py`) builds three **TEMPORARY**
secrets from env (`DUCKLAKE_URI`, `DUCKLAKE_DATA_PATH`, R2 creds) and
attaches:

- `r2` (type r2) — data access for `r2://`
- `pg_main` (type postgres) — catalog metadata store (db `lake`)
- `lake` (type ducklake) — `METADATA_PARAMETERS MAP {'TYPE':'postgres','SECRET':'pg_main'}`, `DATA_PATH 'r2://cdsci-lake/'`

Notes:
- The catalog's **stored** `data_path` governs reads/writes on attach;
  the `DATA_PATH` option only matters at first-time init.
- In a fresh worker (no persisted secrets) plain `CREATE OR REPLACE
  SECRET` makes a session secret — fine. On a dev box that *also* has
  persisted `lake`/`pg_main`/`r2` secrets, the two can collide
  (`Ambiguity detected for secret name ...`). For local validation,
  attach via the persisted secret (`ATTACH 'ducklake:lake'`) instead of
  rebuilding them.

## Raw extraction partitions

Each source's raw extract is partitioned differently; the semaphore
namespace/key mirrors the path layout (`omicidx-prefect semaphores list
<ns>` / `... clear <ns> <key>`):

| Source | Raw partition | Semaphore namespace | Key | Notes |
|---|---|---|---|---|
| SRA | `(entity, date, stage)` mirror file | `sra/<entity>` | `<date>_<stage>` | namespace embeds a slash |
| GEO | calendar month | `geo` | `<YYYY-MM>` | re-runs until 1 day after month end; semaphore = final |
| BioSample | full dump per run | `biosample` | `<date>` | |
| BioProject | full dump per run | `bioproject` | `<date>` | |
| EBI BioSample | calendar day | `ebi_biosample` | `<YYYY-MM-DD>` | current day always re-runs |
| PubMed | individual XML file | `pubmed` | `<file id>` | |
| NCBI Taxonomy | full taxdump per run | `ncbi_taxonomy` | `<date>` | loader reads the newest |

Namespace and key are the two positional args to
`omicidx-prefect semaphores clear`, e.g.
`omicidx-prefect semaphores clear sra/study 2026-01-01_Full` — namespace
`sra/study` (note the embedded slash), key `2026-01-01_Full`.

The `Source` protocol (`omicidx/prefect/source.py`) hides this per-source
scheme behind `list_partitions()` / `extract(key, force)`; the load side
below reads whatever raw the source wrote.

## Upsert strategy (per entity)

All loaders **upsert** a **deduped, typed projection of raw** into the lake
by natural key via cdsci-lake's `upsert(con, target, source_sql, key)`
(ADR-0005; `upsert` builds a DuckDB `MERGE` under the hood). There is no
intermediate consolidated parquet — raw is the rebuildable backstop, lake
snapshots provide history.

- **Source** must yield **one row per key** (the MERGE rejects multiple
  source matches): `QUALIFY row_number() OVER (PARTITION BY <key> ORDER
  BY <recency>) = 1`, null/empty keys filtered.
- **Change gate:** `upsert` UPDATEs a matched row **only when a non-key
  column actually differs** — `t.<col> IS DISTINCT FROM s.<col>` across every
  payload column. There is **no `_row_hash` column**; the gate is computed
  column-wise by `upsert`. Unchanged rows never rewrite a data file (DuckLake
  is copy-on-write) and a re-run produces no snapshot.
- **Shape (built by `upsert`):** `WHEN MATCHED AND (<any payload col> IS
  DISTINCT FROM ...) THEN UPDATE SET *; WHEN NOT MATCHED THEN INSERT *`.
- **Per-load stamp columns** (e.g. a `snapshot_version` that changes every
  run) must be passed as `upsert(..., exclude_change_cols=[...])` so they are
  set on update but ignored by the change gate — otherwise they mark every
  row "changed" and force a full rewrite.
- **Native nested types are preserved** in the lake (`struct[]`,
  `varchar[]`, `timestamp`, `date`) — do **not** flatten to JSON.

Incremental vs full-snapshot — "incremental where possible":

| Source shape | Strategy |
|---|---|
| Raw hive-partitioned by date (SRA) | **High-water-mark**: scope source to `date >= <stored watermark>` (inclusive — boundary re-read is an IS-DISTINCT-FROM-gated no-op), advance to `max(date)` after upsert. |
| Flat full dump (bioproject, biosample) | **Full-snapshot** upsert; the change gate keeps writes incremental. |
| Partitioned NDJSON, no clean date scope (GEO) | **Full-snapshot** from raw NDJSON globs. |
| Flat files, cross-file key revisions (PubMed) | **Full-snapshot** by pmid; deletes (`delete IS TRUE`) removed via a separate labeled `DELETE`. |

High-water marks live in the lake **operational ledger**
(`ops.lake_ops.watermark`) via `ops.get_watermark` / `ops.set_watermark`,
keyed source `sra`, name `<lake_schema>:<entity>` (e.g. `omicidx:sra_study`)
— not semaphore files. Full re-derivation / backfill = run with `force=True`, which
drops the watermark filter (see the `reproduce-from-raw` entrypoint).

SRA monthly Fulls are **never merged on an incremental run** (#194): a Full
finishes extracting days after its `date`, past the watermark. They are the raw
re-derivation backstop, read only with `force=True`. `omicidx-prefect run
prune-sra-raw` (monthly timer) keeps the last two complete Fulls per entity and
everything dated after the older one.

## Commit metadata (self-documenting snapshots)

Every EL write runs inside an `ops.run(con, source=..., target=..., ...)`
block (ADR-0009), which attributes the snapshot automatically (author,
source, run id) so `SELECT * FROM lake.snapshots()` is an audit log. An
idempotent `upsert` writes no snapshot, so the attribution simply doesn't
land when nothing changed.

The transform layer — the **dormant** full-replace loaders in `flows/_parked/`,
which call `replace_to_ducklake` / `_stamped_txn` (defined in `ducklake.py`) —
stamps manually instead, because it issues `CREATE OR REPLACE TABLE`, not an
upsert:

```sql
BEGIN TRANSACTION;
CALL ducklake_set_commit_message('lake', <author>, <message>, extra_info := <json>);
CREATE OR REPLACE TABLE ... ;     -- or DELETE
COMMIT;
```

- The stamp **must share the DML transaction** — DuckLake clears it on
  commit, so an auto-committed statement loses it.
- Conventions: `author = 'omicidx:ducklake-load'`,
  `extra_info` = JSON `{run_id, ...}`, where `run_id` is systemd's
  `INVOCATION_ID` on a scheduled run — so `journalctl
  _SYSTEMD_INVOCATION_ID=<id>` finds the log for a given snapshot.
  Snapshots written before the Prefect excision (#158) carry the older
  `author = 'prefect:ducklake-load'` / `prefect_run_id` shape instead; those
  ids died with the Prefect server and no longer resolve to anything.
- A no-op write produces **no** snapshot, so the stamp simply doesn't land
  when nothing changed.

## Retention, time travel, and maintenance

**Retention is unbounded by default.** The internal lake is omicidx's
primary time machine (deliverables spec §1): each upsert is copy-on-write —
only changed rows are rewritten, so a snapshot stores just the delta — and
for several sources the raw inputs are *larger* than the lake, so keeping
every snapshot is cheap. Raw Parquet under `PUBLISH_ROOT` is the
**re-derivation backstop**: re-deriving from the *retained* raw reproduces
the lake (it is **not** a re-fetch from NCBI/EBI, which move on). It is
insurance — touched only to rebuild the lake, never the normal history-query
path.

> This history lives in the **internal** lake only. The published external
> bundle (Stage B) ships **current-state only** at v1 — it exposes today's
> snapshot, not history. External time travel (`AS OF`) is a later, gated stage
> (B′). Don't infer a downloaded artifact can query past state yet.

## External bundle (Stage B)

`publish-bundle` (`flows/publish_bundle.py`) writes the public daily artifact to
the dedicated public bucket (`PUBLIC_PARQUET_ROOT`, e.g. `r2://data-omicidx`),
served anonymously over HTTPS by the repo-root `site/` Cloudflare Worker
(`PUBLIC_PARQUET_HTTPS_BASE`). This is a **separate** bucket from the internal
lake's `cdsci-lake` — the internal Postgres catalog is never exposed externally,
and no catalog of any kind ships (ADR-0004 β; the frozen file-catalog DuckLake
was removed in #203).

Layout (per publish, `date` = UTC day):

```
<public root>/
  v{date}/                     # immutable dated snapshot
    *.parquet                  # flat per-table Parquet (parquet-export)
    {sradb,geometadb}/*.parquet# marts (parquet-export)
    omicidx.duckdb             # views over every Parquet file in v{date}/
    views.sql                  # the same views as SQL, concrete HTTPS URLs
    manifest.json              # provenance attestation (below)
  latest/                      # rolling; same files, views point at latest/
```

`parquet-export` prunes dated folders to the `PUBLIC_BUNDLE_KEEP_DAILY` (7)
newest plus the newest of each of the `PUBLIC_BUNDLE_KEEP_WEEKLY` (5) newest ISO
weeks (`select_dated_keep`, ADR-0007); today's is never pruned.

### Anonymous read contract (the load-bearing assumption, verified)

`omicidx.duckdb` holds nothing but views whose `read_parquet()` URLs are
absolute HTTPS, so a credential-free client just opens it:

```sql
ATTACH 'https://<base>/latest/omicidx.duckdb' AS omicidx (READ_ONLY);
USE omicidx;
SELECT count(*) FROM sra_runs;
```

(The DuckDB CLI's `.read` takes local files only, so `views.sql` is downloaded
first or pasted.)

Sharp edge (learned): R2 rejects buffered `.open("wb")` uploads with unequal
multipart parts — use `fs.put_file` (each upload is retried, #195). Offline
regression: `tests/test_bundle.py`.

### Bundle manifest (provenance, spec §2a)

`manifest.json` pins each publish to its provenance: `publish_date`,
`published_at`, `dated_path`, `transform_sha` (repo git SHA), `lake_snapshot_id`
(the internal snapshot the publish was cut from), `raw_partition_counts` (per
source namespace), and per-table row counts. This is the primitive that makes a
retained snapshot attestable — the gate B′ time travel builds on. v1 records
partition *counts*; full enumeration (exact reproduce-from-raw) is a B′ upgrade.

### Reading history (time travel)

`lake.snapshots()` is the version index. Pick a `snapshot_id` (that id is
the number you pass as `VERSION`) or a timestamp, then query the table `AT`
that version:

```sql
SELECT snapshot_id, snapshot_time FROM lake.snapshots() ORDER BY snapshot_id;   -- find the version
SELECT * FROM lake.omicidx.sra_study AT (VERSION => 42);                         -- rows as of snapshot 42
SELECT * FROM lake.omicidx.sra_study AT (TIMESTAMP => TIMESTAMP '2026-01-01');   -- rows as of a date
```

To keep an old state, materialize the `AT` query, e.g. `CREATE TABLE
sra_study_v42 AS SELECT * FROM lake.omicidx.sra_study AT (VERSION => 42);`.

### Reclaiming space (weekly, safe)

The weekly `ducklake-maintenance` flow reclaims R2 space *without* dropping
history — it passes no parameters, so it never expires snapshots:

```sql
CALL ducklake_cleanup_old_files('lake', cleanup_all => true);  -- only files no snapshot references
CALL ducklake_merge_adjacent_files('lake');                    -- compaction of small parquet files
```

`cleanup_old_files` never deletes a data file that a retained snapshot
pins, so it is safe under unbounded retention.

### Bounded expiry (opt-in only)

`expire_snapshots` is the **only** call that removes history. The flow runs
it **only** when `retention_days` is passed explicitly — never by default:

```sql
-- e.g. a dev lake that must not grow unbounded — ducklake_maintenance_flow(retention_days=365):
CALL ducklake_expire_snapshots('lake', older_than => now() - INTERVAL 365 DAY);
```

- Any ad-hoc `DROP` must still be followed by `cleanup_old_files` to
  reclaim R2 space.

## SQL gotchas

- Reserved words: `references`, `rows` must be quoted (`"references"`),
  and cannot be unquoted struct keys — use `{'references': "references"}`.
- `read_ndjson_auto(..., union_by_name = true)` is required for
  hive-partitioned NDJSON globs where early partitions are empty;
  otherwise DuckDB infers a single `json` column.
- Validation that bounds a source with `LIMIT N` must add `ORDER BY
  <key>` — a `LIMIT` without a stable order returns different rows across
  runs, so the re-run idempotency check (same input → no new snapshot)
  would spuriously fail.
- `flow_run.get_id()` returns `None` outside a flow context (valid JSON,
  just `null`).
