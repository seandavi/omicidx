# omicidx-prefect

The OmicIDX EL + transform pipeline over DuckLake. **There is no orchestrator**:
as of #158/#160 every job is a plain Python process on a systemd `--user` timer
(`systemd/README.md`). Prefect is gone — server, worker, deployments, decorators
— and the package name is the only thing left that says otherwise (renaming it
is its own churn, deferred).

## Why semaphore files

Partition completion has never lived in an orchestrator's database here, which
is why dropping two orchestrators in a row cost nothing: Dagster tracked
partitions in its Postgres event log, Prefect doesn't model partitions at all,
and leaning on either one's run history would have tied "what's done" to
whatever tool was current that year.

Instead, each partition writes a small JSON marker — a *semaphore* — to
the same storage bucket as the data:

```
{PUBLISH_ROOT}/_semaphores/
  sra/
    study/
      2024-09-12_Full.json
      2024-09-13_Incremental.json
      ...
    sample/...
    experiment/...
    run/...
  geo/
    2024-01.json
    2024-02.json
    ...
  pubmed/
    pubmed25n0001.json
    pubmed25n0002.json
    ...
  biosample/
    2024-09-13.json
  bioproject/
    2024-09-13.json
  ebi_biosample/
    2024-09-13.json
    ...
  sra_accessions_etag/
    latest.json   # stores the most-recently-seen ETag
```

Each semaphore file is `~200 bytes` of JSON: completion timestamp +
caller-supplied metadata (row_count, output_path, etc.).

**Rules:**

- A flow processes a partition only if its semaphore is missing (or
  `force=True` is passed).
- After the partition output is durably written, the flow writes the
  semaphore.
- Backfills are "delete the semaphores you want to redo, then re-run":

  ```bash
  omicidx-prefect semaphores clear sra/study --all          # whole entity
  omicidx-prefect semaphores clear pubmed pubmed25n0042     # one file
  ```
- Inspect with `omicidx-prefect semaphores list <namespace>` and
  `omicidx-prefect semaphores show <namespace> <key>`.

The current-period partition (GEO current month, EBI current day) is
always re-run by default, because upstream data accumulates within the
period. Pass `rerun_current_month=False` / `rerun_current_day=False`
to skip even those.

## Layout

```
packages/omicidx-prefect/
├── pyproject.toml
├── src/omicidx/prefect/
│   ├── config.py            # Settings + storage / duckdb / postgres helpers
│   ├── semaphore.py         # SemaphoreStore
│   ├── source.py            # Source protocol + run_extraction driver
│   ├── run.py               # run_id() + retry (what @flow/@task used to give)
│   ├── cli.py               # `omicidx-prefect` operator CLI
│   ├── flows/
│   │   ├── sra.py           # each extract: `python -m ...flows.<name> run`
│   │   ├── geo.py
│   │   ├── biosample.py
│   │   ├── pubmed.py
│   │   ├── ebi_biosample.py
│   │   ├── postgres.py
│   │   ├── sql.py           # thin duckdb-build (mart views over public Parquet)
│   │   ├── transform.py     # SQLMesh transform (plan/apply)
│   │   └── main.py          # the downstream chain (`omicidx-downstream` unit)
│   ├── transform/           # SQLMesh project: src→stg→geometadb/sradb marts
│   └── sql/                 # raw→parquet SQL (010 only; 020–050 → SQLMesh)
└── tests/
```

## Quick start

```bash
# 1) From the workspace root
uv sync

# 2) Run a flow directly (no scheduler)
uv run omicidx-prefect run sra
uv run omicidx-prefect run geo --start-month 2024-01
uv run omicidx-prefect run pubmed
uv run omicidx-prefect run daily

# 3) Inspect semaphores
uv run omicidx-prefect semaphores list sra/study
uv run omicidx-prefect semaphores show pubmed pubmed25n0001
```

## Scheduling

Every scheduled job is a systemd `--user` timer on onclappc02 — see
`systemd/README.md` at the repo root for the unit table, the install steps, and
the recovery commands. In short:

| Job | Unit |
|---|---|
| one extract per domain | `omicidx-{sra,pubmed,biosample,ebi-biosample}-extract` |
| the whole downstream chain | `omicidx-downstream` |
| weekly catalog maintenance | `omicidx-ducklake-maintenance` |

The downstream chain is `ducklake-load → transform → parquet-export →
postgres-load → publish-bundle`. `transform` is the SQLMesh mart build (lake
views); `parquet-export` is the reverse-ETL (lake tables + marts → public
Parquet, ADR-0004); `publish-bundle` builds the thin marts-only
`omicidx.duckdb` into the frozen bundle.

**Extraction and the downstream chain are decoupled.** Nothing downstream waits
on an extract: the chain loads whatever raw is on R2 when it starts. That is
the point of #149 — under the old single `daily-pipeline` flow, GEO's crawl
held the publish hostage for a month.

GEO's timer is committed but not installed: 74 months (2020-07 onward) have
never been extracted, and that ~27h backlog needs one foreground run first
(#174). Run it by hand with `omicidx-prefect run geo`; it is resumable.

## Environment variables

Same as omicidx-dagster:

```
PUBLISH_ROOT=s3://omicidx
S3_ACCESS_KEY_ID=...
S3_SECRET_ACCESS_KEY=...
S3_ENDPOINT=https://....r2.cloudflarestorage.com
S3_REGION=auto
S3_URL_STYLE=path
POSTGRES_URI=postgresql://omicidx@host:5432/omicidx

# Public Parquet export (reverse-ETL; ADR-0004)
PUBLIC_PARQUET_ROOT=r2://data-omicidx                       # dedicated public bucket
PUBLIC_PARQUET_HTTPS_BASE=https://data.omicidx.cancerdatasci.org  # base for views.sql URLs
```

## Tests

```bash
uv run pytest packages/omicidx-prefect/tests/
```

## Operational notes

- **Concurrency**: `source.run_extraction` fans a source's partitions across a
  bounded `ThreadPoolExecutor`; each source passes its own `max_workers`
  (GEO uses 2 to be polite to the eutils API).
- **Retries**: extraction retries a partition 3x/60s (`source.py`); the load
  stages retry once at 60s (`run.retry`). Beyond that the process exits
  non-zero, which trips the unit's `OnFailure=` and pages via ntfy.
- **Failure semantics**: if a partition fails after retries, the semaphore is
  **not** written — the next run picks it up automatically.
- **Force re-run**: every extract accepts `force` (`--force` on the CLI) to
  bypass semaphores.
- **Tracing a snapshot back to its run**: lake snapshots carry `run_id` in
  `commit_extra_info`; on a scheduled run that is systemd's `INVOCATION_ID`, so
  `journalctl _SYSTEMD_INVOCATION_ID=<id>` finds the log.
