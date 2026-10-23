# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Deliverables & invariants (durable intent)

Durable source of intent: **`docs/specs/omicidx-deliverables.md`** (endpoints,
gap analysis, staged plan, precedent-setting decisions). Read it before
proposing structural changes.

Deliverable dependency structure (not a flat list): steel thread **(1) internal
lake → (2) external frozen artifact**, then derived **(3) marts → (4) docs
site**; **(5) R/Python client** and **(6) performant API** are optional and
gated on explicit opt-in.

Invariants that follow from the finalized decisions:

- **Extraction contract:** each source is a deep module behind a narrow Source
  protocol — `list_partitions() -> keys` and `extract(key, force)`. Output
  format, partitioning, and incrementality are hidden inside the source module,
  not leaked to downstream loaders.
- **Catalog topology:** one internal catalog — the shared cdsci-lake Postgres
  `lake` DB, `omicidx` schema, data at `r2://cdsci-lake/`. The external side is
  a **published bundle of flat Parquet + `omicidx.duckdb`/`views.sql` (views
  over every public Parquet file) + a provenance `manifest.json`** (ADR-0004 β,
  re-affirmed in #203), `omicidx` tables only — never a second live catalog,
  Postgres or file-based.
- **Public read path:** anonymous HTTPS via the Cloudflare Worker /
  custom-domain (range requests). No `--no-sign-request` R2, no client
  credentials. Rolling `latest/` + immutable dated `v{date}/` (kept
  `PUBLIC_BUNDLE_RETENTION_DAYS`, default 30); each folder's views point at
  that folder's Parquet. Opening `omicidx.duckdb` and querying it
  credential-free over HTTPS range is the load-bearing assumption
  (`tests/test_bundle.py`).
- **Time travel:** the internal lake is the primary time machine (extended
  snapshot retention); raw Parquet is the re-derivation backstop, not the primary
  query path. **External** time-travel is an *earned unlock* (spec Stage B′) gated
  on snapshot faithfulness (a per-publish bundle manifest + transform lineage) —
  it is neither a v1 feature nor a closed door.

## Repo structure

This is a **uv workspace** consolidating three packages (`omicidx-dagster` was
retired 2026-07, Prefect excised 2026-08, `omicidx-api` retired 2026-10 in #196):

```
omicidx/                        # workspace root (no package of its own)
├── pyproject.toml              # workspace root: members = ["packages/*"]
├── uv.lock
└── packages/
    ├── omicidx-parsers/        # XML parsers + Pydantic models for NCBI SRA, GEO, BioSample
    │   └── src/omicidx/parsers/
    ├── omicidx-etl/            # Legacy `oidx` extractors (NCBI→raw). Superseded by
    │   └── src/omicidx/etl/    #   omicidx-prefect except europepmc/icite/nih_reporter (unported)
    └── omicidx-prefect/        # EL + transform pipeline (DuckLake), systemd-scheduled
        └── src/omicidx/prefect/  #   name is vestigial — no Prefect since #158/#160
```

All packages share the `omicidx` **namespace package** (PEP 420 implicit — no `__init__.py` at `src/omicidx/`). `omicidx-etl` depends on `omicidx-parsers` as a workspace-local reference (`tool.uv.sources`).

## Commands

All commands run from the workspace root.

```bash
# Install all workspace packages and dependencies
uv sync

# Run tests (parsers has network-hitting tests against live NCBI APIs)
uv run pytest packages/omicidx-parsers/tests/
uv run pytest packages/omicidx-etl/tests/
uv run pytest packages/omicidx-prefect/tests/

# Run a single test file
uv run pytest packages/omicidx-parsers/tests/geo/test_parser.py

# ETL CLI (requires .env with AWS/S3 credentials)
uv run oidx --help
uv run oidx sra extract --dest s3://${OMICIDX_DATA_ROOT}/sra/raw
uv run oidx geo extract s3://${OMICIDX_DATA_ROOT}
uv run oidx biosample extract s3://${OMICIDX_DATA_ROOT}
uv run oidx pubmed extract s3://${OMICIDX_DATA_ROOT}

# Parser CLI (GEO entry point)
uv run omicidx_tool --help

# Just recipes (omicidx-etl) — wraps oidx commands with .env loading
just sra-extract
just geo-extract
just extract-all
```

## Architecture

### omicidx-parsers

Parses raw XML from NCBI FTP/API into typed Pydantic v2 models. Key submodules:

- `omicidx.parsers.sra` — SRA Study/Sample/Experiment/Run XML → `SraStudy`, `SraSample`, etc.
- `omicidx.parsers.geo` — GEO SOFT format → `GEOSeries`, `GEOSample`, `GEOPlatform`
- `omicidx.parsers.biosample` — BioSample/BioProject XML → `BioSampleParser`, `BioProjectParser`
- `omicidx.parsers.scripts.geo` — Click CLI (`omicidx_tool`), exposed as entry point

Parsers return iterators of dicts or Pydantic models. The `sra.parser` module is the primary entry point; it detects entity type from filename.

### omicidx-etl

Long-running extraction jobs that write Parquet/NDJSON to S3-compatible storage. Each data source is a submodule with an `extract` Click command registered in `omicidx.etl.cli:cli` (`oidx`):

- `omicidx.etl.sra` — mirrors NCBI SRA XML, converts to Parquet partitioned by date/stage
- `omicidx.etl.geo` — fetches GEO SOFT files, writes NDJSON to partitioned paths
- `omicidx.etl.biosample` — streams BioSample XML, writes JSONL.gz
- `omicidx.etl.etl.pubmed` — downloads PubMed baseline + updates → Parquet

Only the unported extractors (europepmc, icite, nih_reporter) are still
exclusive to this package; sra/geo/biosample/pubmed are superseded by
omicidx-prefect but still present. The DuckDB SQL runner + `build-db` transform
path was retired 2026-07 (transform now lives in omicidx-prefect / SQLMesh).

Configuration is via `omicidx.etl.config.Settings` (pydantic-settings), loaded from environment or `.env`. Key variable: `PUBLISH_DIRECTORY` (default `/data/omicidx`, supports S3 URIs via `universal-pathlib`).

### omicidx-prefect

The EL + transform pipeline on the DuckLake substrate. **There is no
orchestrator** (#158/#160, 2026-08-15): Prefect is fully excised — no server, no
worker container, no `prefect.yaml`, no `@flow`/`@task`, not even the
dependency. Dagster went the same way in 2026-07. The package name is now the
only Prefect artifact left; renaming it is deferred churn.

Every job is a plain Python process on a systemd `--user` timer. `run.py`
supplies the two things the decorators used to (`run_id()` from systemd's
`INVOCATION_ID`; a tenacity `retry`), `source.py` the fan-out driver.
Partition state lives in **semaphore JSON files** in the storage bucket — never
in an orchestrator's database, which is why dropping two orchestrators cost
nothing. Unit table + install steps: `systemd/README.md`.

```
per-domain extract timers  ──(raw on R2)──▶  omicidx-downstream (one unit)
                                             ducklake-load → transform →
                                             parquet-export → publish-bundle
```

Extraction and the downstream chain are **decoupled**: nothing downstream waits
on an extract, so a slow or failing source can no longer hold the publish
hostage (which is what GEO did for a month — #174).

| Stage | Module | What it does |
|---|---|---|
| extracts | `flows/{sra,geo,biosample,ebi_biosample,pubmed,ncbi_taxonomy}.py` | NCBI/EBI → raw Parquet/NDJSON on R2 (`PUBLISH_ROOT`), semaphore-gated. One timer each, `python -m omicidx.prefect.flows.<name> run`. GEO's backlog drained 2026-08-17 (#174) and its timer is installed (#190); its unit holds `%t/omicidx-geo.lock`, which any manual re-extract must also take. GEO is `MAX_WORKERS = 1` on purpose — more outer concurrency makes it *slower*. |
| `ducklake-load` | `flows/ducklake*.py` | MERGE raw → `lake.omicidx.*` (hash-gated, copy-on-write; SRA high-water-mark incremental) |
| `transform` | `flows/transform.py` + `transform/` (SQLMesh) | `plan(prod)` materializes `src`→`stg`→`geometadb.*`/`sradb.*` marts as views in the lake |
| `parquet-export` | `flows/parquet_export.py` | Reverse-ETL: COPY lake tables **and marts** → public Parquet `r2://data-omicidx/latest/*.parquet` + `latest/{sradb,geometadb}/*.parquet` (ADR-0004) |
| `publish-bundle` | `flows/publish_bundle.py` + `flows/sql.py` | **Thin**: `omicidx.duckdb`/`views.sql` = one `CREATE VIEW` per public Parquet file (base tables + marts, no re-derivation) + `manifest.json`, to `v{date}/` and `latest/` |

- Config: `config.py` (`Settings` + `get_ducklake_connection`, `get_public_parquet_path`, etc.). Key env: `PUBLISH_ROOT`, `DUCKLAKE_URI`, `DUCKLAKE_DATA_PATH`, `PUBLIC_PARQUET_ROOT`, `PUBLIC_PARQUET_HTTPS_BASE`.
- Catalog topology + MERGE/maintenance conventions: `DUCKLAKE.md`. Public-serving contract: `docs/adrs/0004`.
- Operator CLI `omicidx-prefect` (`cli.py`) is the ad-hoc/recovery path: every `run` subcommand calls exactly what the matching timer calls, so a stage that failed overnight is re-run by hand (`omicidx-prefect run transform`, `... run publish-bundle`). All stages are idempotent.
- The units run on the **host**, so `.env` uses `127.0.0.1:5432` for `DUCKLAKE_URI`. The old `pg_main` container-hostname split died with the worker.

### Data flow

```
NCBI/EBI → omicidx-parsers (XML → Pydantic) → omicidx-prefect raw-extract (→ raw Parquet/NDJSON on R2)
                                                         ↓
                                              ducklake-load (MERGE raw → lake.omicidx.*)
                                                         ↓
                                              transform (SQLMesh: src → stg → marts)
                                                         ↓
                                   parquet-export → public Parquet → publish-bundle (omicidx.duckdb views + views.sql + manifest)
```

### GitHub Actions

The only active workflow is root `.github/workflows/ci.yaml` (ruff + tests on
push). No pipeline runs in CI: everything scheduled is a systemd `--user` timer
on onclappc02 (`systemd/README.md`). The legacy `oidx`-based extraction and
`build-db` cron workflows were retired 2026-07.

## Environment / secrets

ETL requires a `.env` in `packages/omicidx-etl/` (loaded automatically by `python-dotenv` and `just`):

```
PUBLISH_DIRECTORY=s3://your-bucket/omicidx
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_ENDPOINT_URL=...        # for S3-compatible stores (Cloudflare R2, etc.)
AWS_URL_STYLE=path
AWS_REGION=...
```

omicidx-prefect's `docker-compose.yml` hardcodes the container-side
`DUCKLAKE_URI` (`host=pg_main`) and interpolates only the secret via
`DUCKLAKE_PG_PASSWORD`, which must be set in the gitignored repo-root `.env`
(symlinked into the package). If unset it interpolates empty and the catalog
connection fails — see [[reference_ducklake_uri_host_split]].
