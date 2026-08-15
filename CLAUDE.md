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
  a **published, retaining file-catalog DuckLake bundle** (Parquet + `omicidx.duckdb`
  + `views.sql` + a read-only **file-based** catalog + a provenance manifest),
  `omicidx` tables only — never a second live Postgres catalog.
- **Frozen-lake read path:** anonymous HTTPS via the Cloudflare Worker /
  custom-domain (range requests). No `--no-sign-request` R2, no client
  credentials. Rolling `latest/` + immutable dated `v{date}/`. Anonymous,
  credential-free file-catalog `ATTACH` over HTTPS range is the load-bearing
  assumption — verify it end-to-end early.
- **Time travel:** the internal lake is the primary time machine (extended
  snapshot retention); raw Parquet is the re-derivation backstop, not the primary
  query path. **External** time-travel is an *earned unlock* (spec Stage B′) gated
  on snapshot faithfulness (a per-publish bundle manifest + transform lineage) —
  it is neither a v1 feature nor a closed door.

## Repo structure

This is a **uv workspace** consolidating four packages (`omicidx-dagster` was
retired 2026-07; Prefect is the sole orchestrator):

```
omicidx/                        # workspace root (no package of its own)
├── pyproject.toml              # workspace root: members = ["packages/*"]
├── uv.lock
└── packages/
    ├── omicidx-parsers/        # XML parsers + Pydantic models for NCBI SRA, GEO, BioSample
    │   └── src/omicidx/parsers/
    ├── omicidx-etl/            # Legacy `oidx` extractors (NCBI→raw). Superseded by
    │   └── src/omicidx/etl/    #   omicidx-prefect except europepmc/icite/nih_reporter (unported)
    ├── omicidx-prefect/        # Prefect 3 pipeline (DuckLake) — the sole live orchestrator
    │   └── src/omicidx/prefect/
    └── omicidx-api/            # Read-only FastAPI REST API backed by PostgreSQL
        └── src/omicidx/api/
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
uv run pytest packages/omicidx-api/tests/

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

Prefect 3 pipeline on the DuckLake substrate — the sole live orchestrator
(reimplements the retired omicidx-dagster pipeline). Partition state lives in
**semaphore JSON files** in the storage bucket. Pipeline:

```
raw-extract → ducklake-load → transform → parquet-export → postgres-load → duckdb-build
```

| Stage | Module | What it does |
|---|---|---|
| `raw-extract` | `flows/{sra,geo,biosample,ebi_biosample,pubmed}.py` | NCBI/EBI → raw Parquet/NDJSON on R2 (`PUBLISH_ROOT`), semaphore-gated |

**Extracts are migrating off Prefect onto systemd `--user` timers** (#144/#149).
`flows/sra.py` is the migrated pilot: no `@flow`/`@task`, stdlib logging, run as
`python -m omicidx.prefect.flows.sra run`, scheduled by
`systemd/omicidx-sra-extract.timer`, and removed from `raw_extract_flow`.
`flows/biosample.py` followed (#155) — one unit for **both** NCBI full dumps
(BioSample + BioProject), `systemd/omicidx-biosample-extract.timer`; being
unpartitioned, it calls its extracts directly instead of via `run_extraction`.
The shared driver `source.py` is orchestrator-neutral (bounded
`ThreadPoolExecutor` + tenacity), so any domain not yet migrated keeps working
unchanged until its own ticket (#154–#157) strips its decorators too.
| `ducklake-load` | `flows/ducklake*.py` | MERGE raw → `lake.omicidx.*` (hash-gated, copy-on-write; SRA high-water-mark incremental) |
| `transform` | `flows/transform.py` + `transform/` (SQLMesh) | `plan(prod)` materializes `src`→`stg`→`geometadb.*`/`sradb.*` marts as views in the lake |
| `parquet-export` | `flows/parquet_export.py` | Reverse-ETL: COPY lake tables **and marts** → public Parquet `r2://data-omicidx/latest/*.parquet` + `latest/{sradb,geometadb}/*.parquet` (ADR-0004) |
| `postgres-load` | `flows/postgres.py` | Reload API-serving Postgres tables from the lake (A/B-slot swap) |
| `duckdb-build` | `flows/sql.py` | **Thin**: generate `CREATE VIEW` per mart over the public mart Parquet (no re-derivation); marts-only |

- Config: `config.py` (`Settings` + `get_ducklake_connection`, `get_public_parquet_path`, etc.). Key env: `PUBLISH_ROOT`, `DUCKLAKE_URI`, `DUCKLAKE_DATA_PATH`, `PUBLIC_PARQUET_ROOT`, `PUBLIC_PARQUET_HTTPS_BASE`, `POSTGRES_URI`.
- Catalog topology + MERGE/maintenance conventions: `DUCKLAKE.md`. Public-serving contract: `docs/adrs/0004`.
- Operator CLI `omicidx-prefect` (`cli.py`); deployments in `prefect.yaml`; worker-only `docker-compose.yml` joins the shared monode `prefect-server`.

### omicidx-api

Read-only REST API for entity lookups, deployed at `api-omicidx.cancerdatasci.org`. Key modules:

- `omicidx.api.main` — FastAPI app, lifespan, middleware registration
- `omicidx.api.models.tables` — SQLAlchemy 2.0 ORM models (BioProject, BioSample, SRA, GEO, PubMed)
- `omicidx.api.routers` — endpoint routers per entity type
- `omicidx.api.pagination` — base64url keyset cursor encode/decode
- `omicidx.api.schemas.envelope` — consistent response envelope (data, meta, links, relationships)
- `omicidx.api.config` — pydantic-settings with `OMICIDX_API_` env prefix

Configuration via `OMICIDX_API_DATABASE_URL` (standard `postgresql://` URI, `+asyncpg` added internally).

### Data flow

```
NCBI/EBI → omicidx-parsers (XML → Pydantic) → omicidx-prefect raw-extract (→ raw Parquet/NDJSON on R2)
                                                         ↓
                                              ducklake-load (MERGE raw → lake.omicidx.*)
                                                         ↓
                                              transform (SQLMesh: src → stg → marts)
                                                         ↓
                                   parquet-export → public Parquet + frozen bundle + thin marts-only omicidx.duckdb
                                                         ↓
                                              postgres-load (A/B-slot swap)
                                                         ↓
                                         omicidx-api (FastAPI REST endpoints)
```

### GitHub Actions

The only active workflow is root `.github/workflows/ci.yaml` (ruff + tests on
push). The daily pipeline runs on the Prefect worker, not CI (deployments in
`packages/omicidx-prefect/prefect.yaml`). The legacy `oidx`-based extraction and
`build-db` cron workflows were retired 2026-07 when Prefect became the sole
orchestrator.

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
