# 0007. Retention and deletion: what OmicIDX keeps, and for how long

- **Status**: Accepted
- Date: 2026-10-22
- Related: [ADR-0004](./0004-public-serving-parquet-plus-views-sql.md),
  [ADR-0006](./0006-pubmed-dataset-releases-via-cdsci-lake.md), #194, #199, #201, #203

## Context

Storage grew by accretion: a frozen DuckLake copy beside the flat Parquet (108 GB
per dated folder), a Postgres copy for an API almost nobody called (368 GB), and SRA
monthly Full mirrors that the lake load never reads (#194). Each layer was kept
because nothing said it could go. This ADR says what each layer is *for*, and keeps
only what that purpose needs.

Two layers are the actual time machine (CLAUDE.md): the **internal lake**, with
unbounded snapshot retention, and **raw on R2**, the re-derivation backstop. The
public side exists to serve current data and to cite past data.

## Decision

| Layer | Purpose | Kept |
|---|---|---|
| Internal lake (`lake.omicidx.*`) | primary time machine | all snapshots (unbounded) |
| SRA raw (`PUBLISH_ROOT`, per entity) | re-derive the lake | the **two newest complete Fulls** and every incremental dated after the older one |
| Other raw sources | re-derive the lake | everything (no rule yet) |
| Public `latest/` | current data | overwritten nightly |
| Public `v{date}/` | day-level pins | **30 days** (`PUBLIC_BUNDLE_RETENTION_DAYS`) |
| HuggingFace `seandavis/omicidx@YYYY-MM` | long-term, citable releases | **forever** (git tags; DOIs lock the repo) |

**SRA raw rule** (`omicidx-prefect run prune-sra-raw`, monthly timer):
- A Full is *complete* when it has a done semaphore.
- Two complete Fulls, not one, so a long or overlapping extraction never leaves an
  entity without a finished backstop.
- An incomplete Full is protected only if it is dated after the older kept Full, the
  only place an extraction can still be running. The extractor lists only NCBI's
  current batch, and the prune holds the extract flock. Incomplete Fulls older than
  both kept Fulls are deleted like any old Full.
- Fulls are never merged on incremental loads; they are read only with `--force`
  (#194 option 1).

**Deleted outright, not retained:**
- The frozen DuckLake bundle (`catalog.ducklake`, `v{date}/data/`): about 817 GB,
  2026-10-21 (#203). No real `ATTACH` users.
- The API's Postgres database and role (`omicidx`, 368 GB), 2026-10-22 (#199).

This amends ADR-0004's "past snapshots never deleted". Dated public folders are a
30-day window; the HuggingFace monthly releases carry the permanence.

## Consequences

- Reproducing a public state older than 30 days means pinning a monthly HF release,
  or re-deriving from the lake snapshot named in that release's `manifest.json`.
- Re-deriving SRA from raw goes back only as far as the older kept Full. Further back
  is the lake's job.
- Deletion is scripted and dry-run by default. The first `--apply` of any new rule is
  reviewed by a human.
- The legacy published copies in `r2://omicidx` (`latest/`, `distribution/`,
  `{sra,geo,pubmed,biosample}/parquet/`, about 114 GiB) are not yet covered and await
  a separate decision.
