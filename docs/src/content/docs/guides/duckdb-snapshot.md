---
title: Working with the DuckDB snapshot
description: Query the public OmicIDX data snapshot with DuckDB — offline, over HTTPS, no account or credentials. Includes the SRAdb- and GEOmetadb-compatible views.
---

OmicIDX publishes its entire metadata index as a **public, credential-free data
snapshot** you can query with [DuckDB](https://duckdb.org). No API key, no
account, no rate limits — just anonymous HTTPS. This is the fastest way to run
large analytical queries (joins, aggregations, full scans) that the REST API is
not built for.

Everything lives under one base URL:

```
https://data.omicidx.cancerdatasci.org/latest/
```

| File | What it is |
|------|-----------|
| `omicidx.duckdb` | A ready-to-query DuckDB database with all views defined |
| `views.sql` | The view definitions, as portable SQL over the Parquet files |
| `*.parquet` | One file per source table (`sra_studies.parquet`, `geo_series.parquet`, `biosamples.parquet`, …) |
| `manifest.json` | Provenance for this snapshot (date, source commit, row counts) |

`latest/` always points at the most recent publish. Immutable dated snapshots
live alongside it at `v{YYYY-MM-DD}/` (e.g. `v2026-07-11/`) — pin to one of
those when you need a result to stay reproducible.

## Quick start — query without downloading

DuckDB can attach the published database directly over HTTPS and read only the
bytes each query touches. Nothing is downloaded up front.

```sql
INSTALL httpfs; LOAD httpfs;
ATTACH 'https://data.omicidx.cancerdatasci.org/latest/omicidx.duckdb'
  AS omicidx (READ_ONLY);
USE omicidx;

SELECT run_accession, study_title
FROM sradb.rnaseq_runs
WHERE taxon_id = 9606          -- human
LIMIT 10;
```

That's it — you're querying tens of millions of SRA runs from a laptop.

## Or download it and work offline

For repeated heavy querying, grab the database once and open it locally:

```bash
curl -O https://data.omicidx.cancerdatasci.org/latest/omicidx.duckdb
duckdb omicidx.duckdb
```

```sql
SELECT study_accession, study_title
FROM sradb.study
WHERE study_accession = 'SRP000001';
```

## What you can query

The snapshot exposes three layers. Most users want the top two.

### `sradb.*` — SRAdb-compatible views

A modernized reconstruction of the classic
[SRAdb](https://bioconductor.org/packages/SRAdb/) schema: legacy column names
kept where the data still exists, always-empty legacy columns dropped, and
modern cross-references (BioProject / GEO / BioSample links, library-layout
stats) added.

| View | Grain |
|------|-------|
| `sradb.study` | one row per SRA study (SRP…) |
| `sradb.sample` | one row per SRA sample (SRS…) |
| `sradb.experiment` | one row per SRA experiment (SRX…) |
| `sradb.run` | one row per SRA run (SRR/ERR/DRR…) |
| `sradb.sra` | the denormalized run×experiment×sample×study join |
| `sradb.run_with_study` | run-level rows enriched with study context |
| `sradb.rnaseq_runs`, `sradb.wgs_runs` | filtered by `library_strategy` |
| `sradb.human_runs`, `sradb.mouse_runs` | filtered by `taxon_id` |

```sql
-- RNA-seq runs for a given BioProject
SELECT run_accession, library_strategy, platform, instrument_model
FROM sradb.run_with_study
WHERE BioProject = 'PRJNA63463' AND library_strategy = 'RNA-Seq';
```

### `geometadb.*` — GEOmetadb-compatible views

A modernized reconstruction of
[GEOmetadb](https://bioconductor.org/packages/GEOmetadb/): series (`gse`),
samples (`gsm`), platforms (`gpl`), the `gse_gsm`/`gse_gpl` join tables, and
`geo_supplemental_files`.

```sql
-- GEO series that have NCBI-computed RNA-seq counts
SELECT gse, title
FROM geometadb.gse
WHERE has_geo_computed_rnaseq
LIMIT 10;

-- All samples in a series
SELECT gsm FROM geometadb.gse_gsm WHERE gse = 'GSE118849';
```

### Base tables — the raw Parquet

Underneath the views are the source-shaped tables, one Parquet file each:
`sra_studies`, `sra_samples`, `sra_experiments`, `sra_runs`, `sra_accessions`,
`geo_series`, `geo_samples`, `geo_platforms`, `geo_series_with_rnaseq_counts`,
`biosamples`, `bioprojects`, `pubmed_articles`. Query any of them directly,
without attaching the database:

```sql
INSTALL httpfs; LOAD httpfs;
SELECT count(*)
FROM read_parquet('https://data.omicidx.cancerdatasci.org/latest/biosamples.parquet');
```

If you'd rather rebuild the `sradb`/`geometadb` views yourself over the Parquet
(for example, to run against a pinned `v{date}/` snapshot), `views.sql` is the
portable definition — download it and `.read` it into any DuckDB session.

## Reproducibility

Every publish writes a `manifest.json` recording exactly what the snapshot
contains:

```bash
curl -s https://data.omicidx.cancerdatasci.org/latest/manifest.json
```

```json
{
  "publish_date": "2026-07-11",
  "dated_path": "v2026-07-11/",
  "transform_sha": "13b9a955…",
  "lake_snapshot_id": 1780,
  "raw_partition_counts": { "sra/study": 70, "geo": 259, … },
  "tables": [ { "schema": "sradb", "table": "study", "row_count": 740807 }, … ]
}
```

To cite a stable result, query the immutable dated URL rather than `latest/`:

```sql
ATTACH 'https://data.omicidx.cancerdatasci.org/v2026-07-11/omicidx.duckdb'
  AS omicidx_20260711 (READ_ONLY);
```

## Notes

- Requires DuckDB ≥ 0.10 with the `httpfs` extension (bundled; `LOAD httpfs`
  installs it on first use).
- Reads are anonymous HTTPS range requests — behind a strict firewall, allow
  `data.omicidx.cancerdatasci.org`.
- For single-record lookups and programmatic access, the
  [REST API](/api/overview/) is the better fit; use this snapshot for bulk and
  analytical queries.
