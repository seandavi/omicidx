# OmicIDX

Cloud-native replacement for [SRAdb](https://bioconductor.org/packages/SRAdb/) and [GEOmetadb](https://bioconductor.org/packages/GEOmetadb/) — query 80M+ SRA runs, 8M GEO samples, and 50M biosamples via DuckDB.

Data is refreshed daily and served as Parquet files over HTTPS at `https://data.omicidx.cancerdatasci.org`. No account, no API key, no download required.

## Quick Start

Query any published table straight from the URL:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")

df = con.sql("""
    SELECT accession, title, total_spots, total_bases
    FROM read_parquet('https://data.omicidx.cancerdatasci.org/latest/sra_runs.parquet')
    WHERE total_bases > 1e10
    LIMIT 10
""").df()
```

Works with any tool that reads Parquet over HTTP — DuckDB, Polars, PyArrow, R `arrow`:

```r
library(arrow)
sra_runs <- read_parquet("https://data.omicidx.cancerdatasci.org/latest/sra_runs.parquet")
```

## SRAdb / GEOmetadb-compatible views (no download)

For the pre-built `sradb.*` / `geometadb.*` views, attach the published **read-only DuckLake catalog** over plain HTTPS — anonymous, credential-free, nothing to download:

```sql
INSTALL ducklake; LOAD ducklake; INSTALL httpfs; LOAD httpfs;
ATTACH 'ducklake:https://data.omicidx.cancerdatasci.org/latest/catalog.ducklake'
  AS omicidx (READ_ONLY);

SELECT * FROM omicidx.geo_platforms LIMIT 5;
```

Prefer a local file? The daily bundle also publishes a thin `omicidx.duckdb` (the
`sradb.*`/`geometadb.*` marts as views over the public Parquet) and a
`views.sql` you can `.read` against a fresh DuckDB. Example mart queries:

```sql
-- SRAdb-style
SELECT * FROM sradb.study WHERE study_type = 'Transcriptome Analysis' LIMIT 10;

-- GEOmetadb-style
SELECT gse, title FROM geometadb.gse
JOIN geometadb.geo_supplemental_files ON gse = accession
WHERE supplementary_file LIKE '%counts%' LIMIT 10;
```

## Available Data

Flat base tables, one Parquet file each, at `latest/<file>.parquet` (rolling) and
`v{date}/<file>.parquet` (immutable daily snapshots):

| Dataset | File | Records | Source |
|---------|------|---------|--------|
| SRA Runs | `sra_runs.parquet` | 83M+ | NCBI SRA |
| SRA Experiments | `sra_experiments.parquet` | 78M+ | NCBI SRA |
| SRA Samples | `sra_samples.parquet` | 81M+ | NCBI SRA |
| SRA Studies | `sra_studies.parquet` | 1.4M+ | NCBI SRA |
| SRA Accessions | `sra_accessions.parquet` | 143M+ | NCBI SRA |
| GEO Samples | `geo_samples.parquet` | 8.3M+ | NCBI GEO |
| GEO Series | `geo_series.parquet` | 280K+ | NCBI GEO |
| GEO Platforms | `geo_platforms.parquet` | 28K+ | NCBI GEO |
| BioSamples | `biosamples.parquet` | 51M+ | NCBI BioSample |
| BioProjects | `bioprojects.parquet` | 1M+ | NCBI BioProject |
| PubMed Articles | `pubmed_articles.parquet` | — | NCBI PubMed |

The `sradb.*` and `geometadb.*` marts are published under
`latest/sradb/*.parquet` and `latest/geometadb/*.parquet` and exposed as views by
the DuckLake catalog / `omicidx.duckdb` above.

## How it's built

The daily pipeline runs on Prefect (`packages/omicidx-prefect/`): NCBI/EBI →
internal DuckLake lake → SQLMesh transform (marts) → a frozen public bundle
(flat Parquet + `catalog.ducklake` + `omicidx.duckdb` + `views.sql` +
provenance manifest). See `CLAUDE.md` and `docs/specs/omicidx-deliverables.md`
for the architecture, and [ADR-0004](docs/adrs/0004-public-serving-parquet-plus-views-sql.md)
for the public-serving contract.

A read-only REST API (`packages/omicidx-api/`) and R/Python clients are optional,
gated deliverables — not part of the core public artifact above.

## License

MIT
