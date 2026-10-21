---
title: Query with DuckDB
description: Query the public OmicIDX Parquet with DuckDB over anonymous HTTPS. Covers the published database, the SRAdb- and GEOmetadb-compatible tables, and the raw source tables.
---

OmicIDX publishes every table as Parquet over anonymous HTTPS. [DuckDB](https://duckdb.org)
reads only the parts of each file that a query needs, so you can query tens of
millions of rows from a laptop without downloading anything first.

## What's published

The base URL is:

```
https://omicidx.cancerdatasci.org/data/
```

(The older `https://data.omicidx.cancerdatasci.org/` serves the same files.)

Under it, `latest/` is rebuilt every night. Each night also gets an immutable
`v{YYYY-MM-DD}/` folder, kept for 30 days. See [Versions and
citation](/guides/versions-and-citation/) for which to use. Each folder contains:

| Path | What it is |
|---|---|
| `<table>.parquet` | One file per source table: `sra_studies`, `sra_samples`, `sra_experiments`, `sra_runs`, `sra_accessions`, `geo_series`, `geo_samples`, `geo_platforms`, `geo_series_with_rnaseq_counts`, `biosamples`, `bioprojects`, `pubmed_articles` |
| `sradb/<table>.parquet` | SRAdb-compatible tables |
| `geometadb/<table>.parquet` | GEOmetadb-compatible tables |
| `omicidx.duckdb` | A small DuckDB database with a view over every Parquet file |
| `views.sql` | The same views as plain SQL |
| `manifest.json` | Publish date, transform commit, lake snapshot id, row counts |

You can browse a folder listing in your browser, e.g.
[`latest/`](https://omicidx.cancerdatasci.org/data/latest/).

## Attach the database

`omicidx.duckdb` is a few hundred KB of view definitions. Attach it over HTTPS
and query the views. The data stays remote.

```sql
ATTACH 'https://omicidx.cancerdatasci.org/data/latest/omicidx.duckdb'
  AS omicidx (READ_ONLY);
USE omicidx;

SELECT run_accession, study_title
FROM sradb.rnaseq_runs
WHERE taxon_id = 9606          -- human
LIMIT 10;
```

DuckDB loads its `httpfs` extension automatically for `https://` paths. If
your install can't auto-load extensions, run `INSTALL httpfs; LOAD httpfs;`
first.

## Read a Parquet file directly

Every table is a plain file, so you can skip the database and use
`read_parquet`:

```sql
SELECT count(*)
FROM read_parquet('https://omicidx.cancerdatasci.org/data/latest/biosamples.parquet');

SELECT study_accession, title, study_type
FROM 'https://omicidx.cancerdatasci.org/data/latest/sra_studies.parquet'
WHERE bioproject = 'PRJNA1147303';
```

Filters and column selection are pushed down, so only the matching parts of a
file are fetched. `pubmed_articles.parquet` is about 37 GB, so filter it in
place.

## SRAdb-compatible tables (`sradb`)

A modernized [SRAdb](https://bioconductor.org/packages/SRAdb/) schema. Legacy
column names are kept where the data still exists, always-empty columns are
dropped, and BioProject, GEO, and PubMed cross-references are added.

| Table | One row per |
|---|---|
| `sradb.study` | SRA study (SRP/ERP/DRP) |
| `sradb.sample` | SRA sample (SRS/ERS/DRS) |
| `sradb.experiment` | SRA experiment (SRX/ERX/DRX) |
| `sradb.run` | SRA run (SRR/ERR/DRR) |
| `sradb.sra` | run, joined with its experiment, sample, and study |
| `sradb.run_with_study` | run, with study title, type, and BioProject |
| `sradb.rnaseq_runs`, `sradb.wgs_runs` | `run_with_study` filtered by library strategy |
| `sradb.human_runs`, `sradb.mouse_runs` | `run_with_study` filtered by taxon |

```sql
SELECT study_accession, study_title, bioproject_accession, pubmed_ids
FROM sradb.study
WHERE study_accession = 'SRP000001';

-- RNA-seq runs for one BioProject
SELECT run_accession, platform, instrument_model, total_bases
FROM sradb.run_with_study
WHERE bioproject = 'PRJNA1147303' AND library_strategy = 'RNA-Seq';
```

## GEOmetadb-compatible tables (`geometadb`)

A modernized [GEOmetadb](https://bioconductor.org/packages/GEOmetadb/) schema:
series (`gse`), samples (`gsm`), platforms (`gpl`), the `gse_gsm` and `gse_gpl`
link tables, and `geo_supplemental_files`.

```sql
-- Series with NCBI-computed RNA-seq counts
SELECT gse, title
FROM geometadb.gse
WHERE has_geo_computed_rnaseq
LIMIT 10;

-- All samples in a series
SELECT gsm FROM geometadb.gse_gsm WHERE gse = 'GSE118849';
```

## Source tables

The source tables (`sra_runs`, `geo_samples`, `biosamples`, …) keep the
upstream record shape, including nested lists of attributes and identifiers.
In `omicidx.duckdb` they're views in the default schema, so after `USE omicidx`
you can write `FROM sra_studies`. Older dated databases contain only the
`sradb` and `geometadb` views; use `read_parquet` for source tables there.

```sql
-- Unnest SRA study attributes
SELECT accession, a.tag, a.value
FROM 'https://omicidx.cancerdatasci.org/data/latest/sra_studies.parquet',
     unnest(attributes) AS t(a)
LIMIT 10;
```

## Work offline

To query heavily, download the files you need and point DuckDB at the local
copies:

```bash
curl -O https://omicidx.cancerdatasci.org/data/latest/sradb/study.parquet
duckdb -c "SELECT count(*) FROM 'study.parquet'"
```

`views.sql` points at absolute HTTPS URLs. To query local copies, edit the
URLs, or write your own `CREATE VIEW` statements.

## Notes

- Requests are anonymous HTTPS range requests. Behind a strict firewall, allow `omicidx.cancerdatasci.org`.
- Any recent DuckDB release works. The examples on this page were tested with DuckDB 1.5.
