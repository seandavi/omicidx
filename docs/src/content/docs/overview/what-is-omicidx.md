---
title: What is OmicIDX?
description: A public index of biological metadata from NCBI, published as Parquet files over anonymous HTTPS.
---

OmicIDX collects metadata from the major NCBI archives, normalizes it, and
publishes it as Parquet files that anyone can query over HTTPS. There's no
account, no API key, and no server to call. You read the files.

## What it indexes

- **NCBI SRA**: studies, samples, experiments, runs, and the SRA accession table.
- **NCBI GEO**: series (GSE), samples (GSM), platforms (GPL), and the list of series with NCBI-computed RNA-seq counts.
- **NCBI BioSample** and **BioProject**: the sample and project records that tie SRA and GEO together.
- **NCBI PubMed**: citation records, for linking publications to the data they describe.

See [Data sources](/overview/data-sources/) for where each comes from and how
often it refreshes.

On top of the source tables, OmicIDX builds two sets of compatibility tables:

- **`sradb`**: a modernized [SRAdb](https://bioconductor.org/packages/SRAdb/) schema.
- **`geometadb`**: a modernized [GEOmetadb](https://bioconductor.org/packages/GEOmetadb/) schema.

## How you get it

- **Nightly:** `https://omicidx.cancerdatasci.org/data/latest/` is rebuilt every night.
- **Dated:** an immutable `v{YYYY-MM-DD}/` folder per night, kept for 30 days.
- **Monthly:** a long-term release on HuggingFace with a git tag and a DOI.

[Working with the data](/guides/duckdb-snapshot/) has the DuckDB details;
[Versions and citation](/guides/versions-and-citation/) explains which one to
use.

## What it isn't

- **Not a sequence archive.** OmicIDX indexes the metadata about sequencing data, not the reads. Get reads from SRA, ENA, or DDBJ.
- **Not real-time.** Data is at most a day or two behind NCBI.
- **Not an API.** There's no REST service. Every table is a file you can read from DuckDB, Python, R, or anything else that reads Parquet.
