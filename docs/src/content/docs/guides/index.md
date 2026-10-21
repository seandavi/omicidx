---
title: Get the data
description: Read OmicIDX Parquet from DuckDB, Python, or R over anonymous HTTPS.
---

Every OmicIDX table is a Parquet file under
`https://omicidx.cancerdatasci.org/data/`. Anything that reads Parquet over
HTTPS can use it. You don't need an account or a key. There's no REST API.

- **DuckDB:** [Query with DuckDB](/guides/duckdb-snapshot/) covers the published database, every table, and example queries.
- **Pinning and citing:** [Versions and citation](/guides/versions-and-citation/) explains `latest/`, dated folders, and the monthly HuggingFace releases.

The recipes below use `latest/`. Swap in `v{YYYY-MM-DD}/` to pin a date.

## Python

With [polars](https://pola.rs), `scan_parquet` reads lazily, so only the
columns and row groups a query needs are fetched:

```python
import polars as pl

base = "https://omicidx.cancerdatasci.org/data/latest"

studies = (
    pl.scan_parquet(f"{base}/sradb/study.parquet")
    .filter(pl.col("study_type") == "Transcriptome Analysis")
    .select("study_accession", "study_title")
    .head(10)
    .collect()
)
```

With [DuckDB](https://duckdb.org/docs/api/python/overview), attach the
published database and get a polars or pandas frame back:

```python
import duckdb

con = duckdb.connect()
con.sql("ATTACH 'https://omicidx.cancerdatasci.org/data/latest/omicidx.duckdb' AS omicidx (READ_ONLY)")
gsms = con.sql("SELECT gsm FROM omicidx.geometadb.gse_gsm WHERE gse = 'GSE118849'").pl()
```

## R

The [duckdb](https://r.duckdb.org) package gives you the same database through
DBI:

```r
library(DBI)

con <- dbConnect(duckdb::duckdb())
dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
dbExecute(con, "ATTACH 'https://omicidx.cancerdatasci.org/data/latest/omicidx.duckdb' AS omicidx (READ_ONLY)")

gsms <- dbGetQuery(con, "SELECT gsm FROM omicidx.geometadb.gse_gsm WHERE gse = 'GSE118849'")

# Any table, without the database
n_studies <- dbGetQuery(con, "
  SELECT count(*) AS n
  FROM read_parquet('https://omicidx.cancerdatasci.org/data/latest/sra_studies.parquet')
")

dbDisconnect(con, shutdown = TRUE)
```

## Command line

```bash
# Row counts for every published table
curl -s https://omicidx.cancerdatasci.org/data/latest/manifest.json

# One table, downloaded
curl -O https://omicidx.cancerdatasci.org/data/latest/geometadb/gse.parquet
```
