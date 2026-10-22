---
title: Use OmicIDX from R (omicidxr)
description: The omicidxr R package connects DuckDB to OmicIDX, pins a version, gives tab-completable dplyr tables, and replaces SRAdb and GEOmetadb.
---

[omicidxr](https://github.com/omicidx/omicidxr) is an R package for OmicIDX.
It connects DuckDB to the published Parquet, so you can query it with SQL or
`dplyr` without downloading anything. It also has helpers for the common
SRAdb and GEOmetadb tasks.

## Install

Install from GitHub with [pak](https://pak.r-lib.org):

```r
# install.packages("pak")
pak::pak("omicidx/omicidxr")
```

`remotes::install_github("omicidx/omicidxr")` works too. The package depends
on [duckdb](https://r.duckdb.org). On Linux without binary packages (e.g. from
[Posit Package Manager](https://packagemanager.posit.co) or
[r-universe](https://r-universe.dev)), duckdb compiles from source, which takes
20 minutes or more. A Bioconductor release is planned.

## Connect

`omicidx_connect()` returns an ordinary DuckDB DBI connection. Tables are
`schema.table`, such as `sradb.study` or `geometadb.gsm`:

```r
library(omicidxr)

con <- omicidx_connect()

omicidx_tables(con)
omicidx_describe(con, "sradb.run")

DBI::dbGetQuery(con, "SELECT count(*) AS n FROM sradb.study")

DBI::dbDisconnect(con)
```

### Pick a version

The `version` argument picks a [version](/guides/versions-and-citation/):

| `version` | What you get |
|---|---|
| `"latest"` (default) | The nightly build at `https://omicidx.cancerdatasci.org/data/latest/` |
| `"YYYY-MM-DD"` | A dated nightly build. Kept for 30 days |
| `"YYYY-MM"` | A monthly release from HuggingFace (`omicidx/omicidx@YYYY-MM`). Kept long term. Cite this one |

```r
con <- omicidx_connect("2026-10-20")  # a dated build
con <- omicidx_connect("2026-10")     # the October 2026 monthly release
```

A monthly release takes about 30 seconds to connect, because DuckDB inspects
every Parquet file as it creates the views. HuggingFace limits anonymous
requests to 3000 per 5 minutes per IP address. A query can make hundreds, so
heavy use can fail with HTTP 429 for a few minutes.

## Tab-completable dplyr tables

`omicidx_db()` returns every table as a lazy `dplyr` table, nested by schema,
so `db$sradb$<TAB>` completes table names. Printing `db` lists the schemas,
tables, and row counts. Nothing is read until `collect()`:

```r
db <- omicidx_db()
db

db$sradb$experiment |>
  dplyr::filter(library_strategy == "RNA-Seq") |>
  dplyr::count(platform, sort = TRUE) |>
  dplyr::collect()

con <- attr(db, "con")  # the DBI connection behind it
DBI::dbDisconnect(con)
```

`omicidx_db()` takes a `version` too, or an existing connection:
`omicidx_db(con)`.

## SRAdb and GEOmetadb helpers

Each helper takes the connection first, runs one query, and returns a tibble.

The runs of an SRA study, with the fields you need to download them:

```r
con <- omicidx_connect()

sra_runs(con, "SRP299835") |>
  dplyr::select(run_accession, experiment_accession, library_layout, spots, instrument_model)
#>   run_accession experiment_accession library_layout   spots instrument_model
#> 1 SRR13332242   SRX9759115           SINGLE         5970564 NextSeq 500
#> 2 SRR13332243   SRX9759115           SINGLE         6168624 NextSeq 500
#> … 72 runs
```

`sra_runs()` also takes experiment, sample, and GEO accessions.

Convert accessions across SRA, GEO, BioSample, and BioProject, in any
direction:

```r
sra_convert(con, "GSE164073", c("run", "biosample")) |>
  dplyr::arrange(run)
#>   run         biosample    gse
#> 1 SRR13332242 SAMN17187141 GSE164073
#> 2 SRR13332243 SAMN17187141 GSE164073
#> … 72 rows
```

The samples and platforms of a GEO series, and the characteristics of a
sample:

```r
geo_samples(con, "GSE164073") |>
  dplyr::select(gsm, title, gpl, sra_experiment)
#>   gsm        title         gpl      sra_experiment
#> 1 GSM4996084 Cornea_mock_1 GPL18573 SRX9759115
#> 2 GSM4996085 Cornea_mock_2 GPL18573 SRX9759116
#> … 18 samples

geo_characteristics(con, "GSM4996084")
#>   gsm        channel tag        value
#> 1 GSM4996084       1 tissue     cornea
#> 2 GSM4996084       1 infection  mock
#> 3 GSM4996084       1 time point 24 hours
```

`sra_search()` and `geo_search()` match study and series titles and
abstracts, case-insensitively:

```r
sra_search(con, "single cell%glioma", limit = 5)
```

### Migrating from SRAdb and GEOmetadb

The `sradb` and `geometadb` schemas follow the SRAdb and GEOmetadb tables, so
most code ports by connecting with `omicidx_connect()` and prefixing table
names with the schema:

| SRAdb / GEOmetadb | omicidxr |
|---|---|
| `getSRAdbFile()` or `getSQLiteFile()`, then `dbConnect(SQLite(), ...)` | `con <- omicidx_connect()` |
| `dbGetQuery(sra_con, "SELECT * FROM study ...")` | `DBI::dbGetQuery(con, "SELECT * FROM sradb.study ...")` |
| `sraConvert(in_acc, out_type, sra_con)` | `sra_convert(con, in_acc, out_type)` |
| `getSRAinfo()`, `listSRAfile()` | `sra_runs(con, in_acc)`. Gives run accessions for the SRA Toolkit, not file URLs |
| `getSRA(search_terms, out_types, sra_con)` | `sra_search(con, query)` |
| Parsing `characteristics_ch1` by hand | `geo_characteristics(con, gsm)` |

There are no SRA submission or GEO DataSet (GDS) tables, and
`characteristics_ch1` is a list of tag/value pairs rather than a string. The
[Getting Started vignette](https://github.com/omicidx/omicidxr/blob/main/vignettes/introduction.qmd)
has the full list of differences, and `help(package = "omicidxr")` documents
every function.
