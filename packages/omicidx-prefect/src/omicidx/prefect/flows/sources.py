"""omicidx's source registry for the shared lake ledger (cdsci-lake ADR-0011 §4).

One `ops.Source` per omicidx *source* (not per lake table — many tables share a
source, e.g. sra_study/sample/experiment/run all come from `sra`). Registered
under producer `omicidx` at the top of `ducklake_load_flow` via
`ops.register_sources`, which is idempotent and self-healing.

All omicidx sources land in the `omicidx` lake schema. Only `sra` is incremental
(high-water by raw `date` partition); the rest are full external dumps whose
`upsert` still writes delta snapshots.
"""

from cdsci.lake import ops

OMICIDX_SOURCES: tuple[ops.Source, ...] = (
    ops.Source(
        "sra", "omicidx",
        "NCBI SRA study/sample/experiment/run metadata (XML → typed projections)",
        "daily", "ncbi-sra-mirror", "us-public-domain",
        watermark_strategy="date",
    ),
    ops.Source(
        "geo", "omicidx",
        "NCBI GEO series/sample/platform metadata (SOFT)",
        "daily", "ncbi-geo", "us-public-domain",
    ),
    ops.Source(
        "biosample", "omicidx",
        "NCBI BioSample records",
        "daily", "ncbi-ftp", "us-public-domain",
    ),
    ops.Source(
        "ebi_biosample", "omicidx",
        "EBI BioSamples records",
        "daily", "ebi-biosamples", "ebi-terms",
    ),
    ops.Source(
        "pubmed", "omicidx",
        "NCBI PubMed baseline + daily-update citations",
        "daily", "ncbi-pubmed", "nlm-terms",
    ),
    ops.Source(
        "bioproject", "omicidx",
        "NCBI BioProject records",
        "daily", "ncbi-ftp", "us-public-domain",
    ),
)
