"""DuckLake FULL-REPLACE derived loader for geo_series_with_rnaseq_counts.

A single-column accession list from the GEO RNA-seq counts parquet, always
recomputed from scratch via `replace_to_ducklake` (CREATE OR REPLACE TABLE in
a single stamped transaction). Kept intact from the pre-cdsci-lake write path.
"""

from omicidx.prefect.config import get_duckdb_path, get_ducklake_connection
from omicidx.prefect.flows.ducklake import (
    LAKE_SCHEMA,
    _commit_extra,
    replace_to_ducklake,
)

from prefect import get_run_logger, task


@task(retries=1, retry_delay_seconds=60)
def geo_rnaseq_counts_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Full-replace lake.<lake_schema>.geo_series_with_rnaseq_counts."""
    log = get_run_logger()
    raw = get_duckdb_path("geo", "raw", "gse_with_rna_seq_counts.parquet")
    source_sql = f"SELECT accession FROM read_parquet('{raw}') ORDER BY accession"
    log.info(
        f"Full-replace lake.{lake_schema}.geo_series_with_rnaseq_counts from {raw}"
    )
    with get_ducklake_connection() as con:
        rows = replace_to_ducklake(
            con,
            schema=lake_schema,
            table="geo_series_with_rnaseq_counts",
            source_sql=source_sql,
            commit_message=(
                f"ducklake-load: geo_series_with_rnaseq_counts -> {lake_schema}"
            ),
            commit_extra_info=_commit_extra(
                entity="geo_series_with_rnaseq_counts",
                source=raw,
            ),
        )
    log.info(
        f"lake.{lake_schema}.geo_series_with_rnaseq_counts now holds {rows:,} rows"
    )
    return {
        "table": f"{lake_schema}.geo_series_with_rnaseq_counts",
        "row_count": rows,
    }
