"""DuckLake load flow for the four SRA entities (incremental by date).

SRA raw lands as hive-partitioned parquet under `sra/raw/<entity>/date=.../
stage=.../*.parquet`. Each entity here is upserted into the DuckLake catalog
by `accession`, reusing the deduped/typed projection from `consolidate.py`
(those SELECTs are authoritative).

Because SRA is large and grows daily, these tasks are *source-incremental*:
a per-entity, per-schema watermark in the lake ledger (source `sra`, name
`<lake_schema>:<entity>`, e.g. `omicidx:sra_study`) stores the highest raw
`date` partition already merged. On a normal run the upsert source only scans
partitions with `date >= <high_water>` (INCLUSIVE — re-reading the boundary
day is a safe no-op because `upsert` gates on IS DISTINCT FROM and DuckLake is
copy-on-write). A `force=True` run drops the filter and scans all partitions
(full backfill / reconciliation).

After a successful upsert the watermark is advanced to `max(date)` present in
raw. `cdsci-lake` (the catalog bucket) is ducklake-controlled exclusively;
raw is read from PUBLISH_ROOT via `get_duckdb_path`.
"""

import duckdb
from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_duckdb_path, get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA

from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run

# ---------------------------------------------------------------------------
# Per-entity source projections.
#
# Each is a python str.format(path=..., filt=...) template:
#   {path} — the r2:// glob for the entity's raw partitions
#   {filt} — "" or "date >= '<high_water>'" (the incremental scope)
# The column lists mirror consolidate.py's sra_*_parquet projections exactly
# (those are authoritative); we add the standard date/stage dedup QUALIFY.
# `upsert` gates UPDATEs on IS DISTINCT FROM, so no `_row_hash` column.
# ---------------------------------------------------------------------------

_STUDY_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(study_accession) AS study_accession,
    trim(alias) AS alias, trim(title) AS title,
    trim(description) AS description,
    trim(abstract) AS abstract,
    trim(study_type) AS study_type,
    trim(center_name) AS center_name,
    trim(broker_name) AS broker_name,
    trim("BioProject") AS bioproject,
    trim("GEO") AS geo,
    identifiers, attributes, xrefs, pubmed_ids
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_SAMPLE_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(alias) AS alias, trim(title) AS title,
    trim(organism) AS organism,
    trim(description) AS description,
    taxon_id,
    trim("BioSample") AS biosample,
    identifiers, attributes, xrefs
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_EXPERIMENT_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(experiment_accession) AS experiment_accession,
    trim(alias) AS alias, trim(title) AS title,
    trim(design) AS design,
    trim(center_name) AS center_name,
    trim(study_accession) AS study_accession,
    trim(sample_accession) AS sample_accession,
    trim(platform) AS platform,
    trim(instrument_model) AS instrument_model,
    trim(library_name) AS library_name,
    trim(library_construction_protocol) AS library_construction_protocol,
    trim(library_layout) AS library_layout,
    trim(library_layout_length) AS library_layout_length,
    trim(library_layout_sdev) AS library_layout_sdev,
    trim(library_strategy) AS library_strategy,
    trim(library_source) AS library_source,
    trim(library_selection) AS library_selection,
    spot_length, nreads,
    identifiers, attributes, xrefs, reads
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""

_RUN_SOURCE = """
SELECT
    trim(accession) AS accession,
    trim(alias) AS alias,
    trim(experiment_accession) AS experiment_accession,
    trim(title) AS title,
    identifiers, attributes, qualities
FROM read_parquet('{path}', hive_partitioning=true)
{filt}
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""


def _max_raw_date(con: duckdb.DuckDBPyConnection, raw: str) -> object | None:
    """Max `date` hive partition present in raw (the new watermark).

    Reads partition metadata only — cheap relative to the merge scan.
    Returns None when no partitions exist so the watermark is left as-is.
    """
    row = con.execute(
        f"SELECT max(date) FROM read_parquet('{raw}', hive_partitioning=true)"
    ).fetchone()
    return row[0] if row else None


def _merge_sra(
    entity: str,
    table: str,
    raw_subdir: str,
    source_template: str,
    lake_schema: str,
    force: bool,
) -> dict:
    """Incrementally upsert one SRA entity's raw partitions into the lake.

    Shared body for the four public tasks. The watermark lives in the lake
    ledger keyed per-entity, per-schema (source `sra`, name
    `<lake_schema>:<entity>`): read it, scope the source to `date >= <wm>`
    (unless first run or force), upsert, then advance it to the max raw
    `date` actually present — all inside one `ops.run` block.
    """
    log = get_run_logger()
    raw = get_duckdb_path("sra", "raw", raw_subdir, "**", "*parquet")
    target = f"lake.{lake_schema}.{table}"
    wm_name = f"{lake_schema}:{entity}"

    with get_lake_connection() as con:
        last = None if force else ops.get_watermark(con, "sra", wm_name)
        if last is not None:
            filt = f"WHERE date >= '{last}'"
            log.info(f"{entity}: incremental scope date >= {last}")
        else:
            filt = ""
            reason = "force=True" if force else "no prior watermark"
            log.info(f"{entity}: full scan ({reason})")

        source_sql = source_template.format(path=raw, filt=filt)
        new_max = _max_raw_date(con, raw)  # partition metadata only; cheap

        log.info(f"Merging {raw} → {target}")
        with ops.run(
            con,
            source="sra",
            target=target,
            version=str(new_max) if new_max is not None else None,
            extra={"prefect_run_id": flow_run.get_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="accession")
            if new_max is not None:
                ops.set_watermark(con, "sra", wm_name, str(new_max), run_id=r.run_id)

        if new_max is not None:
            log.info(f"{entity}: watermark advanced to {new_max}")
        else:
            log.warning(f"{entity}: no raw partitions found; watermark unchanged")
        log.info(f"{target} now holds {r.rows:,} rows")
        return r.summary() | {
            "high_water_from": last,
            "high_water_to": str(new_max) if new_max is not None else None,
            "forced": force,
        }


@task(retries=1, retry_delay_seconds=60)
def sra_study_to_ducklake(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> dict:
    """Upsert raw SRA study partitions → lake.<lake_schema>.sra_study."""
    return _merge_sra(
        entity="sra_study",
        table="sra_study",
        raw_subdir="study",
        source_template=_STUDY_SOURCE,
        lake_schema=lake_schema,
        force=force,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_sample_to_ducklake(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> dict:
    """Upsert raw SRA sample partitions → lake.<lake_schema>.sra_sample."""
    return _merge_sra(
        entity="sra_sample",
        table="sra_sample",
        raw_subdir="sample",
        source_template=_SAMPLE_SOURCE,
        lake_schema=lake_schema,
        force=force,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_experiment_to_ducklake(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> dict:
    """Upsert raw SRA experiment partitions → lake.<lake_schema>.sra_experiment."""
    return _merge_sra(
        entity="sra_experiment",
        table="sra_experiment",
        raw_subdir="experiment",
        source_template=_EXPERIMENT_SOURCE,
        lake_schema=lake_schema,
        force=force,
    )


@task(retries=1, retry_delay_seconds=60)
def sra_run_to_ducklake(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> dict:
    """Upsert raw SRA run partitions → lake.<lake_schema>.sra_run."""
    return _merge_sra(
        entity="sra_run",
        table="sra_run",
        raw_subdir="run",
        source_template=_RUN_SOURCE,
        lake_schema=lake_schema,
        force=force,
    )


@flow(name="ducklake-load-sra")
def ducklake_load_sra_flow(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> None:
    """Merge all four SRA entities into the lake (order unconstrained)."""
    sra_study_to_ducklake(lake_schema=lake_schema, force=force)
    sra_sample_to_ducklake(lake_schema=lake_schema, force=force)
    sra_experiment_to_ducklake(lake_schema=lake_schema, force=force)
    sra_run_to_ducklake(lake_schema=lake_schema, force=force)


if __name__ == "__main__":
    ducklake_load_sra_flow()
