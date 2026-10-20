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

import logging

from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_duckdb_path, get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.run import retry, run_id
from omicidx.prefect.semaphore import SemaphoreStore

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-entity source projections.
#
# Each is a python str.format(paths=...) template:
#   {paths} — SQL list of r2:// globs, one per *completed* raw partition in
#             scope (see `_done_partitions`)
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
FROM read_parquet({paths}, hive_partitioning=true)
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
FROM read_parquet({paths}, hive_partitioning=true)
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
FROM read_parquet({paths}, hive_partitioning=true)
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
FROM read_parquet({paths}, hive_partitioning=true)
QUALIFY row_number() OVER (
    PARTITION BY accession ORDER BY date DESC, stage DESC
) = 1
"""


def _done_partitions(
    raw_subdir: str, since: str | None
) -> tuple[list[str], str | None]:
    """Globs for the raw partitions the extractor marked done, and their max date.

    Raw parquet chunks land while the mirror file is parsed and the semaphore is
    written only at the end, so globbing the raw tree would MERGE a partial
    partition from an in-flight or killed extract (#189). Only keys with a
    semaphore under ``sra/<raw_subdir>`` (``{date}_{stage}``) and ``date >=
    since`` are in scope. A partition that parsed to zero rows wrote no files,
    so it is skipped rather than handed to ``read_parquet`` as an empty glob.
    """
    sem = SemaphoreStore(f"sra/{raw_subdir}")
    picked = []
    for key in sorted(sem.list_keys()):
        date, stage = key.split("_", 1)
        if since is not None and date < since:
            continue
        doc = sem.read(key) or {}
        if doc.get("metadata", {}).get("parquet_parts", 1) == 0:
            continue
        picked.append((date, stage))
    globs = [
        get_duckdb_path(
            "sra", "raw", raw_subdir, f"date={d}", f"stage={st}", "*parquet"
        )
        for d, st in picked
    ]
    return globs, max((d for d, _ in picked), default=None)


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
    (unless first run or force), upsert, then advance it to the max `date`
    merged — all inside one `ops.run` block. Only semaphore-marked partitions
    are read (`_done_partitions`).
    """
    target = f"lake.{lake_schema}.{table}"
    wm_name = f"{lake_schema}:{entity}"

    with get_lake_connection() as con:
        last = None if force else ops.get_watermark(con, "sra", wm_name)
        if last is not None:
            log.info(f"{entity}: incremental scope date >= {last}")
        else:
            reason = "force=True" if force else "no prior watermark"
            log.info(f"{entity}: full scan ({reason})")

        globs, new_max = _done_partitions(raw_subdir, last)
        if not globs:
            log.warning(f"{entity}: no completed raw partitions in scope; skipping")
            return {"high_water_from": last, "high_water_to": last, "forced": force}
        source_sql = source_template.format(
            paths="[" + ", ".join(f"'{g}'" for g in globs) + "]"
        )

        log.info(f"Merging {len(globs)} completed partitions → {target}")
        with ops.run(
            con,
            source="sra",
            target=target,
            version=new_max,
            extra={"run_id": run_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="accession")
            ops.set_watermark(con, "sra", wm_name, new_max, run_id=r.run_id)

        log.info(f"{entity}: watermark advanced to {new_max}")
        log.info(f"{target} now holds {r.rows:,} rows")
        return r.summary() | {
            "high_water_from": last,
            "high_water_to": new_max,
            "forced": force,
        }


@retry
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


@retry
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


@retry
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


@retry
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


def ducklake_load_sra(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> None:
    """Merge all four SRA entities into the lake (order unconstrained)."""
    sra_study_to_ducklake(lake_schema=lake_schema, force=force)
    sra_sample_to_ducklake(lake_schema=lake_schema, force=force)
    sra_experiment_to_ducklake(lake_schema=lake_schema, force=force)
    sra_run_to_ducklake(lake_schema=lake_schema, force=force)


if __name__ == "__main__":
    ducklake_load_sra()
