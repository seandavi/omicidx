"""DuckLake load task: MERGE raw ebi_biosample → lake.<schema>.ebi_biosample.

Follows the same pattern as bioproject_to_ducklake in ducklake.py:
  - Typed projection from read_ndjson_auto on the raw per-day NDJSON.GZ files,
    incremental like the SRA loaders: only days the extractor marked done, from
    the lake-ledger watermark (inclusive) on, so a night reads ~1 day, not all
    ~1,500 files (#79). `force=True` re-reads every done day.
  - The read is typed from the lake table's own columns (`read_json` with
    `columns=`), not inferred: one day can lack whole keys (2026-10-30 has no
    `domain`, `relationships` or `certificates`), and an inferred schema then
    lets `trim(domain) AS domain` bind to its own alias (Binder Error). Missing
    keys read as NULL; unlisted keys (`_links`, `structuredData`) are ignored.
    Only a fresh lake with no table yet falls back to inferring
    (`read_ndjson_auto`, union_by_name=true).
  - QUALIFY dedup on accession (the EBI BioSample API returns one record per
    accession per day; dedup is defensive for the rare same-accession update
    that crosses a day boundary)
  - `upsert` gates no-op UPDATEs via IS DISTINCT FROM (no `_row_hash` column)
  - `ops.run` records the load + self-attributes the DuckLake snapshot

Raw schema (from DESCRIBE read_ndjson_auto, union_by_name=true):
  accession VARCHAR            — natural key (EBI BioSample accession)
  name VARCHAR
  domain VARCHAR
  status VARCHAR
  release VARCHAR              — ISO 8601 timestamp string
  update VARCHAR               — ISO 8601 timestamp string (recency field)
  submitted VARCHAR            — ISO 8601 timestamp string
  create VARCHAR               — ISO 8601 timestamp string
  taxId BIGINT
  sraAccession VARCHAR
  submittedVia VARCHAR
  webinSubmissionAccountId VARCHAR
  characteristics STRUCT(...)[]
  externalReferences STRUCT(url VARCHAR, duo VARCHAR[])[]
  relationships STRUCT(source VARCHAR, type VARCHAR, target VARCHAR)[]
  publications STRUCT(pubmed_id VARCHAR, doi VARCHAR)[]
  organization STRUCT(...)[]
  contact STRUCT(...)[]
  certificates STRUCT(name VARCHAR, version VARCHAR, fileName VARCHAR)[]

Excluded from projection:
  _links — internal HAL navigation, not useful for analytics
  structuredData — deeply nested AMR/assay content, very sparse
"""

import logging

import duckdb
from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_duckdb_path, get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.run import retry, run_id
from omicidx.prefect.semaphore import SemaphoreStore

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source SQL template — {reader} (see `_reader`) is the only format token.
# ---------------------------------------------------------------------------

_EBI_BIOSAMPLE_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(accession)                 AS accession,
        trim(name)                      AS name,
        trim(domain)                    AS domain,
        trim(status)                    AS status,
        trim(release)                   AS release,
        trim("update")                  AS update,
        trim(submitted)                 AS submitted,
        trim("create")                  AS create,
        taxId                           AS taxId,
        trim(sraAccession)              AS sraAccession,
        trim(submittedVia)              AS submittedVia,
        trim(webinSubmissionAccountId)  AS webinSubmissionAccountId,
        characteristics,
        externalReferences,
        relationships,
        publications,
        organization,
        contact,
        certificates,
        row_number() OVER (
            PARTITION BY trim(accession)
            ORDER BY trim("update") DESC NULLS LAST
        ) AS rn
    FROM {reader}
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""


def _done_days(since: str | None) -> tuple[list[str], str | None]:
    """Paths of the raw day files the extractor marked done, and their max day.

    Only days with a semaphore and ``day >= since`` are in scope. A day with no
    updated samples is marked done with ``row_count: 0`` and writes no file, so
    it is skipped rather than handed to ``read_ndjson_auto`` as a missing path.
    """
    sem = SemaphoreStore("ebi_biosample")
    days = []
    for day in sorted(sem.list_keys()):
        if since is not None and day < since:
            continue
        if (sem.read(day) or {}).get("metadata", {}).get("row_count", 1) == 0:
            continue
        days.append(day)
    paths = [
        get_duckdb_path("ebi_biosample", "raw", f"biosamples-{d}.ndjson.gz")
        for d in days
    ]
    return paths, max(days, default=None)


def _reader(con, target: str, paths: list[str]) -> str:
    """The table function that reads `paths`, typed from `target` when it exists."""
    files = "[" + ", ".join(f"'{p}'" for p in paths) + "]"
    try:
        cols = con.sql(f"DESCRIBE {target}").fetchall()
    except duckdb.CatalogException:
        return (
            f"read_ndjson_auto({files}, maximum_object_size = 1000000000, "
            "union_by_name = true)"
        )
    spec = "{" + ", ".join(f"'{name}': '{type_}'" for name, type_, *_ in cols) + "}"
    return (
        f"read_json({files}, format = 'newline_delimited', columns = {spec}, "
        "maximum_object_size = 1000000000)"
    )


@retry
def ebi_biosample_to_ducklake(
    lake_schema: str = LAKE_SCHEMA, force: bool = False
) -> dict:
    """Incrementally upsert raw ebi_biosample day files → lake.<lake_schema>.ebi_biosample.

    Same shape as ``ducklake_sra._merge_sra``: the watermark lives in the lake
    ledger (source ``ebi_biosample``, name ``<lake_schema>:ebi_biosample``);
    read it, merge the done days from it on, advance it to the max day merged,
    all inside one ``ops.run`` block.
    """
    target = f"lake.{lake_schema}.ebi_biosample"
    wm_name = f"{lake_schema}:ebi_biosample"
    with get_lake_connection() as con:
        last = None if force else ops.get_watermark(con, "ebi_biosample", wm_name)
        if last is not None:
            log.info(f"ebi_biosample: incremental scope day >= {last}")
        else:
            reason = "force=True" if force else "no prior watermark"
            log.info(f"ebi_biosample: full scan ({reason})")

        paths, new_max = _done_days(last)
        if not paths:
            log.warning("ebi_biosample: no completed raw days in scope; skipping")
            return {"high_water_from": last, "high_water_to": last, "forced": force}
        source_sql = _EBI_BIOSAMPLE_SOURCE.format(reader=_reader(con, target, paths))

        log.info(f"Merging {len(paths)} completed days → {target}")
        with ops.run(
            con,
            source="ebi_biosample",
            target=target,
            version=new_max,
            extra={"run_id": run_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="accession")
            ops.set_watermark(con, "ebi_biosample", wm_name, new_max, run_id=r.run_id)

        log.info(f"ebi_biosample: watermark advanced to {new_max}")
        log.info(f"{target} now holds {r.rows:,} rows")
        return r.summary() | {
            "high_water_from": last,
            "high_water_to": new_max,
            "forced": force,
        }
