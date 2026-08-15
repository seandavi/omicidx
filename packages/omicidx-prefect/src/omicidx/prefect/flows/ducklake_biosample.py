"""DuckLake load task: upsert raw biosample → lake.<schema>.biosample.

Follows the same pattern as bioproject_to_ducklake in ducklake.py:
  - Typed projection from read_ndjson_auto on the raw JSONL.GZ
  - QUALIFY dedup on accession (one row / accession already, but defensive)
  - `upsert` gates no-op UPDATEs via IS DISTINCT FROM (no `_row_hash` column)
  - `ops.run` records the load + self-attributes the DuckLake snapshot
"""

import logging

from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_duckdb_path, get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.run import retry, run_id

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source SQL template — {path} is the only format token.
# ---------------------------------------------------------------------------

_BIOSAMPLE_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(submission_date) AS submission_date,
        trim(last_update)     AS last_update,
        trim(publication_date) AS publication_date,
        trim(access)          AS access,
        trim(id)              AS id,
        trim(accession)       AS accession,
        id_recs,
        ids,
        trim(sra_sample)      AS sra_sample,
        trim(dbgap)           AS dbgap,
        trim(gsm)             AS gsm,
        trim(title)           AS title,
        trim(description)     AS description,
        trim(taxonomy_name)   AS taxonomy_name,
        taxon_id,
        attribute_recs,
        attributes,
        trim(model)           AS model,
        row_number() OVER (
            PARTITION BY trim(accession) ORDER BY last_update DESC NULLS LAST
        ) AS rn
    FROM read_ndjson_auto('{path}', maximum_object_size = 1000000000)
    WHERE accession IS NOT NULL AND trim(accession) <> ''
) WHERE rn = 1
"""


@retry
def biosample_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Upsert raw biosample JSONL → lake.<lake_schema>.biosample."""
    raw = get_duckdb_path("biosample", "raw", "data.jsonl.gz")
    source_sql = _BIOSAMPLE_SOURCE.format(path=raw)
    target = f"lake.{lake_schema}.biosample"
    with get_lake_connection() as con:
        log.info(f"Merging {raw} → {target}")
        with ops.run(
            con,
            source="biosample",
            target=target,
            extra={"run_id": run_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="accession")
        log.info(f"{target} now holds {r.rows:,} rows")
        return r.summary()
