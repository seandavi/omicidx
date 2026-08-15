"""DuckLake load task: MERGE + DELETE raw pubmed → lake.<schema>.pubmed_article.

Design: full-snapshot MERGE (no high-water-mark)
-------------------------------------------------
PubMed raw Parquet is NOT hive-partitioned by date — it lives at a flat
path (``pubmed/raw/*.parquet``) that mixes baseline and daily-update
files. Because PubMed update packages can revise *any* historical PMID
(date_revised is not monotone across files), scoping the MERGE source by
a high-water mark would silently miss back-dated revisions. A full-
snapshot read is therefore correct.

The `upsert` change-gate makes this efficient: unchanged rows (IS DISTINCT
FROM matches nothing) are not updated, so DuckLake (copy-on-write) writes
no new data files for them and only a trivial catalog snapshot is
produced. The merge is therefore incremental at the storage level even
though the SQL source is a full scan.

DELETE handling
---------------
Raw parquet rows with ``delete IS TRUE`` are PubMed retraction/deletion
records. After the upsert (which excludes deleted PMIDs via
``WHERE delete IS NOT TRUE``) we run a DELETE inside the same ``ops.run``
block, attributed via ``r.attribute("deletes")``, to purge any previously
loaded PMIDs that subsequently appeared as deletions.
"""

import logging

from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_duckdb_path, get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.run import retry, run_id

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source projection
# ---------------------------------------------------------------------------

# Mirrors the SELECT in consolidate.pubmed_parquet (authoritative column list)
# with the following additions:
#   - WHERE delete IS NOT TRUE  (exclude retraction records from the live table)
#   - QUALIFY dedup by (pmid, date_revised DESC, date_completed DESC)
#
# Only {path} is a Python format placeholder.
_PUBMED_SOURCE = """
SELECT * EXCLUDE (rn) FROM (
    SELECT
        trim(pmid)                     AS pmid,
        trim(title)                    AS title,
        trim(issue)                    AS issue,
        trim(pages)                    AS pages,
        trim(abstract)                 AS abstract,
        trim(journal)                  AS journal,
        authors,
        trim(pubdate)                  AS pubdate,
        trim(mesh_terms)               AS mesh_terms,
        trim(publication_types)        AS publication_types,
        trim(chemical_list)            AS chemical_list,
        trim(keywords)                 AS keywords,
        trim(doi)                      AS doi,
        "references",
        trim(languages)                AS languages,
        trim(vernacular_title)         AS vernacular_title,
        trim(date_completed)           AS date_completed,
        trim(date_revised)             AS date_revised,
        trim(pmc)                      AS pmc,
        trim(other_id)                 AS other_id,
        trim(medline_ta)               AS medline_ta,
        trim(nlm_unique_id)            AS nlm_unique_id,
        trim(issn_linking)             AS issn_linking,
        trim(country)                  AS country,
        grant_ids,
        row_number() OVER (
            PARTITION BY pmid
            ORDER BY
                TRY_CAST(date_revised  AS DATE) DESC NULLS LAST,
                TRY_CAST(date_completed AS DATE) DESC NULLS LAST
        ) AS rn
    FROM read_parquet('{path}')
    WHERE delete IS NOT TRUE
) WHERE rn = 1
"""

# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------


@retry
def pubmed_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Upsert raw pubmed → lake.<lake_schema>.pubmed_article; DELETE retracted PMIDs.

    Full-snapshot strategy: all raw parquet files are scanned on every run.
    Unchanged rows generate no data writes in DuckLake (upsert gates on IS
    DISTINCT FROM). Rows with ``delete IS TRUE`` are excluded from the upsert
    and then explicitly deleted from the lake table in the same ``ops.run``
    block, attributed via ``r.attribute("deletes")`` (its own snapshot).

    Returns the ``ops.run`` summary plus ``deleted_count`` (PMIDs removed by
    the delete pass).
    """
    raw = get_duckdb_path("pubmed", "raw", "*.parquet")
    source_sql = _PUBMED_SOURCE.format(path=raw)
    target = f"lake.{lake_schema}.pubmed_article"

    # PubMed signals article deletions via rows with delete=TRUE in raw.
    # These rows are excluded from the upsert above; now remove any
    # previously loaded PMIDs that appear in the delete set.
    delete_set = (
        f"SELECT DISTINCT trim(pmid) FROM read_parquet('{raw}') WHERE delete IS TRUE"
    )

    with get_lake_connection() as con:
        log.info(f"Merging {raw} → {target}")
        with ops.run(
            con,
            source="pubmed",
            target=target,
            extra={"run_id": run_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="pmid")
            log.info(f"{target} holds {r.rows:,} rows after merge")

            # DELETE in its own attributed snapshot inside the same run.
            with r.attribute("deletes"):
                # DuckDB has no changes(); count rows that will go first.
                deleted_count = con.execute(
                    f"SELECT count(*) FROM {target} WHERE pmid IN ({delete_set})"
                ).fetchone()[0]
                con.execute(f"DELETE FROM {target} WHERE pmid IN ({delete_set})")

        log.info(f"Deleted {deleted_count:,} retracted PMIDs from {target}")
        return r.summary() | {"deleted_count": deleted_count}
