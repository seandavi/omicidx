"""DuckLake load: newest raw taxdump → lake.<schema>.ncbi_taxonomy{,_merged} (#226).

- ``ncbi_taxonomy``: one row per live taxon — ``tax_id``, ``scientific_name``,
  ``rank``, ``parent_tax_id`` (nodes.dmp joined to names.dmp's scientific name).
  Upserted on ``tax_id``; taxa missing from the newest dump (merged or deleted
  since) are then deleted, so the table mirrors current NCBI Taxonomy.
- ``ncbi_taxonomy_merged``: merged.dmp, ``old_tax_id`` → ``new_tax_id``. The
  transform follows it so a record still citing a merged taxid gets the
  surviving taxon's name.

The three ``.dmp`` members are pulled out of the tarball into a scratch dir and
parsed by DuckDB; the ``*_sql`` builders are the test seam.
"""

import logging
import tarfile
import tempfile

from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.flows.ncbi_taxonomy import latest_taxdump
from omicidx.prefect.run import retry, run_id

log = logging.getLogger(__name__)

DMP_FILES = ("names.dmp", "nodes.dmp", "merged.dmp")


def _dmp_fields(path: str) -> str:
    """Relation of one .dmp file as ``f VARCHAR[]`` rows.

    .dmp lines are ``\\t|\\t``-separated and ``\\t|``-terminated. Read whole lines
    (``chr(1)`` never occurs) and split, so a column NCBI appends later cannot
    break the load.
    """
    return (
        "(SELECT string_split(regexp_replace(line, '\t\\|$', ''), '\t|\t') AS f "
        f"FROM read_csv('{path}', delim = chr(1), quote = '', escape = '', "
        "header = false, columns = {'line': 'VARCHAR'}))"
    )


def ncbi_taxonomy_sql(dmp_dir: str) -> str:
    """One row per node: tax_id, scientific_name, rank, parent_tax_id."""
    return f"""
SELECT n.tax_id, s.scientific_name, n.rank, n.parent_tax_id
FROM (
    SELECT CAST(f[1] AS BIGINT) AS tax_id, CAST(f[2] AS BIGINT) AS parent_tax_id,
           f[3] AS rank
    FROM {_dmp_fields(f"{dmp_dir}/nodes.dmp")}
) n
LEFT JOIN (
    SELECT CAST(f[1] AS BIGINT) AS tax_id, f[2] AS scientific_name
    FROM {_dmp_fields(f"{dmp_dir}/names.dmp")}
    WHERE f[4] = 'scientific name'
) s USING (tax_id)
QUALIFY row_number() OVER (PARTITION BY n.tax_id ORDER BY s.scientific_name) = 1
"""


def ncbi_taxonomy_merged_sql(dmp_dir: str) -> str:
    """merged.dmp: old_tax_id → new_tax_id."""
    return f"""
SELECT CAST(f[1] AS BIGINT) AS old_tax_id, CAST(f[2] AS BIGINT) AS new_tax_id
FROM {_dmp_fields(f"{dmp_dir}/merged.dmp")}
QUALIFY row_number() OVER (PARTITION BY old_tax_id ORDER BY new_tax_id) = 1
"""


@retry
def ncbi_taxonomy_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Upsert the newest landed taxdump into the two taxonomy lake tables."""
    raw = latest_taxdump()
    if raw is None:
        log.warning("No ncbi_taxonomy raw landed yet; skipping its load")
        return {"skipped": True}

    target = f"lake.{lake_schema}.ncbi_taxonomy"
    merged_target = f"lake.{lake_schema}.ncbi_taxonomy_merged"
    with tempfile.TemporaryDirectory() as dmp_dir:
        with raw.open("rb") as f, tarfile.open(fileobj=f, mode="r|gz") as tar:
            for member in tar:
                if member.name in DMP_FILES:
                    tar.extract(member, dmp_dir, filter="data")
        nodes_sql = ncbi_taxonomy_sql(dmp_dir)

        with get_lake_connection() as con:
            log.info(f"Merging {raw} → {target}, {merged_target}")
            with ops.run(
                con,
                source="ncbi_taxonomy",
                target=target,
                extra={"run_id": run_id(), "raw": str(raw)},
            ) as r:
                upsert(con, target, nodes_sql, key="tax_id")
                with r.attribute("deletes"):
                    gone = f"tax_id NOT IN (SELECT tax_id FROM ({nodes_sql}))"
                    deleted = con.execute(
                        f"SELECT count(*) FROM {target} WHERE {gone}"
                    ).fetchone()[0]
                    con.execute(f"DELETE FROM {target} WHERE {gone}")
                r.rows = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
            with ops.run(
                con,
                source="ncbi_taxonomy",
                target=merged_target,
                extra={"run_id": run_id(), "raw": str(raw)},
            ) as m:
                m.rows = upsert(
                    con,
                    merged_target,
                    ncbi_taxonomy_merged_sql(dmp_dir),
                    key="old_tax_id",
                )
    log.info(
        f"{target} holds {r.rows:,} taxa ({deleted:,} dropped); "
        f"{merged_target} holds {m.rows:,} merges"
    )
    return r.summary() | {"deleted_count": deleted, "merged_rows": m.rows}
