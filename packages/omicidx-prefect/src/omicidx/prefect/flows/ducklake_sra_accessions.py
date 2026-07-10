"""DuckLake load task: upsert SRA_Accessions.tab → lake.<schema>.sra_accessions.

Full external file, full-scan upsert each run (no watermark): NCBI publishes
a single SRA_Accessions.tab (tens of millions of rows) that is rewritten in
place, so every run reads the whole file. `upsert` gates UPDATEs on IS
DISTINCT FROM, so an unchanged row writes nothing and adds no snapshot.
Null sentinel '-' is handled by read_csv_auto(nullstr='-').
"""

from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA

from prefect import get_run_logger, task
from prefect.runtime import flow_run

_SRA_ACCESSIONS_URL = (
    "https://ftp.ncbi.nlm.nih.gov/sra/reports/Metadata/SRA_Accessions.tab"
)

_SRA_ACCESSIONS_SQL = """
SELECT
    trim("Accession")   AS accession,
    trim("Submission")  AS submission,
    trim("Status")      AS status,
    "Updated"           AS updated,
    "Published"         AS published,
    "Received"          AS received,
    trim("Type")        AS type,
    trim("Center")      AS center,
    trim("Visibility")  AS visibility,
    trim("Alias")       AS alias,
    trim("Experiment")  AS experiment,
    trim("Sample")      AS sample,
    trim("Study")       AS study,
    "Loaded"            AS loaded,
    "Spots"             AS spots,
    "Bases"             AS bases,
    trim("Md5sum")      AS md5sum,
    trim("BioSample")   AS biosample,
    trim("BioProject")  AS bioproject,
    trim("ReplacedBy")  AS replacedby
FROM read_csv_auto(
    '{url}',
    nullstr = '-'
)
"""


@task(retries=1, retry_delay_seconds=60)
def sra_accessions_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Upsert lake.<lake_schema>.sra_accessions from SRA_Accessions.tab."""
    log = get_run_logger()
    source_sql = _SRA_ACCESSIONS_SQL.format(url=_SRA_ACCESSIONS_URL)
    target = f"lake.{lake_schema}.sra_accessions"
    with get_lake_connection() as con:
        log.info(f"Upsert {target} from {_SRA_ACCESSIONS_URL}")
        with ops.run(
            con,
            source="sra",
            target=target,
            extra={"prefect_run_id": flow_run.get_id()},
        ) as r:
            r.rows = upsert(con, target, source_sql, key="accession")
        log.info(f"{target} now holds {r.rows:,} rows")
        return r.summary()
