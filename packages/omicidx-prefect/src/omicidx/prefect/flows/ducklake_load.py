"""Top-level DuckLake load flow.

Assembles every per-entity loader into one flow that upserts raw data into
`lake.<schema>.*`. Sits between `raw-extract` and `postgres-load` in the
daily pipeline (wired in P3). Shared helpers live in `ducklake.py`; each
entity's source projection + task lives in its own `ducklake_<entity>.py`
module so they can evolve independently.

Targets `LAKE_SCHEMA` (`omicidx`; pass an explicit `lake_schema` to target
a dev schema for validation).
"""

from cdsci.lake import ops
from omicidx.prefect.config import get_lake_connection
from omicidx.prefect.flows.ducklake import (
    LAKE_SCHEMA,
    bioproject_to_ducklake,
    ducklake_maintenance,
)
from omicidx.prefect.flows.ducklake_biosample import biosample_to_ducklake
from omicidx.prefect.flows.ducklake_geo import (
    geo_platform_to_ducklake,
    geo_sample_to_ducklake,
    geo_series_to_ducklake,
)
from omicidx.prefect.flows.ducklake_geo_rnaseq_counts import (
    geo_rnaseq_counts_to_ducklake,
)
from omicidx.prefect.flows.ducklake_pubmed import pubmed_to_ducklake
from omicidx.prefect.flows.ducklake_sra import (
    sra_experiment_to_ducklake,
    sra_run_to_ducklake,
    sra_sample_to_ducklake,
    sra_study_to_ducklake,
)
from omicidx.prefect.flows.ducklake_sra_accessions import sra_accessions_to_ducklake
from omicidx.prefect.flows.sources import OMICIDX_SOURCES

from prefect import flow


# ponytail: timeouts are sized ~3x the slowest completed run in Prefect's
# history, purely to stop orphans (a ducklake-load once sat Running 35 days).
# Not a SLA — raise the number if a legitimate run ever trips it.
@flow(name="ducklake-load", timeout_seconds=14400)  # slowest completed: 78m
def ducklake_load_flow(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> None:
    """Upsert every entity's raw data into the DuckLake catalog.

    Tasks are independent (distinct lake tables); order is unconstrained.
    SRA loaders are high-water-mark incremental; the rest are full-snapshot
    with the `upsert` IS DISTINCT FROM gate. PubMed also applies deletes.

    `force=True` is the **reproduce-from-raw** mode (spec §1, A3): it drops the
    SRA high-water-mark filter so every retained raw partition is re-scanned,
    reproducing lake state from raw. The full-snapshot loaders already read all
    raw every run, so they need no flag. Reproducing is idempotent — an
    unchanged re-run writes zero new data files (see `tests/test_idempotency.py`).
    """
    # Register omicidx's sources under producer `omicidx` (idempotent,
    # self-healing; cdsci-lake ADR-0011 §4) before any loader runs.
    with get_lake_connection() as con:
        ops.register_sources(con, writer="omicidx", sources=OMICIDX_SOURCES)

    bioproject_to_ducklake(lake_schema=lake_schema)
    biosample_to_ducklake(lake_schema=lake_schema)
    geo_series_to_ducklake(lake_schema=lake_schema)
    geo_sample_to_ducklake(lake_schema=lake_schema)
    geo_platform_to_ducklake(lake_schema=lake_schema)
    geo_rnaseq_counts_to_ducklake(lake_schema=lake_schema)
    sra_study_to_ducklake(lake_schema=lake_schema, force=force)
    sra_sample_to_ducklake(lake_schema=lake_schema, force=force)
    sra_experiment_to_ducklake(lake_schema=lake_schema, force=force)
    sra_run_to_ducklake(lake_schema=lake_schema, force=force)
    sra_accessions_to_ducklake(lake_schema=lake_schema)
    pubmed_to_ducklake(lake_schema=lake_schema)


@flow(name="ducklake-maintenance")
def ducklake_maintenance_flow(
    retention_days: int | None = None,
    compact: bool = True,
) -> None:
    """Scheduled (weekly) catalog maintenance: cleanup + compaction.

    Runs independently of ducklake-load. Retention is UNBOUNDED by default
    (spec §1: the internal lake is the primary time machine; raw Parquet under
    PUBLISH_ROOT is the re-derivation backstop). The weekly deployment passes no
    parameters, so it coalesces small parquet files and cleans truly
    unreferenced files without ever expiring snapshots. Pass an explicit
    ``retention_days`` to deliberately re-enable bounded expiry — the only path
    that removes history.
    """
    ducklake_maintenance(retention_days=retention_days, compact=compact)


if __name__ == "__main__":
    ducklake_load_flow()
