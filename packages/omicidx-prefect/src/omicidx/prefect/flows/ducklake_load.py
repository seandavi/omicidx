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


# ponytail: timeouts are sized ~3x the slowest completed run in Prefect's
# history, purely to stop orphans (a ducklake-load once sat Running 35 days).
# Not a SLA — raise the number if a legitimate run ever trips it.
def ducklake_load(lake_schema: str = LAKE_SCHEMA, force: bool = False) -> None:
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


# ponytail: the weekly-maintenance wrapper that used to sit here existed only
# to be a second deployable @flow. `ducklake.ducklake_maintenance` is the real
# thing and already documents the retention contract; it runs independently of
# this module on `systemd/omicidx-ducklake-maintenance.timer`.


if __name__ == "__main__":
    ducklake_load()
