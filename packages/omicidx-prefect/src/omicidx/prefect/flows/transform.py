"""Transform flow: build the mart views in the DuckLake catalog (ADR-0008).

The marts (`src` → `stg` → `geometadb.*`/`sradb.*`) are plain SQL files under
``transform/models/<schema>/<name>.sql``, run by cdsci-lake's transform runner
(cdsci-lake ADR-0021). Every model is ``-- materialized: view``, so each run
is a ``CREATE OR REPLACE VIEW lake.<schema>.<name>`` in dependency order,
inside ``ops.run`` (one ``lake_ops.run`` row + attributed snapshot per model,
table/column lineage into ``lake_ops``). It reads the `lake.omicidx.*` tables
and NEVER touches the raw->lake incremental load (cdsci-lake `upsert`/`ops`).
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

MODELS_DIR = Path(__file__).parent.parent / "transform" / "models"


def transform() -> None:
    """Rebuild every mart view in the live DuckLake catalog, dependencies first.

    Idempotent (``CREATE OR REPLACE VIEW``). Run after ``ducklake-load``.
    """
    from cdsci.lake.transform.models import load_models
    from cdsci.lake.transform.runner import run_all
    from omicidx.prefect.config import get_lake_connection

    models = load_models(MODELS_DIR)
    log.info(f"transform: {len(models)} models from {MODELS_DIR}")
    with get_lake_connection() as con:
        rows = run_all(con, models)
    for target, n in rows.items():
        log.info(f"transform: {target}: {n} rows")
    log.info("transform complete")


if __name__ == "__main__":
    transform()
