"""Transform flow: apply the SQLMesh mart models into the DuckLake catalog.

SQLMesh is omicidx's transform (T) engine over DuckLake. It reads the
`lake.omicidx.*` tables (declared as external models) and materializes the
`src`/`stg`/`geometadb`/`sradb` marts as views inside the lake, at deploy
time — `plan(prod, auto_apply=True)` computes the changed models and does the
virtual-layer swap. It does NOT run in the daily data path and NEVER touches
the raw->lake incremental load (that stays cdsci-lake `upsert`/`ops`).

The project lives at ``transform/`` (config.py + models/ + external_models.yaml),
shipped as package data. See transform/README.md.
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)

TRANSFORM_DIR = Path(__file__).parent.parent / "transform"


def transform(environment: str = "prod") -> None:
    """Plan + auto-apply the SQLMesh marts against the live DuckLake catalog.

    Writes mart views into the lake. Idempotent: an unchanged plan applies
    nothing (SQLMesh reuses fingerprinted models). Run after ``ducklake-load``.
    """
    from sqlmesh import Context

    log.info(f"SQLMesh plan --auto-apply (env={environment}) from {TRANSFORM_DIR}")
    ctx = Context(paths=str(TRANSFORM_DIR))
    ctx.plan(environment=environment, no_prompts=True, auto_apply=True)
    log.info("SQLMesh apply complete")


if __name__ == "__main__":
    transform()
