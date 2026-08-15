"""SQLMesh config for the omicidx transform layer (marts over DuckLake).

Reuses omicidx-prefect's own Settings + libpq parsing so the same `.env`
that drives the pipeline drives SQLMesh — no duplicated creds, no YAML
env_var templating. Attaches the shared cdsci-lake DuckLake catalog as
`lake`; SQLMesh writes mart views into it. SQLMesh state lives in the same
lake Postgres under the `sqlmesh` schema (independent of DuckLake's own
`ducklake_*` metadata; spike-verified to coexist).
"""

from urllib.parse import urlparse

from omicidx.prefect.config import _parse_libpq, settings
from sqlmesh.core.config import Config, GatewayConfig, ModelDefaultsConfig
from sqlmesh.core.config.connection import (
    DuckDBAttachOptions,
    DuckDBConnectionConfig,
    PostgresConnectionConfig,
)

_s = settings()
if not _s.ducklake_uri or not _s.ducklake_data_path:
    raise RuntimeError("DUCKLAKE_URI and DUCKLAKE_DATA_PATH must be set")
_pg = _parse_libpq(_s.ducklake_uri)
_account_id = (urlparse(_s.s3_endpoint).netloc or _s.s3_endpoint).split(".")[0]

# DuckLake attach: catalog metadata in the lake Postgres, data on R2. The
# password is inlined into the libpq DSN SQLMesh embeds in the ATTACH string
# (local process only). DATA_PATH governs only first-time init; the catalog's
# stored path governs existing tables (same note as config.get_ducklake_connection).
_lake_dsn = (
    "postgres:host={host} port={port} dbname={dbname} user={user} password={pw}".format(
        host=_pg["host"],
        port=_pg.get("port", "5432"),
        dbname=_pg["dbname"],
        user=_pg["user"],
        pw=_pg.get("password", ""),
    )
)

connection = DuckDBConnectionConfig(
    extensions=["httpfs", "postgres", "ducklake"],
    secrets=[
        {
            "type": "r2",
            "key_id": _s.s3_access_key_id,
            "secret": _s.s3_secret_access_key,
            "account_id": _account_id,
        }
    ],
    catalogs={
        "lake": DuckDBAttachOptions(
            type="ducklake",
            path=_lake_dsn,
            data_path=_s.ducklake_data_path,
        )
    },
)

state_connection = PostgresConnectionConfig(
    host=_pg["host"],
    port=int(_pg.get("port", "5432")),
    user=_pg["user"],
    password=_pg.get("password", ""),
    database=_pg["dbname"],
)

config = Config(
    # Names this project in the shared SQLMesh state. Load-bearing, not cosmetic:
    # SQLMesh only preserves another project's models when planning a subset if
    # every project names itself (`any(self._projects)` in core/context.py — the
    # default "" is falsy and disables the guard for ALL projects sharing the
    # state). Without this, a second project planning prod would drop omicidx's
    # sradb.*/geometadb.* virtual layer, which parquet_export publishes.
    project="omicidx",
    gateways={
        "lake": GatewayConfig(
            connection=connection,
            state_connection=state_connection,
            state_schema="sqlmesh",
        )
    },
    default_gateway="lake",
    model_defaults=ModelDefaultsConfig(dialect="duckdb"),
)
