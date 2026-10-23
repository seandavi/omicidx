"""views.sql + dataset card generation for the HF release (offline)."""

from types import SimpleNamespace

import pytest
from omicidx.prefect.config import Settings
from omicidx.prefect.flows import hf_release
from omicidx.prefect.flows.hf_release import build_card, build_views_sql

PATHS = ["sra_studies.parquet", "sradb/study.parquet"]
HF_REPO_ID = Settings.model_fields["hf_repo_id"].default


@pytest.fixture(autouse=True)
def _repo_id(monkeypatch):
    monkeypatch.setattr(
        hf_release, "settings", lambda: SimpleNamespace(hf_repo_id=HF_REPO_ID)
    )


def test_default_repo_is_the_maintainers():
    assert HF_REPO_ID == "seandavis/omicidx"


def test_views_pin_the_release_tag():
    sql = build_views_sql("2026-10", PATHS)
    assert "CREATE SCHEMA IF NOT EXISTS sradb;" in sql
    assert (
        "CREATE OR REPLACE VIEW sra_studies AS SELECT * FROM read_parquet("
        "'hf://datasets/seandavis/omicidx@2026-10/sra_studies.parquet');"
    ) in sql
    assert "VIEW sradb.study AS" in sql
    assert "seandavis/omicidx@2026-10/sradb/study.parquet" in sql


def test_card_lists_tables_and_provenance():
    manifest = {"publish_date": "2026-10-20", "transform_sha": "abc123"}
    tables = [
        {"path": "sra_studies.parquet", "rows": 774584, "bytes": 176_770_978},
        {"path": "sradb/study.parquet", "rows": 774584, "bytes": 147_199_125},
    ]
    card = build_card("2026-10", manifest, tables)
    assert card.startswith("---\nlicense: other\n")
    assert "- config_name: sradb_study\n  data_files: sradb/study.parquet" in card
    assert "| `sra_studies` | `sra_studies.parquet` | 774,584 | 0.18 |" in card
    assert "v2026-10-20/" in card and "`abc123`" in card
    assert "https://data.omicidx.cancerdatasci.org/latest/" in card
    assert "hf://datasets/seandavis/omicidx@2026-10/sra_studies.parquet" in card
    assert "omicidx/omicidx" not in card
