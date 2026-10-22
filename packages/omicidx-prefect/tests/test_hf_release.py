"""views.sql + dataset card generation for the HF release (offline)."""

from omicidx.prefect.flows.hf_release import build_card, build_views_sql

PATHS = ["sra_studies.parquet", "sradb/study.parquet"]


def test_views_pin_the_release_tag():
    sql = build_views_sql("2026-10", PATHS)
    assert "CREATE SCHEMA IF NOT EXISTS sradb;" in sql
    assert (
        "CREATE OR REPLACE VIEW sra_studies AS SELECT * FROM read_parquet("
        "'hf://datasets/omicidx/omicidx@2026-10/sra_studies.parquet');"
    ) in sql
    assert "VIEW sradb.study AS" in sql
    assert "omicidx@2026-10/sradb/study.parquet" in sql


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
