"""Dataset card generation for the HF release (offline)."""

from types import SimpleNamespace

import pytest
import yaml
from omicidx.prefect.config import Settings
from omicidx.prefect.flows import hf_release
from omicidx.prefect.flows.hf_release import build_card

PATHS = ["sra_studies.parquet", "sradb/study.parquet"]
HF_REPO_ID = Settings.model_fields["hf_repo_id"].default


@pytest.fixture(autouse=True)
def _repo_id(monkeypatch):
    monkeypatch.setattr(
        hf_release, "settings", lambda: SimpleNamespace(hf_repo_id=HF_REPO_ID)
    )


def test_default_repo_is_the_maintainers():
    assert HF_REPO_ID == "seandavis/omicidx"


def test_card_lists_tables_and_provenance():
    manifest = {"publish_date": "2026-10-20", "transform_sha": "abc123"}
    # The manifest's tables (schema/table/path/row_count/...), with bytes added
    # by hf_push.
    tables = [
        {"path": "sra_studies.parquet", "row_count": 774584, "bytes": 176_770_978},
        {"path": "sradb/study.parquet", "row_count": 774584, "bytes": 147_199_125},
    ]
    card = build_card("2026-10", manifest, tables)
    assert card.startswith("---\nlicense: other\n")
    assert "- config_name: sradb_study\n  data_files: sradb/study.parquet" in card
    assert "| `sra_studies` | `sra_studies.parquet` | 774,584 | 0.18 |" in card
    assert "v2026-10-20/" in card and "`abc123`" in card
    assert "https://data.omicidx.cancerdatasci.org/latest/" in card
    assert "hf://datasets/seandavis/omicidx@2026-10/sra_studies.parquet" in card
    # views.sql on HF is relative to the download (cdsci.publish.hf), not hf://.
    assert "--revision 2026-10 --local-dir" in card
    assert "omicidx/omicidx" not in card
    assert "cdsci-lake/issues/136" in card
    assert "and PubMed" not in card  # intro source list
    assert "BioProject, PubMed" not in card  # license provider list


def test_card_has_one_config_per_table():
    """HF Data Studio and load_dataset(repo, name) need a config per table (#225)."""
    tables = [{"path": p, "row_count": 1, "bytes": 1} for p in PATHS]
    front_matter = build_card("2026-10", {"publish_date": "2026-10-20"}, tables)
    configs = yaml.safe_load(front_matter.split("---")[1])["configs"]
    assert configs == [
        {"config_name": "sra_studies", "data_files": "sra_studies.parquet"},
        {"config_name": "sradb_study", "data_files": "sradb/study.parquet"},
    ]
