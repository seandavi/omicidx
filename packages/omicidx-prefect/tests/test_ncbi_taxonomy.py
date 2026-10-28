"""NCBI Taxonomy (#226): the taxdump parsers and the stg views that resolve names."""

import duckdb
from omicidx.prefect.flows.ducklake_ncbi_taxonomy import (
    ncbi_taxonomy_merged_sql,
    ncbi_taxonomy_sql,
)

from .test_marts_schema import MODELS_DIR, _model_select


def _dmp(*rows: tuple) -> str:
    return "".join("\t|\t".join(map(str, r)) + "\t|\n" for r in rows)


def test_taxdump_parsers(tmp_path):
    (tmp_path / "nodes.dmp").write_text(
        _dmp(
            (1, 1, "no rank", "", 8, 0, 1, 0, 0, 0, 0, 0, ""),
            (724, 712, "genus", "", 0, 1, 11, 1, 0, 1, 0, 0, ""),
            (729, 724, "species", "HP", 0, 1, 11, 1, 0, 1, 1, 0, "code compliant"),
        )
    )
    (tmp_path / "names.dmp").write_text(
        _dmp(
            (1, "all", "", "synonym"),
            (1, "root", "", "scientific name"),
            (724, "Parahaemophilus", "", "scientific name"),
            (729, "Haemophilus parainfluenzae", "", "synonym"),
            (729, '"Haemophilus parainfluenzae" Rivers 1922', "", "authority"),
            (729, "Parahaemophilus parainfluenzae", "", "scientific name"),
        )
    )
    (tmp_path / "merged.dmp").write_text(_dmp((12, 74109), (30, 29)))

    con = duckdb.connect()
    nodes = con.execute(f"{ncbi_taxonomy_sql(tmp_path)} ORDER BY tax_id").fetchall()
    assert nodes == [
        (1, "root", "no rank", 1),
        (724, "Parahaemophilus", "genus", 712),
        (729, "Parahaemophilus parainfluenzae", "species", 724),
    ]
    merged = con.execute(
        f"{ncbi_taxonomy_merged_sql(tmp_path)} ORDER BY old_tax_id"
    ).fetchall()
    assert merged == [(12, 74109), (30, 29)]


def test_stg_views_resolve_organism_names_by_taxid():
    """stg views: current name by taxid, merges followed, submitted name kept."""
    con = duckdb.connect()
    for schema in ("src", "stg"):
        con.execute(f"CREATE SCHEMA {schema}")
    con.execute(
        "CREATE TABLE src.ncbi_taxonomy AS SELECT * FROM (VALUES "
        "(729, 'Parahaemophilus parainfluenzae'), (28037, 'Streptococcus mitis'), "
        "(72520, 'Microchloropsis gaditana')) t(tax_id, scientific_name)"
    )
    # 999 was merged into 729.
    con.execute(
        "CREATE TABLE src.ncbi_taxonomy_merged AS "
        "SELECT 999 AS old_tax_id, 729 AS new_tax_id"
    )
    # 5 is unknown to the taxonomy: its submitted name must survive.
    con.execute(
        "CREATE TABLE src.geo_series AS SELECT * FROM (VALUES "
        "('GSE1', [729, 28037, 5], ['Haemophilus parainfluenzae', 'Streptococcus mitis', 'x'],"
        " [999], ['Old name']), "
        "('GSE2', [], [], [], [])) "
        "t(accession, sample_taxid, sample_organism, platform_taxid, platform_organism)"
    )
    con.execute(
        "CREATE TABLE src.geo_samples AS SELECT * FROM (VALUES "
        "('GSM1', [{'label': 'a', 'taxid': [72520, 5], "
        "'organism': 'Nannochloropsis gaditana' || chr(10) || 'y'}, "
        "{'label': 'b', 'taxid': [999], 'organism': 'Old name'}])) "
        "t(accession, channels)"
    )
    # Single resolved, merged, multi-organism with an unknown taxid, NULL taxid
    # (not yet re-extracted) and a taxid/organism length mismatch (#231).
    con.execute(
        "CREATE TABLE src.geo_platforms AS SELECT * FROM (VALUES "
        "('GPL1', 'Haemophilus parainfluenzae', [729]), "
        "('GPL2', 'Old name', [999]), "
        "('GPL3', 'Nannochloropsis gaditana' || chr(10) || 'y', [72520, 5]), "
        "('GPL4', 'Kept', NULL::BIGINT[]), "
        "('GPL5', 'a' || chr(10) || 'b', [729])) "
        "t(accession, organism, taxid)"
    )
    con.execute(
        "CREATE TABLE src.sra_samples AS SELECT * FROM (VALUES "
        "('SRS1', 'Haemophilus parainfluenzae', 729), ('SRS2', 'z', 5)) "
        "t(accession, organism, taxon_id)"
    )
    con.execute(
        "CREATE TABLE src.biosamples AS SELECT * FROM (VALUES "
        "('SAMN1', 'Nannochloropsis gaditana', 72520)) "
        "t(accession, taxonomy_name, taxon_id)"
    )
    for view in (
        "ncbi_taxonomy_names",
        "geo_series",
        "geo_samples",
        "geo_platforms",
        "sra_samples",
        "biosamples",
    ):
        sql = _model_select(MODELS_DIR / "stg" / f"{view}.sql")
        con.execute(f"CREATE VIEW stg.{view} AS {sql}")

    assert con.execute(
        "SELECT accession, sample_organism, sample_organism_as_submitted, "
        "platform_organism, platform_organism_as_submitted "
        "FROM stg.geo_series ORDER BY accession"
    ).fetchall() == [
        (
            "GSE1",
            ["Parahaemophilus parainfluenzae", "Streptococcus mitis", "x"],
            ["Haemophilus parainfluenzae", "Streptococcus mitis", "x"],
            ["Parahaemophilus parainfluenzae"],
            ["Old name"],
        ),
        ("GSE2", [], [], [], []),
    ]
    assert con.execute(
        "SELECT channels, channel_organism_as_submitted FROM stg.geo_samples"
    ).fetchone() == (
        [
            {
                "label": "a",
                "taxid": [72520, 5],
                "organism": "Microchloropsis gaditana\ny",
            },
            {
                "label": "b",
                "taxid": [999],
                "organism": "Parahaemophilus parainfluenzae",
            },
        ],
        ["Nannochloropsis gaditana\ny", "Old name"],
    )
    assert con.execute(
        "SELECT accession, organism, organism_as_submitted "
        "FROM stg.geo_platforms ORDER BY accession"
    ).fetchall() == [
        (
            "GPL1",
            "Parahaemophilus parainfluenzae",
            "Haemophilus parainfluenzae",
        ),
        ("GPL2", "Parahaemophilus parainfluenzae", "Old name"),
        (
            "GPL3",
            "Microchloropsis gaditana\ny",
            "Nannochloropsis gaditana\ny",
        ),
        ("GPL4", "Kept", "Kept"),
        ("GPL5", "a\nb", "a\nb"),
    ]
    assert con.execute(
        "SELECT * FROM stg.sra_samples ORDER BY accession"
    ).fetchall() == [
        ("SRS1", "Parahaemophilus parainfluenzae", 729, "Haemophilus parainfluenzae"),
        ("SRS2", "z", 5, "z"),
    ]
    assert con.execute("SELECT * FROM stg.biosamples").fetchall() == [
        ("SAMN1", "Microchloropsis gaditana", 72520, "Nannochloropsis gaditana")
    ]
