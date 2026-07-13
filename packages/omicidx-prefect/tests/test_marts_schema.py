"""Schema-resemblance acceptance test for the marts (Stage C / C3).

Builds the geometadb.* / sradb.* views (sql/040, sql/050) in an in-memory
DuckDB over empty, correctly-typed synthetic upstream tables, then asserts each
mart view exposes exactly the expected column set. No network / no parquet /
no live catalog — this checks the SQL is well-formed and that the modernization
(drop always-NULL legacy columns, keep legacy names, add modern cross-refs)
holds. The mapping rationale lives in docs/specs/omicidx-marts-adaptation.md.

End-to-end fidelity against the real parquet still requires a live-catalog run
(ducklake-load -> parquet-export -> duckdb-build).
"""

from pathlib import Path

import duckdb
import sqlglot

SQL_DIR = Path(__file__).parent.parent / "src" / "omicidx" / "prefect" / "sql"

# Contact struct shared by all GEO entities (superset of fields the views read).
_CONTACT = (
    'STRUCT("name" STRUCT("first" VARCHAR, middle VARCHAR, "last" VARCHAR), '
    "country VARCHAR, email VARCHAR, institute VARCHAR)"
)
_CHANNEL = (
    "STRUCT(source_name VARCHAR, organism VARCHAR, characteristics VARCHAR[], "
    "molecule VARCHAR, label VARCHAR, treatment_protocol VARCHAR, "
    "extract_protocol VARCHAR, label_protocol VARCHAR)"
)

# Empty typed upstream tables — columns mirror sql/030 (SRA) and the ducklake
# GEO loader SELECTs (flows/ducklake_geo.py). Only the columns the marts read.
_UPSTREAM_DDL = [
    """CREATE TABLE main.stg_sra_studies (
        accession VARCHAR, alias VARCHAR, title VARCHAR, study_type VARCHAR,
        abstract VARCHAR, broker_name VARCHAR, center_name VARCHAR,
        bioproject_accession VARCHAR, description VARCHAR, attributes VARCHAR,
        geo_accession VARCHAR, pubmed_ids INTEGER[])""",
    """CREATE TABLE main.stg_sra_samples (
        accession VARCHAR, alias VARCHAR, taxon_id INTEGER, organism VARCHAR,
        description VARCHAR, attributes VARCHAR, biosample_accession VARCHAR)""",
    """CREATE TABLE main.stg_sra_experiments (
        accession VARCHAR, alias VARCHAR, center_name VARCHAR, title VARCHAR,
        study_accession VARCHAR, design VARCHAR, sample_accession VARCHAR,
        library_name VARCHAR, library_strategy VARCHAR, library_source VARCHAR,
        library_selection VARCHAR, library_layout VARCHAR,
        library_construction_protocol VARCHAR, spot_length INTEGER,
        reads VARCHAR, platform VARCHAR, instrument_model VARCHAR,
        attributes VARCHAR, library_layout_length INTEGER,
        library_layout_sdev DOUBLE, nreads INTEGER)""",
    """CREATE TABLE main.stg_sra_runs (
        accession VARCHAR, alias VARCHAR, experiment_accession VARCHAR,
        total_spots BIGINT, total_bases BIGINT, attributes VARCHAR)""",
    f"""CREATE TABLE main.src_geo_samples (
        title VARCHAR, accession VARCHAR, platform_id VARCHAR, status VARCHAR,
        submission_date DATE, last_update_date DATE, type VARCHAR,
        channels {_CHANNEL}[], hyb_protocol VARCHAR, description VARCHAR,
        data_processing VARCHAR, contact {_CONTACT}, supplemental_files VARCHAR[],
        data_row_count INTEGER, channel_count INTEGER, biosample VARCHAR,
        sra_experiment VARCHAR, library_source VARCHAR)""",
    f"""CREATE TABLE main.src_geo_series (
        accession VARCHAR, title VARCHAR, status VARCHAR, submission_date DATE,
        last_update_date DATE, summary VARCHAR, pubmed_id INTEGER[],
        type VARCHAR[], contributor VARCHAR[], overall_design VARCHAR,
        contact {_CONTACT}, supplemental_files VARCHAR[], sample_id VARCHAR[],
        bioprojects VARCHAR[], sra_studies VARCHAR[], subseries VARCHAR[])""",
    f"""CREATE TABLE main.src_geo_platforms (
        title VARCHAR, accession VARCHAR, status VARCHAR, submission_date DATE,
        last_update_date DATE, technology VARCHAR, distribution VARCHAR,
        organism VARCHAR, manufacturer VARCHAR[], manufacture_protocol VARCHAR,
        description VARCHAR, contact {_CONTACT}, data_row_count INTEGER,
        series_id VARCHAR[])""",
    "CREATE TABLE main.src_geo_series_with_rnaseq_counts (accession VARCHAR)",
]

EXPECTED = {
    "sradb.study": {
        "study_ID", "study_alias", "study_accession", "study_title",
        "study_type", "study_abstract", "broker_name", "center_name",
        "center_project_name", "study_description", "study_attribute",
        "bioproject_accession", "geo_accession", "pubmed_ids",
    },
    "sradb.sample": {
        "sample_ID", "sample_alias", "sample_accession", "taxon_id",
        "scientific_name", "description", "sample_attribute",
        "biosample_accession",
    },
    "sradb.experiment": {
        "experiment_ID", "experiment_alias", "experiment_accession",
        "center_name", "title", "study_accession", "design_description",
        "sample_accession", "library_name", "library_strategy",
        "library_source", "library_selection", "library_layout",
        "library_construction_protocol", "spot_length", "read_spec", "platform",
        "instrument_model", "experiment_attribute", "library_layout_length",
        "library_layout_sdev", "nreads",
    },
    "sradb.run": {
        "run_ID", "run_alias", "run_accession", "experiment_accession",
        "total_spots", "total_bases", "run_attribute",
    },
    "sradb.sra": {
        "sra_ID", "run_alias", "run_accession", "spots", "bases",
        "run_attribute", "experiment_alias", "experiment_accession",
        "experiment_title", "design_description", "library_name",
        "library_strategy", "library_source", "library_selection",
        "library_layout", "library_construction_protocol", "read_spec",
        "platform", "instrument_model", "experiment_attribute", "sample_alias",
        "sample_accession", "taxon_id", "scientific_name", "description",
        "sample_attribute", "biosample_accession", "study_alias",
        "study_accession", "study_title", "study_type", "study_abstract",
        "center_project_name", "bioproject_accession", "geo_accession",
        "study_description", "study_attribute",
    },
    "geometadb.gse": {
        "gse", "title", "status", "submission_date", "last_update_date",
        "summary", "pubmed_id", "type", "contributor", "web_link",
        "overall_design", "contact_country", "contact_email",
        "contact_first_name", "contact_institute", "contact_last_name",
        "contact", "supplemental_files", "has_geo_computed_rnaseq",
        "bioprojects", "sra_studies", "subseries",
    },
    "geometadb.gpl": {
        "title", "gpl", "status", "submission_date", "last_update_date",
        "technology", "distribution", "organism", "manufacturer",
        "manufacture_protocol", "description", "web_link", "contact",
        "data_row_count",
    },
    # dropped legacy columns that must NOT reappear (always-NULL in modern data)
    "_dropped": {
        "sradb.study": {"sra_link", "ddbj_link", "ena_link", "related_studies",
                        "submission_accession", "sradb_updated"},
        "sradb.run": {"run_date", "run_center", "bamFile", "instrument_name",
                      "sra_link", "sradb_updated"},
        "sradb.experiment": {"base_caller", "quality_scorer", "adapter_spec",
                             "bamFile", "fastqFTP", "sradb_updated"},
    },
}

# gsm expected set built programmatically (16 per-channel columns).
_CH = ["source_name", "organism", "characteristics", "molecule", "label",
       "treatment_protocol", "extract_protocol", "label_protocol"]
EXPECTED["geometadb.gsm"] = {
    "title", "gsm", "gpl", "status", "submission_date", "last_update_date",
    "type", "channel_records", "hyb_protocol", "description", "data_processing",
    "contact", "supplemental_files", "data_row_count", "channel_count",
    "biosample", "sra_experiment", "library_source",
} | {f"{c}_ch1" for c in _CH} | {f"{c}_ch2" for c in _CH}


def _build() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    for ddl in _UPSTREAM_DDL:
        con.execute(ddl)
    for name in ("040_geometadb_views.sql", "050_sradb_views.sql"):
        sql = (SQL_DIR / name).read_text()
        for stmt in sqlglot.transpile(sql, read="duckdb"):
            con.execute(stmt)
    return con


def _columns(con: duckdb.DuckDBPyConnection, view: str) -> set[str]:
    schema, name = view.split(".")
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = ? AND table_name = ?",
        [schema, name],
    ).fetchall()
    return {r[0] for r in rows}


def test_mart_schema_resemblance():
    con = _build()
    try:
        for view, expected in EXPECTED.items():
            if view == "_dropped":
                continue
            assert _columns(con, view) == expected, f"{view} column set drifted"
        for view, dropped in EXPECTED["_dropped"].items():
            leaked = _columns(con, view) & dropped
            assert not leaked, f"{view} still exposes always-NULL columns: {leaked}"
    finally:
        con.close()


if __name__ == "__main__":
    test_mart_schema_resemblance()
    print("ok")
