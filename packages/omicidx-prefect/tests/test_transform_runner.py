"""The transform flow's runner path, offline (ADR-0008).

Runs all 41 models through cdsci-lake's real ``run_all`` -- ``ops.run``,
``CREATE OR REPLACE VIEW``, view comment, sqlglot lineage into ``lake_ops`` --
against a local file-catalog DuckLake holding empty ``lake.omicidx.*`` tables
with the production column types. No R2, no Postgres, no credentials. This is
the check that the cutover is mechanically sound before the first production
run replaces SQLMesh's views in place.
"""

from cdsci.lake import ops
from cdsci.lake.transform.models import load_models
from cdsci.lake.transform.runner import run_all

from .test_export_steel_thread import _local_lake
from .test_marts_schema import MODELS_DIR

# lake.omicidx.<table> column types as introspected from the production lake on
# 2026-11-04 (the retired SQLMesh external_models.yaml). Only names/types matter
# here: the models' `SELECT * REPLACE (...)` and struct field accesses must bind.
LAKE_TABLES: dict[str, dict[str, str]] = {
    "bioproject": {
        "accession": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "name": "TEXT",
        "publications": "STRUCT(pubdate TIMESTAMP, id TEXT, db TEXT)[]",
        "locus_tags": 'STRUCT(biosample_id TEXT, assembly_id TEXT, "value" TEXT)[]',
        "release_date": "TIMESTAMP",
        "data_types": "TEXT[]",
        "external_links": 'STRUCT(category TEXT, "label" TEXT, url TEXT)[]',
    },
    "biosample": {
        "submission_date": "TEXT",
        "last_update": "TEXT",
        "publication_date": "TEXT",
        "access": "TEXT",
        "id": "TEXT",
        "accession": "TEXT",
        "id_recs": 'STRUCT(db TEXT, "label" TEXT, id TEXT)[]',
        "ids": "TEXT[]",
        "sra_sample": "TEXT",
        "dbgap": "TEXT",
        "gsm": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "taxonomy_name": "TEXT",
        "taxon_id": "BIGINT",
        "attribute_recs": 'STRUCT(attribute_name TEXT, "value" TEXT, harmonized_name TEXT, display_name TEXT, unit TEXT)[]',
        "attributes": "TEXT[]",
        "model": "TEXT",
    },
    "geo_platform": {
        "title": "TEXT",
        "status": "TEXT",
        "submission_date": "DATE",
        "last_update_date": "DATE",
        "accession": "TEXT",
        "contact": 'STRUCT(city TEXT, "name" STRUCT("first" TEXT, middle TEXT, "last" TEXT), email TEXT, state TEXT, address TEXT, department TEXT, country TEXT, web_link TEXT, institute TEXT, zip_postal_code JSON, phone TEXT)',
        "organism": "TEXT",
        "sample_id": "TEXT[]",
        "series_id": "TEXT[]",
        "technology": "TEXT",
        "description": "TEXT",
        "distribution": "TEXT",
        "manufacturer": "TEXT[]",
        "data_row_count": "BIGINT",
        "contributor": 'STRUCT("first" TEXT, middle TEXT, "last" TEXT)[]',
        "relation": "TEXT[]",
        "manufacture_protocol": "TEXT",
        "taxid": "BIGINT[]",
    },
    "geo_sample": {
        "title": "TEXT",
        "status": "TEXT",
        "submission_date": "DATE",
        "last_update_date": "DATE",
        "type": "TEXT",
        "anchor": "TEXT",
        "contact": 'STRUCT(city TEXT, "name" STRUCT("first" TEXT, middle TEXT, "last" TEXT), email TEXT, state TEXT, address TEXT, department TEXT, country TEXT, web_link TEXT, institute TEXT, zip_postal_code JSON, phone TEXT)',
        "description": "TEXT",
        "accession": "TEXT",
        "biosample": "TEXT",
        "tag_count": "BIGINT",
        "tag_length": "DOUBLE",
        "platform_id": "TEXT",
        "hyb_protocol": "TEXT",
        "channel_count": "BIGINT",
        "scan_protocol": "TEXT",
        "data_row_count": "BIGINT",
        "library_source": "TEXT",
        "sra_experiment": "TEXT",
        "data_processing": "TEXT",
        "supplemental_files": "TEXT[]",
        "channels": 'STRUCT("label" TEXT, taxid BIGINT[], molecule TEXT, organism TEXT, source_name TEXT, label_protocol TEXT, growth_protocol TEXT, extract_protocol TEXT, treatment_protocol TEXT, "characteristics" STRUCT(tag TEXT, "value" TEXT)[])[]',
        "contributor": "JSON[]",
    },
    "geo_series": {
        "title": "TEXT",
        "status": "TEXT",
        "submission_date": "DATE",
        "last_update_date": "DATE",
        "accession": "TEXT",
        "subseries": "TEXT[]",
        "bioprojects": "TEXT[]",
        "sra_studies": "TEXT[]",
        "contact": 'STRUCT(city TEXT, "name" STRUCT("first" TEXT, middle TEXT, "last" TEXT), email TEXT, state TEXT, address TEXT, department TEXT, country TEXT, web_link TEXT, institute TEXT, zip_postal_code JSON, phone TEXT)',
        "type": "TEXT[]",
        "summary": "TEXT",
        "relation": "TEXT[]",
        "pubmed_id": "BIGINT[]",
        "sample_id": "TEXT[]",
        "sample_taxid": "BIGINT[]",
        "sample_organism": "TEXT[]",
        "platform_id": "TEXT[]",
        "platform_taxid": "BIGINT[]",
        "platform_organism": "TEXT[]",
        "supplemental_files": "TEXT[]",
        "overall_design": "TEXT",
        "contributor": 'STRUCT("first" TEXT, middle TEXT, "last" TEXT)[]',
    },
    "geo_series_with_rnaseq_counts": {
        "accession": "TEXT",
    },
    "ncbi_taxonomy": {
        "tax_id": "BIGINT",
        "scientific_name": "TEXT",
        "rank": "TEXT",
        "parent_tax_id": "BIGINT",
    },
    "ncbi_taxonomy_merged": {
        "old_tax_id": "BIGINT",
        "new_tax_id": "BIGINT",
    },
    "pubmed_article": {
        "pmid": "TEXT",
        "title": "TEXT",
        "issue": "TEXT",
        "pages": "TEXT",
        "abstract": "TEXT",
        "journal": "TEXT",
        "authors": "STRUCT(lastname TEXT, forename TEXT, initials TEXT, identifier TEXT, affiliation TEXT)[]",
        "pubdate": "TEXT",
        "mesh_terms": "TEXT",
        "publication_types": "TEXT",
        "chemical_list": "TEXT",
        "keywords": "TEXT",
        "doi": "TEXT",
        "references": "STRUCT(citation TEXT, pmid TEXT)[]",
        "languages": "TEXT",
        "vernacular_title": "TEXT",
        "date_completed": "TEXT",
        "date_revised": "TEXT",
        "pmc": "TEXT",
        "other_id": "TEXT",
        "medline_ta": "TEXT",
        "nlm_unique_id": "TEXT",
        "issn_linking": "TEXT",
        "country": "TEXT",
        "grant_ids": "STRUCT(grant_id TEXT, grant_acronym TEXT, country TEXT, agency TEXT)[]",
    },
    "sra_accessions": {
        "accession": "TEXT",
        "submission": "TEXT",
        "status": "TEXT",
        "updated": "TIMESTAMPTZ",
        "published": "TIMESTAMPTZ",
        "received": "TIMESTAMPTZ",
        "type": "TEXT",
        "center": "TEXT",
        "visibility": "TEXT",
        "alias": "TEXT",
        "experiment": "TEXT",
        "sample": "TEXT",
        "study": "TEXT",
        "loaded": "BIGINT",
        "spots": "BIGINT",
        "bases": "BIGINT",
        "md5sum": "TEXT",
        "biosample": "TEXT",
        "bioproject": "TEXT",
        "replacedby": "TEXT",
    },
    "sra_experiment": {
        "accession": "TEXT",
        "experiment_accession": "TEXT",
        "alias": "TEXT",
        "title": "TEXT",
        "design": "TEXT",
        "center_name": "TEXT",
        "study_accession": "TEXT",
        "sample_accession": "TEXT",
        "platform": "TEXT",
        "instrument_model": "TEXT",
        "library_name": "TEXT",
        "library_construction_protocol": "TEXT",
        "library_layout": "TEXT",
        "library_layout_length": "TEXT",
        "library_layout_sdev": "TEXT",
        "library_strategy": "TEXT",
        "library_source": "TEXT",
        "library_selection": "TEXT",
        "spot_length": "BIGINT",
        "nreads": "BIGINT",
        "identifiers": "STRUCT(namespace TEXT, id TEXT, uuid TEXT)[]",
        "attributes": 'STRUCT(tag TEXT, "value" TEXT)[]',
        "xrefs": "STRUCT(db TEXT, id TEXT)[]",
        "reads": "STRUCT(base_coord BIGINT, read_class TEXT, read_index BIGINT, read_type TEXT)[]",
    },
    "sra_run": {
        "accession": "TEXT",
        "alias": "TEXT",
        "experiment_accession": "TEXT",
        "title": "TEXT",
        "identifiers": "STRUCT(namespace TEXT, id TEXT, uuid TEXT)[]",
        "attributes": 'STRUCT(tag TEXT, "value" TEXT)[]',
        "qualities": "STRUCT(quality INT, count BIGINT)[]",
    },
    "sra_sample": {
        "accession": "TEXT",
        "alias": "TEXT",
        "title": "TEXT",
        "organism": "TEXT",
        "description": "TEXT",
        "taxon_id": "INT",
        "biosample": "TEXT",
        "identifiers": "STRUCT(namespace TEXT, id TEXT, uuid TEXT)[]",
        "attributes": 'STRUCT(tag TEXT, "value" TEXT)[]',
        "xrefs": "STRUCT(db TEXT, id TEXT)[]",
    },
    "sra_study": {
        "accession": "TEXT",
        "study_accession": "TEXT",
        "alias": "TEXT",
        "title": "TEXT",
        "description": "TEXT",
        "abstract": "TEXT",
        "study_type": "TEXT",
        "center_name": "TEXT",
        "broker_name": "TEXT",
        "bioproject": "TEXT",
        "geo": "TEXT",
        "identifiers": "STRUCT(namespace TEXT, id TEXT, uuid TEXT)[]",
        "attributes": 'STRUCT(tag TEXT, "value" TEXT)[]',
        "xrefs": "STRUCT(db TEXT, id TEXT)[]",
        "pubmed_ids": "TEXT[]",
    },
}


def test_run_all_builds_every_view_on_a_local_lake(tmp_path):
    con = _local_lake(tmp_path)
    con.execute(f"ATTACH '{tmp_path / 'ops.duckdb'}' AS {ops.OPS}")
    ops.bootstrap(con)
    con.execute("CREATE SCHEMA lake.omicidx")
    for table, columns in LAKE_TABLES.items():
        ddl = ", ".join(f'"{c}" {t}' for c, t in columns.items())
        con.execute(f"CREATE TABLE lake.omicidx.{table} ({ddl})")

    models = load_models(MODELS_DIR)
    rows = run_all(con, models, writer="omicidx")

    # Views are not counted (cdsci-lake #168); every model is a view.
    assert set(rows) == set(models) and set(rows.values()) == {None}
    views = {
        f"{s}.{n}"
        for s, n in con.execute(
            "SELECT schema_name, view_name FROM duckdb_views() "
            "WHERE database_name = 'lake' AND NOT internal"
        ).fetchall()
    }
    assert views == set(models)
    # One successful lake_ops.run row per model, and the description landed as
    # the view comment.
    runs = ops.list_runs(con, limit=100)
    assert {r["source"] for r in runs} == set(models)
    assert {r["status"] for r in runs} == {"success"}
    writers = con.execute(f"SELECT DISTINCT writer FROM {ops.OPS}.lake_ops.source").fetchall()
    assert writers == [("omicidx",)]
    comment = con.execute(
        "SELECT comment FROM duckdb_views() WHERE database_name = 'lake' "
        "AND schema_name = 'sradb' AND view_name = 'study'"
    ).fetchone()[0]
    assert comment.startswith(models["sradb.study"].description)
