"""Publish the ``omicidx-pubmed`` dataset release via ``cdsci.lake.publish``.

A release is an immutable full snapshot of ``lake.omicidx.pubmed_article``
(latest MEDLINE revision per PMID; deleted citations are absent), published to
local storage only. By default the tree is built in a temporary directory that
is removed when the run ends; ``--out DIR`` keeps it. Retention is
``keep_last=7`` (pinned releases are never pruned). See
``docs/adrs/0006-pubmed-dataset-releases-via-cdsci-lake.md``.
"""

import argparse
import tempfile
import uuid
from datetime import date
from pathlib import Path

import duckdb
from cdsci.lake.contracts import (
    ColumnContract,
    DatasetContract,
    TableContract,
    TemporalModel,
)
from cdsci.lake.publish.builder import LocalDirStore
from cdsci.lake.publish.pipeline import publish_release
from cdsci.lake.publish.release import (
    ReleaseManifest,
    SourceAssetVersion,
    release_date,
)
from omicidx.prefect.config import get_lake_connection

_NULL_MEANING = "Absent from the article's MEDLINE record."

_AUTHORS = (
    "list<struct<lastname: string, forename: string, initials: string, "
    "identifier: string, affiliation: string>>"
)
_REFERENCES = "list<struct<citation: string, pmid: string>>"
_GRANT_IDS = (
    "list<struct<grant_id: string, grant_acronym: string, country: string, "
    "agency: string>>"
)

# (name, arrow_type, identifier_namespace, description) in the lake's column order.
_COLUMNS: tuple[tuple[str, str, str | None, str], ...] = (
    ("pmid", "string", "pubmed", "PubMed unique identifier (MEDLINE PMID element)."),
    ("title", "string", None, "Article title (MEDLINE ArticleTitle element)."),
    (
        "issue",
        "string",
        None,
        "Journal issue number (MEDLINE JournalIssue/Issue element).",
    ),
    ("pages", "string", None, "Page range (MEDLINE Pagination/MedlinePgn element)."),
    (
        "abstract",
        "string",
        None,
        "Article abstract text (MEDLINE Abstract/AbstractText elements).",
    ),
    ("journal", "string", None, "Full journal title (MEDLINE Journal/Title element)."),
    (
        "authors",
        _AUTHORS,
        None,
        "Author list (MEDLINE AuthorList/Author elements), in byline order.",
    ),
    (
        "pubdate",
        "string",
        None,
        "Journal publication date as recorded (MEDLINE JournalIssue/PubDate element).",
    ),
    (
        "mesh_terms",
        "string",
        None,
        "MeSH descriptor headings (MEDLINE MeshHeadingList element), delimited text.",
    ),
    (
        "publication_types",
        "string",
        None,
        "Publication types (MEDLINE PublicationTypeList element), delimited text.",
    ),
    (
        "chemical_list",
        "string",
        None,
        "Substance names (MEDLINE ChemicalList element), delimited text.",
    ),
    (
        "keywords",
        "string",
        None,
        "Author and NLM keywords (MEDLINE KeywordList element), delimited text.",
    ),
    (
        "doi",
        "string",
        "doi",
        "Digital Object Identifier (MEDLINE ArticleId with IdType doi).",
    ),
    (
        "references",
        _REFERENCES,
        None,
        "Cited references (MEDLINE ReferenceList/Reference elements).",
    ),
    (
        "languages",
        "string",
        None,
        "Article languages (MEDLINE Language element), delimited text.",
    ),
    (
        "vernacular_title",
        "string",
        None,
        "Title in the original language (MEDLINE VernacularTitle element).",
    ),
    (
        "date_completed",
        "string",
        None,
        "Date NLM completed indexing (MEDLINE DateCompleted element).",
    ),
    (
        "date_revised",
        "string",
        None,
        "Date of the latest record revision (MEDLINE DateRevised element).",
    ),
    (
        "pmc",
        "string",
        "pmc",
        "PubMed Central identifier (MEDLINE ArticleId with IdType pmc).",
    ),
    (
        "other_id",
        "string",
        "medline_other_id",
        "Other identifiers assigned by data providers (MEDLINE OtherID element).",
    ),
    (
        "medline_ta",
        "string",
        None,
        "MEDLINE journal title abbreviation (MedlineJournalInfo/MedlineTA element).",
    ),
    (
        "nlm_unique_id",
        "string",
        "nlm",
        "NLM catalog unique journal identifier (MedlineJournalInfo/NlmUniqueID element).",
    ),
    (
        "issn_linking",
        "string",
        None,
        "Linking ISSN of the journal (MedlineJournalInfo/ISSNLinking element).",
    ),
    (
        "country",
        "string",
        None,
        "Journal country of publication (MedlineJournalInfo/Country element).",
    ),
    (
        "grant_ids",
        _GRANT_IDS,
        None,
        "Grants supporting the work (MEDLINE GrantList/Grant elements).",
    ),
)

PUBMED_ARTICLE = TableContract(
    name="pubmed_article",
    description=(
        "PubMed/MEDLINE citation records: one current row per PMID, from the "
        "latest MEDLINE revision."
    ),
    grain="one row per PMID, latest MEDLINE revision; deleted citations absent",
    primary_key=("pmid",),
    sort_by=("pmid",),
    temporal_model=TemporalModel.UPSERT_LATEST_SNAPSHOT,
    owner="omicidx",
    license=(
        "NLM PubMed terms and conditions "
        "(https://www.nlm.nih.gov/databases/download/terms_and_conditions.html)"
    ),
    columns=tuple(
        ColumnContract(
            name=name,
            arrow_type=arrow_type,
            description=description,
            nullable=name != "pmid",
            identifier_namespace=namespace,
            null_meaning=None if name == "pmid" else _NULL_MEANING,
        )
        for name, arrow_type, namespace, description in _COLUMNS
    ),
)

PUBMED = DatasetContract(
    id="omicidx-pubmed",
    title="OmicIDX: PubMed articles",
    description=(
        "Full snapshot of PubMed/MEDLINE citation records as loaded into the "
        "OmicIDX lake: one row per PMID, with authors, references and grants "
        "as nested lists."
    ),
    publisher="omicidx",
    tables={"pubmed_article": PUBMED_ARTICLE},
    keep_last=7,
)


def _select_sql(snapshot_id: int) -> str:
    cols = ", ".join(f'"{name}"' for name, *_ in _COLUMNS)
    return (
        f"SELECT {cols} FROM lake.omicidx.pubmed_article AT (VERSION => {snapshot_id})"
    )


def publish_pubmed_release(
    con: duckdb.DuckDBPyConnection,
    out: Path | None = None,
    *,
    today: date | None = None,
) -> ReleaseManifest:
    """Publish one ``omicidx-pubmed`` release; ``con`` must have the lake attached as ``lake``."""
    snapshot_id = con.sql("SELECT max(snapshot_id) FROM lake.snapshots()").fetchone()[0]
    source_versions = (
        SourceAssetVersion(
            ref="lake.omicidx.pubmed_article", version=f"snapshot:{snapshot_id}"
        ),
    )
    tables = {"pubmed_article": con.sql(_select_sql(snapshot_id))}

    def _publish(root: Path) -> ReleaseManifest:
        return publish_release(
            LocalDirStore(root),
            contract=PUBMED,
            tables=tables,
            source_asset_versions=source_versions,
            run_id=str(uuid.uuid4()),
            today=today,
            con=None,
        )

    if out is None:
        with tempfile.TemporaryDirectory(prefix="omicidx-pubmed-") as tmp:
            manifest = _publish(Path(tmp))
        where = "temporary, removed"
    else:
        manifest = _publish(out)
        where = str(out)
    rows = next(t for t in manifest.tables if t.name == "pubmed_article").row_count
    print(
        f"{PUBMED.id} {manifest.release} ({release_date(manifest.release)}): "
        f"pubmed_article {rows:,} rows; output {where}"
    )
    return manifest


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--out", type=Path, default=None, help="keep the release tree here"
    )
    args = parser.parse_args()
    publish_pubmed_release(get_lake_connection(read_only=True), args.out)
