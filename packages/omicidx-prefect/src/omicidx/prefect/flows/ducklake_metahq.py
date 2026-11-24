"""DuckLake load: newest raw MetaHQ release → lake.<schema>.metahq_* (#266).

Every table mirrors the newest release (``upsert`` with ``prune="true"``) and
carries ``metahq_version``, excluded from the change check so a new release
rewrites only rows whose data moved. Older releases stay reachable through lake
time travel.

- ``metahq_annotation``: one row per (level, accession, attribute, source, value).
  ``level`` is ``sample`` (GSM) or ``series`` (GSE); ``attribute`` is tissue,
  disease, sex or age; ``value`` is one UBERON/MONDO id, ``M``/``F``, or an age
  bin. MetaHQ joins several values with ``|``; they are split to rows here.
- ``metahq_entity``: per (level, accession), organism plus the cross-reference
  ids (platform, series, srp, srs, srx), all split on ``|`` into lists.
- ``metahq_ontology_term`` / ``metahq_ontology_synonym``: from the release's
  ``ontology_search.duckdb``.
- ``metahq_ontology_ancestor``: (ontology, term_id, ancestor_id), reflexive. The
  release ships it as a dense square matrix per ontology, ``M[r][c] = 1`` when
  term c is-a* term r (row i is the term of column i); this turns it into edges.
- ``metahq_refinebio_map``, ``metahq_external_link``: refine.bio ids and links
  out to Bgee/DiSignAtlas/Gemma, per GSE/GSM.

The release's GEO text fields (``metadata__level-*.parquet``) are not modeled:
they are GEO's own fields, already in ``geo_samples``/``geo_series``.
"""

import logging
import tarfile
import tempfile
from collections.abc import Iterator
from pathlib import Path

import bson
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from cdsci.lake import ops
from cdsci.lake.connect import upsert
from omicidx.prefect.config import get_lake_connection
from omicidx.prefect.flows.ducklake import LAKE_SCHEMA
from omicidx.prefect.flows.metahq import latest_tarball
from omicidx.prefect.run import retry, run_id

log = logging.getLogger(__name__)

LEVELS = ("sample", "series")
ID_FIELDS = ("platform", "series", "srp", "srs", "srx")
ONTOLOGIES = ("mondo", "uberon_ext")


def _split(value: str | None) -> list[str]:
    return [v for v in value.split("|") if v] if value else []


def parse_annotations(doc: dict, level: str) -> tuple[list[dict], list[dict]]:
    """One decoded ``combined__level-*.bson`` doc → (annotation rows, entity rows)."""
    annotations, entities = [], []
    for accession, record in doc.items():
        ids = record.get("accession_ids", {})
        entities.append(
            {"level": level, "accession": accession}
            | {"organism": _split(record.get("organism"))}
            | {f: _split(ids.get(f)) for f in ID_FIELDS}
        )
        for attribute, by_source in record.items():
            if attribute in ("accession_ids", "organism"):
                continue
            for source, call in by_source.items():
                for value in dict.fromkeys(
                    _split(call.get("id"))
                ):  # dedupe, keep order
                    annotations.append(
                        {
                            "level": level,
                            "accession": accession,
                            "attribute": attribute,
                            "source": source,
                            "ecode": call.get("ecode"),
                            "value": value,
                        }
                    )
    return annotations, entities


def closure_edges(path: Path, ontology: str, batch: int = 1000) -> Iterator[pa.Table]:
    """Dense closure matrix → (ontology, term_id, ancestor_id) tables, ``batch`` columns at a time."""
    pf = pq.ParquetFile(path)
    names = pa.array(pf.schema_arrow.names)
    for start in range(0, len(names), batch):
        chunk = pf.read(columns=names[start : start + batch].to_pylist())
        for j, col in enumerate(chunk.columns):
            ancestors = names.take(pc.indices_nonzero(col.combine_chunks()))
            n = len(ancestors)
            yield pa.table(
                {
                    "ontology": pa.array([ontology] * n, pa.string()),
                    "term_id": pa.array([names[start + j].as_py()] * n, pa.string()),
                    "ancestor_id": ancestors,
                }
            )


def stage_release(release: Path, out: Path) -> dict[str, str]:
    """Parse an unpacked release dir into Parquet under ``out``; table → SELECT over it."""
    annotations, entities = [], []
    for level in LEVELS:
        raw = (release / "annotations" / f"combined__level-{level}.bson").read_bytes()
        (doc,) = bson.decode_all(raw)
        a, e = parse_annotations(doc, level)
        annotations += a
        entities += e
    pq.write_table(pa.Table.from_pylist(annotations), out / "annotation.parquet")
    pq.write_table(pa.Table.from_pylist(entities), out / "entity.parquet")
    with pq.ParquetWriter(
        out / "ancestor.parquet",
        pa.schema([(c, pa.string()) for c in ("ontology", "term_id", "ancestor_id")]),
    ) as writer:
        for ontology in ONTOLOGIES:
            for t in closure_edges(
                release / "ontology" / ontology / "relations.parquet", ontology
            ):
                writer.write_table(t)

    def pq_(p: Path | str) -> str:
        return f"read_parquet('{p}')"

    onto = "metahq_onto"
    meta = release / "metadata"
    return {
        "metahq_annotation": f"SELECT DISTINCT * FROM {pq_(out / 'annotation.parquet')}",
        "metahq_entity": f"SELECT * FROM {pq_(out / 'entity.parquet')}",
        "metahq_ontology_term": (
            f"SELECT id AS term_id, ontology, name, type FROM {onto}.ontology_terms"
        ),
        "metahq_ontology_synonym": (
            f"SELECT DISTINCT term_id, synonym, scope FROM {onto}.ontology_synonyms"
        ),
        "metahq_ontology_ancestor": f"SELECT DISTINCT * FROM {pq_(out / 'ancestor.parquet')}",
        "metahq_refinebio_map": f"SELECT DISTINCT gsm, gse, refinebio_sample, refinebio_experiment FROM {pq_(meta / 'refinebio_map.parquet')}",
        "metahq_external_link": " UNION ".join(
            f"SELECT series, '{r}' AS resource, CAST(x.id AS VARCHAR) AS id, x.url "
            f"FROM {pq_(meta / 'external_links.parquet')}, UNNEST({r}.records) AS u(x)"
            for r in ("BGee", "DiSignAtlas", "Gemma")
        ),
    }


KEYS = {
    "metahq_annotation": ["level", "accession", "attribute", "source", "value"],
    "metahq_entity": ["level", "accession"],
    "metahq_ontology_term": "term_id",
    "metahq_ontology_synonym": ["term_id", "synonym", "scope"],
    "metahq_ontology_ancestor": ["ontology", "term_id", "ancestor_id"],
    "metahq_refinebio_map": ["gsm", "gse", "refinebio_sample", "refinebio_experiment"],
    "metahq_external_link": ["series", "resource", "id", "url"],
}


@retry
def metahq_to_ducklake(lake_schema: str = LAKE_SCHEMA) -> dict:
    """Mirror the newest landed MetaHQ release into the ``metahq_*`` lake tables."""
    newest = latest_tarball()
    if newest is None:
        log.warning("No metahq raw landed yet; skipping its load")
        return {"skipped": True}
    version, raw = newest

    rows = {}
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        with raw.open("rb") as f, tarfile.open(fileobj=f, mode="r|gz") as tar:
            tar.extractall(work, filter="data")
        (release,) = [p for p in work.iterdir() if p.is_dir()]
        stage = work / "_stage"
        stage.mkdir()
        sources = stage_release(release, stage)

        with get_lake_connection() as con:
            con.execute(
                f"ATTACH '{release / 'ontology' / 'ontology_search.duckdb'}' "
                "AS metahq_onto (READ_ONLY)"
            )
            for table, sql in sources.items():
                target = f"lake.{lake_schema}.{table}"
                log.info(f"Merging MetaHQ {version} → {target}")
                with ops.run(
                    con,
                    source="metahq",
                    target=target,
                    extra={"run_id": run_id(), "raw": str(raw), "version": version},
                ) as r:
                    upsert(
                        con,
                        target,
                        f"SELECT *, '{version}' AS metahq_version FROM ({sql})",
                        key=KEYS[table],
                        exclude_change_cols=["metahq_version"],
                        prune="true",
                    )
                    r.rows = con.execute(f"SELECT count(*) FROM {target}").fetchone()[0]
                rows[table] = r.rows
    log.info(f"MetaHQ {version}: {rows}")
    return {"version": version, "rows": rows}
