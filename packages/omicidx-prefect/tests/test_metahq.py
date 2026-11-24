"""MetaHQ (#266): BSON annotation parsing and the dense-closure → edges conversion."""

import pyarrow as pa
import pyarrow.parquet as pq
from omicidx.prefect.flows.ducklake_metahq import closure_edges, parse_annotations


def test_parse_annotations_splits_pipes_into_rows():
    doc = {
        "GSM1": {
            "accession_ids": {"platform": "GPL1", "series": "GSE1|GSE2", "srx": "SRX1"},
            "organism": "homo sapiens",
            "tissue": {
                "Gemma": {"ecode": "expert-curated", "id": "UBERON:1|UBERON:2|UBERON:1"}
            },
            "sex": {"ALE": {"ecode": "crowd-sourced", "id": "F"}},
        }
    }
    annotations, entities = parse_annotations(doc, "sample")
    assert entities == [
        {
            "level": "sample",
            "accession": "GSM1",
            "organism": ["homo sapiens"],
            "platform": ["GPL1"],
            "series": ["GSE1", "GSE2"],
            "srp": [],
            "srs": [],
            "srx": ["SRX1"],
        }
    ]
    assert [
        (a["attribute"], a["source"], a["ecode"], a["value"]) for a in annotations
    ] == [
        ("tissue", "Gemma", "expert-curated", "UBERON:1"),
        ("tissue", "Gemma", "expert-curated", "UBERON:2"),
        ("sex", "ALE", "crowd-sourced", "F"),
    ]


def test_closure_edges_reads_columns_as_terms(tmp_path):
    # M[r][c] = 1 when c is-a* r. root <- organ <- brain, reflexive.
    m = {"root": [1, 0, 0], "organ": [1, 1, 0], "brain": [1, 1, 1]}
    pq.write_table(
        pa.table({k: pa.array(v, pa.int8()) for k, v in m.items()}),
        tmp_path / "r.parquet",
    )
    edges = pa.concat_tables(closure_edges(tmp_path / "r.parquet", "uberon", batch=2))
    got = sorted(
        zip(edges["term_id"].to_pylist(), edges["ancestor_id"].to_pylist(), strict=True)
    )
    assert got == [
        ("brain", "brain"),
        ("brain", "organ"),
        ("brain", "root"),
        ("organ", "organ"),
        ("organ", "root"),
        ("root", "root"),
    ]
    assert set(edges["ontology"].to_pylist()) == {"uberon"}
