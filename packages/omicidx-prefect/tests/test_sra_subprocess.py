"""SRA partitions parse in a spawned process (#188): the round trip must still work."""

import gzip

import pyarrow.parquet as pq
from omicidx.prefect.flows.sra import _write_parquet_chunks_in_subprocess
from upath import UPath

STUDY = """<STUDY accession="SRP{n:06d}" alias="s{n}" center_name="NCBI">
  <IDENTIFIERS><PRIMARY_ID>SRP{n:06d}</PRIMARY_ID></IDENTIFIERS>
  <DESCRIPTOR>
    <STUDY_TITLE>t{n}</STUDY_TITLE>
    <STUDY_TYPE existing_study_type="Other"/>
  </DESCRIPTOR>
</STUDY>"""


def test_parse_in_subprocess_roundtrip(tmp_path):
    src = tmp_path / "meta_study_set.xml.gz"
    body = "".join(STUDY.format(n=n) for n in range(3))
    src.write_bytes(gzip.compress(f"<STUDY_SET>{body}</STUDY_SET>".encode()))

    out = UPath(tmp_path / "out")
    assert _write_parquet_chunks_in_subprocess(str(src), "study", out) == (3, 1)
    table = pq.read_table(tmp_path / "out" / "data_00000.parquet")
    assert table.column("accession").to_pylist() == [
        "SRP000000",
        "SRP000001",
        "SRP000002",
    ]
