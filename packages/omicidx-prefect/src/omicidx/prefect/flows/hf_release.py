"""Monthly HuggingFace release of the public Parquet set (#204, #201 D4).

``hf_release(date) -> tagged commit on hf.co/datasets/omicidx/omicidx``. The
release is a verbatim copy of one complete dated R2 folder ``v{date}/``: its
flat Parquet (top level + ``sradb/`` + ``geometadb/``) and ``manifest.json``.
Nothing is re-derived. Two files are generated for the HF copy:

- ``views.sql`` — one view per Parquet file, pointing at
  ``hf://datasets/omicidx/omicidx@<tag>/…`` so the release is self-contained.
- ``README.md`` — the dataset card, built from ``manifest.json`` + the files.

A folder is complete once ``manifest.json`` exists (publish writes it last).
One commit per release, tagged ``YYYY-MM`` of the publish date. A month whose
tag already exists is skipped, so a re-fired timer is a no-op. The download is
resumable (files already on disk at the right size are kept) and the upload is
xet-deduplicated, so a failed run is simply re-run.

DOIs are minted by hand in the HF UI per tag — they lock the repo, so that
stays a deliberate act.
"""

import json
import logging
import shutil
import tempfile
from pathlib import Path

import click
import pyarrow.parquet as pq
from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi
from omicidx.prefect.config import get_public_upath, settings
from omicidx.prefect.flows.parquet_export import MART_EXPORTS
from omicidx.prefect.run import retry

log = logging.getLogger(__name__)

HF_REPO = "omicidx/omicidx"
MART_SCHEMAS = sorted({schema for schema, _ in MART_EXPORTS})
DOCS_URL = "https://omicidx.cancerdatasci.org"
LATEST_URL = "https://data.omicidx.cancerdatasci.org/latest/"
CODE_URL = "https://github.com/seandavi/omicidx"


def latest_complete_date() -> str:
    """Newest ``v{date}`` folder in the public bucket that has a manifest.json."""
    dates = sorted(
        (c.name[1:] for c in get_public_upath().iterdir() if c.name.startswith("v")),
        reverse=True,
    )
    for date in dates:
        if get_public_upath(f"v{date}", "manifest.json").exists():
            return date
    raise RuntimeError("No complete dated folder (with manifest.json) found")


@retry
def _download(src, dst: Path) -> None:
    size = src.stat().st_size
    if dst.exists() and dst.stat().st_size == size:
        return
    dst.parent.mkdir(parents=True, exist_ok=True)
    log.info(f"Downloading {src.path} ({size / 1e9:.2f} GB)")
    src.fs.get_file(src.path, str(dst))


def download_release(date: str, workdir: Path) -> list[str]:
    """Copy v{date}/'s Parquet + manifest.json into workdir; return Parquet paths.

    Skips everything else in the folder (``data/``, ``catalog.ducklake``,
    ``omicidx.duckdb``, the R2 ``views.sql``).
    """
    folder = get_public_upath(f"v{date}")
    files = [c for c in folder.iterdir() if c.name.endswith(".parquet")]
    for schema in MART_SCHEMAS:
        files += [c for c in (folder / schema).iterdir() if c.name.endswith(".parquet")]
    rel_paths = []
    for f in files:
        rel = f.path.split(f"v{date}/", 1)[1]
        _download(f, workdir / rel)
        rel_paths.append(rel)
    _download(folder / "manifest.json", workdir / "manifest.json")
    return sorted(rel_paths)


def view_name(rel_path: str) -> str:
    """``sradb/study.parquet`` → ``sradb.study``; ``sra_runs.parquet`` → ``sra_runs``."""
    return rel_path.removesuffix(".parquet").replace("/", ".")


def hf_url(tag: str, rel_path: str) -> str:
    return f"hf://datasets/{HF_REPO}@{tag}/{rel_path}"


def build_views_sql(tag: str, rel_paths: list[str]) -> str:
    """CREATE VIEW per Parquet file, pinned to the release tag on HF."""
    schemas = sorted({p.split("/")[0] for p in rel_paths if "/" in p})
    stmts = [f"CREATE SCHEMA IF NOT EXISTS {s};" for s in schemas]
    stmts += [
        f"CREATE OR REPLACE VIEW {view_name(p)} AS "
        f"SELECT * FROM read_parquet('{hf_url(tag, p)}');"
        for p in rel_paths
    ]
    return "\n".join(stmts) + "\n"


def build_card(tag: str, manifest: dict, tables: list[dict]) -> str:
    """Dataset card. ``tables``: ``{"path", "rows", "bytes"}`` per Parquet file."""
    configs = "\n".join(
        f"- config_name: {view_name(t['path']).replace('.', '_')}\n"
        f"  data_files: {t['path']}"
        for t in tables
    )
    rows = "\n".join(
        f"| `{view_name(t['path'])}` | `{t['path']}` | {t['rows']:,} "
        f"| {t['bytes'] / 1e9:.2f} |"
        for t in tables
    )
    total_gb = sum(t["bytes"] for t in tables) / 1e9
    date = manifest["publish_date"]
    https = f"https://huggingface.co/datasets/{HF_REPO}/resolve/{tag}"
    return f"""---
license: other
license_name: upstream-ncbi-ebi-terms
license_link: https://www.ncbi.nlm.nih.gov/home/about/policies/
pretty_name: OmicIDX
tags:
- biology
- genomics
- metadata
configs:
{configs}
---

# OmicIDX {tag}

OmicIDX is a regularly refreshed, analysis-ready copy of public genomics
metadata: NCBI SRA (studies, samples, experiments, runs, accessions), NCBI GEO
(series, samples, platforms), NCBI and EBI BioSample, BioProject, and PubMed.
It also ships the `sradb.*` and `geometadb.*` marts. Everything is plain
Parquet that any engine can read.

This is the monthly release **`{tag}`**, a verbatim copy of the daily publish
`v{date}/`. Pin it with the `@{tag}` revision. For the daily-refreshed copy, use
R2 `latest/` at {LATEST_URL}. Documentation lives at {DOCS_URL}.

## Provenance

| | |
|---|---|
| Publish date | {date} |
| Published at | {manifest.get("published_at", "")} |
| Transform SHA | `{manifest.get("transform_sha", "")}` |
| Lake snapshot id | {manifest.get("lake_snapshot_id", "")} |

The full provenance record is [`manifest.json`](manifest.json).

## Tables

{len(tables)} files, {total_gb:.1f} GB in total.

| View | File | Rows | Size (GB) |
|---|---|---:|---:|
{rows}

## Querying

DuckDB, one file:

```sql
SELECT count(*) FROM 'hf://datasets/{HF_REPO}@{tag}/sra_studies.parquet';
```

DuckDB, every table as a view (`views.sql` points at this release):

```bash
curl -sL {https}/views.sql | duckdb omicidx.duckdb
duckdb omicidx.duckdb -c "SELECT count(*) FROM sradb.study"
```

Python (polars):

```python
import polars as pl

df = pl.scan_parquet("hf://datasets/{HF_REPO}@{tag}/sra_studies.parquet").head(10).collect()
```

R (arrow):

```r
url <- "{https}/sra_studies.parquet"
tf <- tempfile(fileext = ".parquet")
download.file(url, tf, mode = "wb")
studies <- arrow::read_parquet(tf)
```

## License

The OmicIDX code is MIT-licensed ({CODE_URL}). The data are redistributed
from NCBI (SRA, GEO, BioSample, BioProject, PubMed) and EBI (BioSamples) and
remain under those providers' terms of use.

## Citation

```bibtex
@misc{{omicidx_{tag.replace("-", "_")},
  title  = {{OmicIDX, release {tag}}},
  author = {{Davis, Sean}},
  year   = {{{tag[:4]}}},
  url    = {{https://huggingface.co/datasets/{HF_REPO}/tree/{tag}}}
}}
```
"""


def hf_release(date: str | None = None) -> dict:
    """Publish v{date}/ (default: newest complete) as HF release ``YYYY-MM``."""
    date = date or latest_complete_date()
    tag = date[:7]
    api = HfApi(token=settings().hf_token)
    api.create_repo(HF_REPO, repo_type="dataset", private=False, exist_ok=True)
    tags = {t.name for t in api.list_repo_refs(HF_REPO, repo_type="dataset").tags}
    if tag in tags:
        log.info(f"Release {tag} already exists on {HF_REPO}; nothing to do")
        return {"date": date, "tag": tag, "skipped": True}

    workdir = Path(tempfile.gettempdir()) / "omicidx-hf-release" / f"v{date}"
    rel_paths = download_release(date, workdir)
    manifest = json.loads((workdir / "manifest.json").read_text())
    tables = [
        {
            "path": p,
            "rows": pq.ParquetFile(workdir / p).metadata.num_rows,
            "bytes": (workdir / p).stat().st_size,
        }
        for p in rel_paths
    ]
    (workdir / "views.sql").write_text(build_views_sql(tag, rel_paths))
    (workdir / "README.md").write_text(build_card(tag, manifest, tables))

    # create_commit, not upload_folder: with hf_xet installed, upload_folder
    # splits a large upload into several commits; a release is exactly one.
    # Remote files not in this release are deleted, so a table dropped since
    # the last release leaves main (it stays reachable at older tags).
    local = [
        p.relative_to(workdir).as_posix() for p in workdir.rglob("*") if p.is_file()
    ]
    ops: list = [CommitOperationAdd(p, workdir / p) for p in local]
    ops += [
        CommitOperationDelete(p)
        for p in api.list_repo_files(HF_REPO, repo_type="dataset")
        if p not in local and p != ".gitattributes"
    ]
    commit = api.create_commit(
        HF_REPO,
        ops,
        repo_type="dataset",
        commit_message=f"OmicIDX release {tag} (v{date})",
    )
    api.create_tag(
        HF_REPO,
        repo_type="dataset",
        tag=tag,
        revision=commit.oid,
        tag_message=f"OmicIDX release {tag} (v{date})",
    )
    log.info(f"Released {HF_REPO}@{tag} from v{date} ({commit.oid})")
    shutil.rmtree(workdir)
    return {"date": date, "tag": tag, "commit": commit.oid}


@click.group()
def cli() -> None:
    """Monthly HF release (`python -m omicidx.prefect.flows.hf_release run`)."""


@cli.command("run")
@click.option("--date", default=None, help="v{date} folder (default: newest complete).")
def run_command(date: str | None) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    result = hf_release(date=date)
    click.echo(f"hf-release: {result}")


if __name__ == "__main__":
    cli()
