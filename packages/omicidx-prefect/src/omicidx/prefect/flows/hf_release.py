"""Monthly HuggingFace release of the public Parquet set (#204, #201 D4).

``hf_release(date) -> tagged commit on hf.co/datasets/<HF_REPO_ID>``. The
release is a verbatim copy of one complete dated R2 folder ``v{date}/``: the
Parquet files its ``manifest.json`` lists, and the manifest itself. Nothing is
re-derived. The push is ``cdsci.publish.hf.hf_push`` (cdsci-lake ADR-0027):
one commit, tagged ``YYYY-MM`` of the publish date, remote files not in the
release deleted from ``main``; ``views.sql``/``restore.sql`` are regenerated
over paths relative to the download directory (nothing points at ``hf://``,
where anonymous reads are rate-limited), and ``README.md`` is the dataset card
built here. ``omicidx.duckdb`` is not pushed: its views read R2.

A folder is complete once ``manifest.json`` exists (publish writes it last).
A month whose tag already exists is a no-op when the tag holds this
``manifest.json`` byte for byte, and fails otherwise: a release is never
replaced. The download is resumable (files already on disk at the right size
are kept), so a failed run is simply re-run.

DOIs are minted by hand in the HF UI per tag — they lock the repo, so that
stays a deliberate act.
"""

import json
import logging
import shutil
import tempfile
from pathlib import Path

import click
from cdsci.publish.hf import hf_push
from huggingface_hub import HfApi
from huggingface_hub.errors import RepositoryNotFoundError
from omicidx.prefect.config import get_public_upath, settings
from omicidx.prefect.run import retry

log = logging.getLogger(__name__)

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


def download_manifest(date: str, workdir: Path) -> dict:
    """Copy v{date}/manifest.json into workdir and return it."""
    _download(get_public_upath(f"v{date}", "manifest.json"), workdir / "manifest.json")
    return json.loads((workdir / "manifest.json").read_text())


def download_release(date: str, manifest: dict, workdir: Path) -> list[str]:
    """Copy every Parquet file ``manifest`` lists from v{date}/ into workdir."""
    folder = get_public_upath(f"v{date}")
    paths = [t["path"] for t in manifest["tables"]]
    for rel in paths:
        _download(folder / rel, workdir / rel)
    return paths


def view_name(rel_path: str) -> str:
    """``sradb/study.parquet`` → ``sradb.study``; ``sra_runs.parquet`` → ``sra_runs``."""
    return rel_path.removesuffix(".parquet").replace("/", ".")


def build_card(tag: str, manifest: dict, tables: list[dict]) -> str:
    """Dataset card. ``tables``: the manifest's tables, each with its file's ``bytes``."""
    configs = "\n".join(
        f"- config_name: {view_name(t['path']).replace('.', '_')}\n"
        f"  data_files: {t['path']}"
        for t in tables
    )
    rows = "\n".join(
        f"| `{view_name(t['path'])}` | `{t['path']}` | {t['row_count']:,} "
        f"| {t['bytes'] / 1e9:.2f} |"
        for t in tables
    )
    total_gb = sum(t["bytes"] for t in tables) / 1e9
    date = manifest["publish_date"]
    repo = settings().hf_repo_id
    https = f"https://huggingface.co/datasets/{repo}/resolve/{tag}"
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
(series, samples, platforms), NCBI and EBI BioSample, and BioProject.
It also ships the `sradb.*` and `geometadb.*` marts. Everything is plain
Parquet that any engine can read.

This is the monthly release **`{tag}`**, a verbatim copy of the daily publish
`v{date}/`. Pin it with the `@{tag}` revision. For the daily-refreshed copy, use
R2 `latest/` at {LATEST_URL}. Documentation lives at {DOCS_URL}.

PubMed articles (`pubmed_articles.parquet`) were published through release
`2026-10` and are not in later releases. PubMed is moving to its own dataset
(https://github.com/seandavi/cdsci-lake/issues/136).

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
SELECT count(*) FROM 'hf://datasets/{repo}@{tag}/sra_studies.parquet';
```

Every table as a view: download the release, then `views.sql` (paths relative
to the download directory; `restore.sql` makes a local copy instead):

```bash
hf download {repo} --repo-type dataset --revision {tag} --local-dir omicidx-{tag}
cd omicidx-{tag} && duckdb omicidx.duckdb < views.sql
duckdb omicidx.duckdb -c "SELECT count(*) FROM sradb.study"
```

Python (polars):

```python
import polars as pl

df = pl.scan_parquet("hf://datasets/{repo}@{tag}/sra_studies.parquet").head(10).collect()
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
from NCBI (SRA, GEO, BioSample, BioProject) and EBI (BioSamples) and
remain under those providers' terms of use.

## Citation

```bibtex
@misc{{omicidx_{tag.replace("-", "_")},
  title  = {{OmicIDX, release {tag}}},
  author = {{Davis, Sean}},
  year   = {{{tag[:4]}}},
  url    = {{https://huggingface.co/datasets/{repo}/tree/{tag}}}
}}
```
"""


def hf_release(date: str | None = None, apply: bool = True) -> dict:
    """Publish v{date}/ (default: newest complete) as HF release ``YYYY-MM``.

    Dry run unless ``apply``: the folder is downloaded (and kept) and the plan
    printed.
    """
    date = date or latest_complete_date()
    tag = date[:7]
    repo = settings().hf_repo_id
    api = HfApi(token=settings().hf_token)
    workdir = Path(tempfile.gettempdir()) / "omicidx-hf-release" / f"v{date}"
    manifest = download_manifest(date, workdir)
    try:
        tags = {t.name for t in api.list_repo_refs(repo, repo_type="dataset").tags}
    except RepositoryNotFoundError:  # before the first push
        tags = set()
    if tag not in tags:
        # The tag exists: hf_push settles it from manifest.json alone (a no-op
        # or a refusal), so the ~50 GB download is only for a real push.
        download_release(date, manifest, workdir)
    commit = hf_push(
        workdir,
        api,
        repo=repo,
        tag=tag,
        card=build_card,
        message=f"OmicIDX release {tag} (v{date})",
        apply=apply,
    )
    if apply:
        shutil.rmtree(workdir)
    return {"date": date, "tag": tag, "commit": commit, "skipped": commit is None}


@click.group()
def cli() -> None:
    """Monthly HF release (`python -m omicidx.prefect.flows.hf_release run`)."""


@cli.command("run")
@click.option("--date", default=None, help="v{date} folder (default: newest complete).")
@click.option(
    "--dry-run", is_flag=True, help="Download and print the plan; push nothing."
)
def run_command(date: str | None, dry_run: bool) -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    result = hf_release(date=date, apply=not dry_run)
    click.echo(f"hf-release: {result}")


if __name__ == "__main__":
    cli()
