"""PubMed extract: a standalone scheduled EL process (#157, following #153).

Partitions are individual PubMed XML files (e.g., `pubmed25n0001`).
Each file gets a semaphore under `_semaphores/pubmed/{file_id}.json`.
The extractor lists the NCBI FTP and extracts every file whose semaphore is
missing, writing one parquet per file.

No orchestrator: run it as `python -m omicidx.prefect.flows.pubmed run` (that
is what `systemd/omicidx-pubmed-extract.service` does), logs go to stdout ->
journald, failures trip `OnFailure=ntfy-notify@%N.service`, and the semaphores
are the per-partition ledger. The module path still says `prefect` only because
the rename is #160.
"""

import logging
import re
import shutil
import tempfile
from datetime import datetime
from functools import lru_cache
from urllib.request import urlretrieve

import click
import pubmed_parser as pp
import pyarrow as pa
import pyarrow.parquet as pq
from omicidx.prefect.config import get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from omicidx.prefect.source import run_extraction
from upath import UPath

log = logging.getLogger(__name__)

PUBMED_BASE = UPath("https://ftp.ncbi.nlm.nih.gov/pubmed")
_XML_GZ_RE = re.compile(r"^(pubmed\d+n\d+)\.xml\.gz$")

#: Concurrent PubMed files in flight. Was `ProcessPoolTaskRunner(max_workers=12)`;
#: kept at 12 so the migration changes the mechanism, not the load on NCBI.
#: ponytail: the driver's pool is threads, so the MEDLINE parse is now GIL-bound
#: where it used to be truly parallel — irrelevant on the typical run (0 pending)
#: and on incremental days (a handful of files), but an annual baseline drop
#: (~1,200 files) will be download-bound-then-serial-parse. Swap the driver to a
#: ProcessPoolExecutor if a baseline year ever runs long enough to matter.
MAX_WORKERS = 12


@lru_cache(maxsize=1)
def _list_pubmed_files() -> dict[str, str]:
    """List PubMed XML files via HTTPS. Returns {partition_key: url_string}.

    Cached per process: the extracts re-resolve a key's URL from this index so
    ``extract(key, force)`` stays a uniform two-arg call without the URL leaking
    through it. The driver's worker threads share this cache, so the FTP is
    listed once per run.
    """
    result: dict[str, str] = {}
    for subdir in ["baseline", "updatefiles"]:
        for entry in (PUBMED_BASE / subdir).iterdir():
            m = _XML_GZ_RE.match(entry.name)
            if m:
                result[m.group(1)] = str(entry)
    return result


def _sanitize_utf8(obj):
    """Recursively replace invalid UTF-8 bytes in any string values."""
    if isinstance(obj, str):
        return obj.encode("utf-8", errors="surrogateescape").decode(
            "utf-8", errors="replace"
        )
    if isinstance(obj, dict):
        return {k: _sanitize_utf8(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_utf8(v) for v in obj]
    return obj


def extract_pubmed_file(key: str, force: bool = False) -> dict:
    """Extract a single PubMed XML file to parquet, gated by a semaphore.

    Retries are the driver's (`run_extraction`), not this function's.
    """
    sem = SemaphoreStore("pubmed")
    if not force and sem.exists(key):
        log.info(f"pubmed/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    url = _list_pubmed_files()[key]
    output_dir = get_upath("pubmed", "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{key}.parquet"

    with (
        tempfile.NamedTemporaryFile(suffix=".xml.gz") as tmp_xml,
        tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_parquet,
    ):
        log.info(f"Downloading {url}")
        urlretrieve(str(url), filename=tmp_xml.name)

        log.info(f"Parsing {key}")
        articles = list(
            pp.parse_medline_xml(
                tmp_xml.name,
                year_info_only=False,
                nlm_category=True,
                author_list=True,
                reference_list=True,
                parse_downto_mesh_subterms=True,
            )
        )

        for obj in articles:
            obj["_inserted_at"] = datetime.now()
            obj["_read_from"] = str(url)

        articles = [_sanitize_utf8(a) for a in articles]
        table = pa.Table.from_pylist(articles)
        pq.write_table(table, tmp_parquet.name, compression="zstd")

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_parquet.name, "rb") as src, output_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

    log.info(f"Wrote {len(articles)} articles to {output_path}")
    sem.mark_done(
        key,
        metadata={
            "row_count": len(articles),
            "output_path": str(output_path),
            "source_url": str(url),
        },
    )
    return {"key": key, "skipped": False, "row_count": len(articles)}


class PubmedSource:
    """PubMed baseline + update files, one partition per XML file.

    Hides: the FTP baseline/updatefiles listing, MEDLINE XML parsing, UTF-8
    sanitizing, and the per-file parquet layout. Immutable files (no cursor):
    a file is done once, forever.
    """

    name = "pubmed"
    extract = staticmethod(extract_pubmed_file)

    def list_partitions(self, force: bool = False) -> list[str]:
        available = _list_pubmed_files()
        if force:
            return sorted(available)
        done = set(SemaphoreStore("pubmed").list_keys())
        return sorted(set(available) - done)


def pubmed_extract(force: bool = False, max_workers: int = MAX_WORKERS) -> list[dict]:
    """Extract every PubMed file whose semaphore is missing."""
    # Fresh FTP listing per run: the lru_cache dedups the listing within a run
    # (threads share it) but must not persist across runs in a reused process.
    _list_pubmed_files.cache_clear()
    return run_extraction(PubmedSource(), force=force, max_workers=max_workers)


@click.group()
def cli() -> None:
    """PubMed raw extraction (`python -m omicidx.prefect.flows.pubmed run`)."""


@cli.command("run")
@click.option("--force", is_flag=True, help="Re-extract every partition.")
@click.option("--max-workers", default=MAX_WORKERS, show_default=True)
def run_command(force: bool, max_workers: int) -> None:
    """Extract every pending PubMed partition."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    results = pubmed_extract(force=force, max_workers=max_workers)
    extracted = [r for r in results if not r.get("skipped")]
    rows = sum(r.get("row_count", 0) for r in extracted)
    click.echo(
        f"pubmed: {len(extracted)} partitions extracted "
        f"({len(results) - len(extracted)} skipped), {rows:,} rows"
    )


if __name__ == "__main__":
    cli()
