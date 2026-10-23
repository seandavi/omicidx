"""NCBI Taxonomy extract: the taxdump, landed whole, one partition per day (#226).

GEO (and SRA, BioSample) records carry the organism *name* as of submission, and
GEO renames organisms without bumping ``last_update_date``, so our incremental
extract never sees the rename. The taxids are right; the names go stale. The
transform resolves names from taxids against the current taxonomy, which this
source lands.

``taxdump.tar.gz`` is copied byte-for-byte to
``{PUBLISH_ROOT}/ncbi_taxonomy/raw/{YYYY-MM-DD}/taxdump.tar.gz`` (all members,
not just the three the loader reads) and gated by a per-day semaphore at
``_semaphores/ncbi_taxonomy/{YYYY-MM-DD}.json``. The download is checked against
NCBI's published ``.md5`` before the semaphore is written, so a marked partition
is always a complete dump.

Run it as ``python -m omicidx.prefect.flows.ncbi_taxonomy run`` (that is what
``systemd/omicidx-ncbi-taxonomy-extract.service`` does).
"""

import hashlib
import logging
import shutil
import tempfile
from datetime import date

import click
import httpx
from omicidx.prefect.config import get_upath
from omicidx.prefect.flows.biosample import _download
from omicidx.prefect.semaphore import SemaphoreStore
from omicidx.prefect.source import run_extraction
from upath import UPath

log = logging.getLogger(__name__)

TAXDUMP_URL = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/taxdump.tar.gz"
SEMAPHORE_NAMESPACE = "ncbi_taxonomy"


def _taxdump_path(key: str) -> UPath:
    return get_upath("ncbi_taxonomy", "raw", key, "taxdump.tar.gz")


def latest_taxdump() -> UPath | None:
    """The newest completely-landed taxdump, or None before the first extract."""
    keys = SemaphoreStore(SEMAPHORE_NAMESPACE).list_keys()  # sorted ISO dates
    return _taxdump_path(keys[-1]) if keys else None


def extract_ncbi_taxonomy(key: str, force: bool = False) -> dict:
    """Download today's taxdump, verify its md5, land it whole, mark the day done."""
    sem = SemaphoreStore(SEMAPHORE_NAMESPACE)
    if not force and sem.exists(key):
        log.info(f"ncbi_taxonomy/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    response = httpx.get(f"{TAXDUMP_URL}.md5", timeout=60)
    response.raise_for_status()
    expected_md5 = response.text.split()[0]
    output_path = _taxdump_path(key)
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmp:
        _download(TAXDUMP_URL, tmp.name)
        md5 = hashlib.md5()
        with open(tmp.name, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                md5.update(chunk)
        # A mismatch is usually NCBI regenerating the dump mid-download; raising
        # lets the driver's retry fetch a consistent pair.
        if md5.hexdigest() != expected_md5:
            raise ValueError(
                f"taxdump md5 {md5.hexdigest()} != published {expected_md5}"
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp.name, "rb") as src, output_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        size_bytes = UPath(tmp.name).stat().st_size

    meta = {
        "output_path": str(output_path),
        "file_size_bytes": size_bytes,
        "md5": expected_md5,
        "source_url": TAXDUMP_URL,
    }
    sem.mark_done(key, metadata=meta)
    log.info(f"Landed {size_bytes / 1e6:.1f} MB taxdump at {output_path}")
    return {"key": key, "skipped": False, **meta}


class NcbiTaxonomySource:
    """NCBI Taxonomy, one full taxdump per calendar day (the ``Source`` protocol).

    Hides: the FTP URL, md5 verification, and the per-day raw layout.
    """

    name = SEMAPHORE_NAMESPACE
    extract = staticmethod(extract_ncbi_taxonomy)

    def list_partitions(self, force: bool = False) -> list[str]:
        return SemaphoreStore(SEMAPHORE_NAMESPACE).pending_keys(
            [date.today().isoformat()], force=force
        )


def ncbi_taxonomy_extract(force: bool = False) -> list[dict]:
    """Land today's taxdump unless today's semaphore already exists."""
    return run_extraction(NcbiTaxonomySource(), force=force, max_workers=1)


@click.group()
def cli() -> None:
    """NCBI Taxonomy raw extraction (`python -m omicidx.prefect.flows.ncbi_taxonomy run`)."""


@cli.command("run")
@click.option(
    "--force", is_flag=True, help="Re-extract even if today's semaphore exists."
)
def run_command(force: bool) -> None:
    """Land today's NCBI taxdump on R2."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    results = ncbi_taxonomy_extract(force=force)
    if results:
        click.echo(f"ncbi_taxonomy: {results[0]['output_path']}")
    else:
        click.echo("ncbi_taxonomy: skipped (today's semaphore exists)")


if __name__ == "__main__":
    cli()
