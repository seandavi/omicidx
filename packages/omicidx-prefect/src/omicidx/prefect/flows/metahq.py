"""MetaHQ extract: the newest Zenodo release, landed whole, one partition per version (#266).

MetaHQ (KrishnanLab, https://doi.org/10.5281/zenodo.17663086, CC-BY-NC-4.0)
harmonizes GEO sample/series tissue, disease, sex and age annotations from ~15
curated sources onto UBERON/MONDO.

Each run *is* the new-version check: it asks Zenodo for the concept record's
newest version, and that version string (``v1.2.0``) is the partition key. A
version already landed is a no-op (one API call), so the daily timer is cheap.
A new one is downloaded, checked against Zenodo's md5, and copied byte-for-byte
to ``{PUBLISH_ROOT}/metahq/raw/{version}/metahq_data.tar.gz``. Its semaphore,
``_semaphores/metahq/{version}.json``, is written only after that, so a marked
partition is always a complete tarball. The downstream chain loads the newest
semaphored version.

Run it as ``python -m omicidx.prefect.flows.metahq run`` (that is what
``systemd/omicidx-metahq-extract.service`` does).
"""

import hashlib
import logging
import shutil
import tempfile

import click
import httpx
from omicidx.prefect.config import get_upath
from omicidx.prefect.flows.biosample import _download
from omicidx.prefect.semaphore import SemaphoreStore
from omicidx.prefect.source import run_extraction
from upath import UPath

log = logging.getLogger(__name__)

# Concept record: resolves to whichever version is newest.
CONCEPT_RECORD = "17663086"
LATEST_URL = f"https://zenodo.org/api/records/{CONCEPT_RECORD}/versions/latest"
TARBALL = "metahq_data.tar.gz"
SEMAPHORE_NAMESPACE = "metahq"


def _tarball_path(key: str) -> UPath:
    return get_upath("metahq", "raw", key, TARBALL)


def latest_tarball() -> tuple[str, UPath] | None:
    """(version, path) of the newest completely-landed release, or None."""
    keys = SemaphoreStore(SEMAPHORE_NAMESPACE).list_keys()
    if not keys:
        return None
    # "v1.10.0" > "v1.9.0": compare as versions, not strings. ponytail: a
    # non-numeric version ("v2.0.0-rc1") raises here; use packaging if that happens.
    key = max(keys, key=lambda k: tuple(int(x) for x in k.lstrip("v").split(".")))
    return key, _tarball_path(key)


def latest_release() -> dict:
    """Zenodo's newest version of the concept record: version, record id, file url/md5."""
    response = httpx.get(LATEST_URL, timeout=60, follow_redirects=True)
    response.raise_for_status()
    record = response.json()
    (file,) = [f for f in record["files"] if f["key"] == TARBALL]
    return {
        "version": record["metadata"]["version"],
        "record_id": record["id"],
        "doi": record["doi"],
        "url": file["links"]["self"],
        "md5": file["checksum"].removeprefix("md5:"),
    }


def extract_metahq(key: str, force: bool = False) -> dict:
    """Land the release ``key`` (must be Zenodo's newest), md5-checked, then mark it done."""
    sem = SemaphoreStore(SEMAPHORE_NAMESPACE)
    if not force and sem.exists(key):
        log.info(f"metahq/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    release = latest_release()
    if release["version"] != key:
        raise ValueError(f"Zenodo's newest MetaHQ is {release['version']}, not {key}")
    output_path = _tarball_path(key)
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as tmp:
        _download(release["url"], tmp.name)
        md5 = hashlib.md5()
        with open(tmp.name, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                md5.update(chunk)
        if md5.hexdigest() != release["md5"]:
            raise ValueError(
                f"{TARBALL} md5 {md5.hexdigest()} != Zenodo {release['md5']}"
            )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp.name, "rb") as src, output_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)
        size_bytes = UPath(tmp.name).stat().st_size

    meta = {
        "output_path": str(output_path),
        "file_size_bytes": size_bytes,
        **release,
    }
    sem.mark_done(key, metadata=meta)
    log.info(f"Landed MetaHQ {key} ({size_bytes / 1e6:.1f} MB) at {output_path}")
    return {"key": key, "skipped": False, **meta}


class MetaHQSource:
    """MetaHQ on Zenodo, one partition per released version (the ``Source`` protocol).

    Hides: the Zenodo concept record, version discovery, md5 check, raw layout.
    """

    name = SEMAPHORE_NAMESPACE
    extract = staticmethod(extract_metahq)

    def list_partitions(self, force: bool = False) -> list[str]:
        return SemaphoreStore(SEMAPHORE_NAMESPACE).pending_keys(
            [latest_release()["version"]], force=force
        )


def metahq_extract(force: bool = False) -> list[dict]:
    """Land Zenodo's newest MetaHQ release unless it is already landed."""
    return run_extraction(MetaHQSource(), force=force, max_workers=1)


@click.group()
def cli() -> None:
    """MetaHQ raw extraction (`python -m omicidx.prefect.flows.metahq run`)."""


@cli.command("run")
@click.option("--force", is_flag=True, help="Re-extract even if the version is landed.")
def run_command(force: bool) -> None:
    """Check Zenodo for a new MetaHQ release and land it on R2."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    results = metahq_extract(force=force)
    if results:
        click.echo(f"metahq: {results[0]['output_path']}")
    else:
        click.echo("metahq: no new release")


if __name__ == "__main__":
    cli()
