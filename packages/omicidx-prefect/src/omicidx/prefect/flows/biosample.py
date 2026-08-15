"""BioSample and BioProject extract: one standalone scheduled EL process (#155).

Neither source is partitioned — each run overwrites the single output
file. We still emit a semaphore per run (keyed by today's date) so
operators can see when the last successful run finished.

No orchestrator: run it as `python -m omicidx.prefect.flows.biosample run`
(that is what `systemd/omicidx-biosample-extract.service` does), logs go to
stdout -> journald, failures trip `OnFailure=ntfy-notify@%N.service`. The
module path still says `prefect` only because the rename is #160.

ponytail: no `run_extraction`. That driver exists to fan a source's *many*
partitions across a bounded pool; here `list_partitions()` returns at most one
key (today's date), so the pool, the `Source` classes, and the "N partitions
pending" line were ceremony around a single direct call. The semaphore gate
lives inside `extract_*` already, so the gating is unchanged. The driver's
blanket tenacity retry is also the wrong policy for a multi-GB full dump: the
flaky part is the download, which `_download` already retries on its own, and
re-pulling gigabytes three times on a parse error would burn the unit's whole
`TimeoutStartSec=` instead of failing loudly and letting tomorrow's timer retry.
"""

import gzip
import logging
import shutil
import tempfile
import time
from datetime import date

import click
import httpx
import orjson
import tenacity
from omicidx.parsers.biosample import BioProjectParser, BioSampleParser
from omicidx.prefect.config import get_duckdb_connection, get_duckdb_path, get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from upath import UPath

log = logging.getLogger(__name__)

BIOSAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/biosample/biosample_set.xml.gz"
BIOPROJECT_URL = "https://ftp.ncbi.nlm.nih.gov/bioproject/bioproject.xml"


@tenacity.retry(
    wait=tenacity.wait_exponential(multiplier=1, min=4, max=30),
    retry=tenacity.retry_if_exception_type(httpx.RequestError),
    stop=tenacity.stop_after_attempt(5),
)
def _download(url: str, dest: str) -> None:
    log.info(f"Downloading {url}")
    with (
        open(dest, "wb") as f,
        httpx.stream("GET", url, timeout=120, follow_redirects=True) as response,
    ):
        response.raise_for_status()
        for chunk in response.iter_bytes():
            f.write(chunk)
    log.info(f"Download complete: {url}")


def _extract_entity(
    *,
    url: str,
    entity: str,
    parser_class: type,
    use_gzip_input: bool,
    output_dir: UPath,
) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "data.jsonl.gz"

    start = time.time()
    count = 0

    with tempfile.NamedTemporaryFile(suffix=".download") as dl_tmp:
        _download(url, dl_tmp.name)

        open_fn = gzip.open if use_gzip_input else open

        with tempfile.NamedTemporaryFile(suffix=".jsonl.gz", delete=False) as out_tmp:
            out_tmp_path = out_tmp.name

        try:
            with (
                open_fn(dl_tmp.name, "rb") as infile,
                gzip.open(out_tmp_path, "wb") as outfile,
            ):
                for obj in parser_class(infile, validate_with_schema=False):
                    outfile.write(orjson.dumps(obj))
                    outfile.write(b"\n")
                    count += 1
                    if count % 100_000 == 0:
                        log.info(f"{entity}: parsed {count:,} records")

            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(out_tmp_path, "rb") as src, output_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        finally:
            UPath(out_tmp_path).unlink(missing_ok=True)

    duration = time.time() - start
    log.info(
        f"{entity}: wrote {count:,} records to {output_path} "
        f"in {duration:.1f}s ({count / max(duration, 1e-3):.0f} rec/s)"
    )
    return {
        "row_count": count,
        "output_path": str(output_path),
        "duration_seconds": duration,
        "source_url": url,
    }


def extract_biosample(key: str, force: bool = False) -> dict:
    sem = SemaphoreStore("biosample")
    if not force and sem.exists(key):
        log.info(f"biosample/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    output_dir = get_upath("biosample", "raw")
    meta = _extract_entity(
        url=BIOSAMPLE_URL,
        entity="biosample",
        parser_class=BioSampleParser,
        use_gzip_input=True,
        output_dir=output_dir,
    )
    sem.mark_done(key, metadata=meta)
    return {"key": key, "skipped": False, **meta}


def extract_bioproject(key: str, force: bool = False) -> dict:
    sem = SemaphoreStore("bioproject")
    if not force and sem.exists(key):
        log.info(f"bioproject/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    output_dir = get_upath("bioproject", "raw")
    meta = _extract_entity(
        url=BIOPROJECT_URL,
        entity="bioproject",
        parser_class=BioProjectParser,
        use_gzip_input=False,
        output_dir=output_dir,
    )
    sem.mark_done(key, metadata=meta)
    return {"key": key, "skipped": False, **meta}


def bioproject_to_parquet() -> dict:
    """Convert BioProject JSONL → parquet via DuckDB. Always runs (cheap)."""
    input_path = get_duckdb_path("bioproject", "raw", "data.jsonl.gz")
    output_path = get_duckdb_path("bioproject", "parquet", "bioprojects.parquet")
    # ponytail: kept as-is by the #155 migration (mechanical: same work, no
    # orchestrator). Nothing in this repo reads
    # `{PUBLISH_ROOT}/bioproject/parquet/bioprojects.parquet` any more — the
    # DuckLake loader reads the JSONL, and the public `bioprojects.parquet`
    # comes out of `parquet_export` from the lake table. Delete when someone
    # confirms no external consumer; not this ticket's call.
    sql = f"""
        COPY (
            SELECT
                trim(title) as title,
                trim(description) as description,
                trim(name) as name,
                trim(accession) as accession,
                publications,
                locus_tags,
                release_date,
                data_types,
                external_links
            FROM read_ndjson_auto(
                '{input_path}',
                maximum_object_size = 1000000000
            )
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """
    with get_duckdb_connection() as con:
        log.info(f"Converting {input_path} to {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
    log.info(f"Wrote {row_count:,} rows to {output_path}")
    return {"row_count": row_count, "output_path": output_path}


def biosample_extract(force: bool = False) -> dict:
    """Extract today's BioSample full dump (no-op if today's semaphore exists)."""
    return extract_biosample(date.today().isoformat(), force=force)


def bioproject_extract(force: bool = False) -> dict:
    """Extract today's BioProject full dump, then refresh its parquet copy."""
    result = extract_bioproject(date.today().isoformat(), force=force)
    bioproject_to_parquet()
    return result


@click.group()
def cli() -> None:
    """BioSample/BioProject raw extraction.

    (`python -m omicidx.prefect.flows.biosample run`)
    """


@cli.command("run")
@click.option(
    "--force", is_flag=True, help="Re-extract even if today's semaphore exists."
)
def run_command(force: bool) -> None:
    """Extract both NCBI full dumps: BioSample, then BioProject.

    One unit, both sources: they are the same machinery over two URLs, each a
    single unpartitioned file gated by a per-day semaphore, and the DuckLake
    loader consumes them together. BioSample first, preserving the order
    `raw_extract_flow` ran them in — so a BioSample failure still blocks
    BioProject exactly as it did before.
    """
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    for name, fn in (
        ("biosample", biosample_extract),
        ("bioproject", bioproject_extract),
    ):
        result = fn(force=force)
        if result.get("skipped"):
            click.echo(f"{name}: skipped (semaphore for {result['key']} exists)")
        else:
            click.echo(
                f"{name}: {result['row_count']:,} rows -> {result['output_path']}"
            )


if __name__ == "__main__":
    cli()
