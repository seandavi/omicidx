"""EBI BioSample extract: a standalone scheduled EL process (#156, following #153).

Partitions are calendar days (YYYY-MM-DD), starting at 2021-01-01. Each
day gets a semaphore at `_semaphores/ebi_biosample/{YYYY-MM-DD}.json`,
including empty days (legitimately many days have zero updates). The
extractor defaults to enumerating from the start date to today, processing
only missing-semaphore days. The current day is always re-run.

No orchestrator: run it as `python -m omicidx.prefect.flows.ebi_biosample run`
(that is what `systemd/omicidx-ebi-biosample-extract.service` does), logs go to
stdout -> journald, failures trip `OnFailure=ntfy-notify@%N.service`, and the
semaphores are the per-partition ledger. The module path still says `prefect`
only because the rename is #160.
"""

import asyncio
import gzip
import logging
import shutil
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import click
import httpx
import orjson
import tenacity
from omicidx.prefect.config import get_duckdb_connection, get_duckdb_path, get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from omicidx.prefect.source import run_extraction

log = logging.getLogger(__name__)

EBI_BIOSAMPLES_BASE_URL = "https://www.ebi.ac.uk/biosamples/samples"
EBI_PAGE_SIZE = 200
EBI_REQUEST_TIMEOUT = 40.0

#: Concurrent calendar days in flight. Was `ThreadPoolTaskRunner(max_workers=4)`
#: — already threads, so this is a like-for-like swap onto the shared driver's
#: pool: mechanism changed, load on the EBI BioSamples API unchanged. Each day
#: is a cursor-paged HTTP crawl, i.e. IO-bound, so threads are the right pool.
MAX_WORKERS = 4


def _partition_filename(partition_date: date) -> str:
    return f"biosamples-{partition_date.isoformat()}.ndjson.gz"


class _SampleFetcher:
    def __init__(self, *, partition_date: date, local_path: Path) -> None:
        self.partition_date = partition_date
        self.local_path = local_path
        self.cursor = "*"
        self.next_url: str | None = None

    def _filter(self) -> str:
        d = self.partition_date.isoformat()
        return f"dt:update:from={d}until={d}"

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(10),
        wait=tenacity.wait_random_exponential(multiplier=1, max=40),
        retry=tenacity.retry_if_exception_type(httpx.HTTPError),
    )
    async def _request(self, client: httpx.AsyncClient) -> dict:
        if self.next_url is not None:
            response = await client.get(self.next_url, timeout=EBI_REQUEST_TIMEOUT)
        else:
            params = {
                "cursor": self.cursor,
                "size": EBI_PAGE_SIZE,
                "filter": self._filter(),
            }
            response = await client.get(
                EBI_BIOSAMPLES_BASE_URL,
                params=params,
                timeout=EBI_REQUEST_TIMEOUT,
            )
        response.raise_for_status()
        return response.json()

    async def _iter_samples(self, client: httpx.AsyncClient):
        while True:
            payload = await self._request(client)
            samples = payload.get("_embedded", {}).get("samples")
            if not samples:
                return
            for sample in samples:
                flattened = []
                for k, values in sample.get("characteristics", {}).items():
                    for v in values:
                        v["characteristic"] = k
                        flattened.append(v)
                sample["characteristics"] = flattened
                yield sample

            next_link = payload.get("_links", {}).get("next")
            if not next_link:
                return
            self.next_url = next_link["href"]

    async def run(self) -> int:
        count = 0
        async with httpx.AsyncClient() as client:
            with gzip.open(self.local_path, "wb") as fh:
                async for sample in self._iter_samples(client):
                    fh.write(orjson.dumps(sample))
                    fh.write(b"\n")
                    count += 1
        return count


def extract_ebi_biosample(key: str, force: bool = False) -> dict:
    """Extract one calendar-day partition of EBI BioSamples.

    The current day always re-extracts (updates accrue through the day); a past
    day with a semaphore is skipped unless ``force``. Retries are the driver's
    (`run_extraction`), not this function's; the per-page HTTP retry below is
    separate and stays.
    """
    sem = SemaphoreStore("ebi_biosample")
    # "Current day is volatile" is defined in two paired places: Ebi
    # BiosampleSource.list_partitions keeps it pending (always=[current]); this
    # guard makes it never skip on a stale semaphore. Change both together.
    if not force and key != date.today().isoformat() and sem.exists(key):
        log.info(f"ebi_biosample/{key}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    partition_date = datetime.strptime(key, "%Y-%m-%d").date()
    output_dir = get_upath("ebi_biosample", "raw")
    output_dir.mkdir(parents=True, exist_ok=True)
    final_path = output_dir / _partition_filename(partition_date)

    log.info(f"Fetching EBI biosamples for {partition_date.isoformat()}")

    with tempfile.NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as tmp:
        local_path = Path(tmp.name)

    try:
        fetcher = _SampleFetcher(partition_date=partition_date, local_path=local_path)
        record_count = asyncio.run(fetcher.run())

        if record_count == 0:
            log.info(
                f"No samples updated on {partition_date.isoformat()} "
                "(this is expected for many days)"
            )
            sem.mark_done(
                key,
                metadata={"row_count": 0, "note": "no samples updated"},
            )
            return {"key": key, "skipped": False, "row_count": 0}

        with open(local_path, "rb") as src, final_path.open("wb") as dst:
            shutil.copyfileobj(src, dst)

        size_bytes = local_path.stat().st_size
        log.info(
            f"Wrote {record_count:,} records to {final_path} "
            f"({size_bytes / (1024 * 1024):.2f} MB)"
        )
        sem.mark_done(
            key,
            metadata={
                "row_count": record_count,
                "output_path": str(final_path),
                "file_size_bytes": size_bytes,
            },
        )
        return {"key": key, "skipped": False, "row_count": record_count}
    finally:
        local_path.unlink(missing_ok=True)


def _enumerate_days(start: str = "2021-01-01", end: str | None = None) -> list[str]:
    start_d = datetime.strptime(start, "%Y-%m-%d").date()
    end_d = datetime.strptime(end, "%Y-%m-%d").date() if end else date.today()
    keys: list[str] = []
    cur = start_d
    while cur <= end_d:
        keys.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return keys


def consolidate_ebi_biosample_parquet() -> dict:
    """Consolidate per-day NDJSON into a single parquet via DuckDB.

    ponytail: kept because removing it would change behaviour, not because
    anything reads it — `ducklake_ebi_biosample.py` MERGEs from the raw NDJSON
    glob, and the public `ebi_biosample.parquet` comes out of `parquet_export`
    from the lake. Nothing in this repo consumes
    `ebi_biosample/parquet/ebi_biosamples.parquet`. If that holds after #158
    audits the downstream unit, delete this and pass `--no-consolidate` becomes
    moot.
    """
    input_glob = get_duckdb_path("ebi_biosample", "raw", "biosamples-*.ndjson.gz")
    output_path = get_duckdb_path("ebi_biosample", "parquet", "ebi_biosamples.parquet")
    sql = f"""
        COPY (
            SELECT *
            FROM read_ndjson_auto(
                '{input_glob}',
                maximum_object_size = 1000000000,
                union_by_name = true,
                ignore_errors = false
            )
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    with get_duckdb_connection() as con:
        log.info(f"Consolidating {input_glob} → {output_path}")
        con.execute(sql)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_path}')"
        ).fetchone()[0]
    log.info(f"Wrote {row_count:,} rows to {output_path}")
    return {"row_count": row_count, "output_path": output_path}


class EbiBiosampleSource:
    """EBI BioSamples, partitioned by calendar day (the ``Source`` protocol).

    Hides: the cursor-paged BioSamples API, characteristics flattening, the
    per-day NDJSON layout, and the "current day is never done" cursor.
    """

    name = "ebi_biosample"
    extract = staticmethod(extract_ebi_biosample)

    def __init__(
        self,
        start_day: str = "2021-01-01",
        end_day: str | None = None,
        rerun_current_day: bool = True,
    ) -> None:
        self.start_day = start_day
        self.end_day = end_day
        self.rerun_current_day = rerun_current_day

    def list_partitions(self, force: bool = False) -> list[str]:
        days = _enumerate_days(start=self.start_day, end=self.end_day)
        current = date.today().isoformat()
        always = [current] if self.rerun_current_day else []
        return SemaphoreStore("ebi_biosample").pending_keys(
            days, always=always, force=force
        )


def ebi_biosample_extract(
    start_day: str = "2021-01-01",
    end_day: str | None = None,
    rerun_current_day: bool = True,
    force: bool = False,
    consolidate: bool = True,
    max_workers: int = MAX_WORKERS,
) -> list[dict]:
    """Extract every calendar day whose semaphore is missing, plus today."""
    results = run_extraction(
        EbiBiosampleSource(
            start_day=start_day,
            end_day=end_day,
            rerun_current_day=rerun_current_day,
        ),
        force=force,
        max_workers=max_workers,
    )

    if consolidate:
        consolidate_ebi_biosample_parquet()

    return results


@click.group()
def cli() -> None:
    """EBI BioSample raw extraction (`python -m omicidx.prefect.flows.ebi_biosample run`)."""


@cli.command("run")
@click.option("--start-day", default="2021-01-01", show_default=True)
@click.option("--end-day", default=None)
@click.option("--force", is_flag=True, help="Re-extract every partition.")
@click.option("--no-consolidate", is_flag=True, help="Skip the parquet rollup.")
@click.option("--max-workers", default=MAX_WORKERS, show_default=True)
def run_command(
    start_day: str,
    end_day: str | None,
    force: bool,
    no_consolidate: bool,
    max_workers: int,
) -> None:
    """Extract every pending EBI BioSample day partition."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    results = ebi_biosample_extract(
        start_day=start_day,
        end_day=end_day,
        force=force,
        consolidate=not no_consolidate,
        max_workers=max_workers,
    )
    extracted = [r for r in results if not r.get("skipped")]
    rows = sum(r.get("row_count", 0) for r in extracted)
    click.echo(
        f"ebi_biosample: {len(extracted)} partitions extracted "
        f"({len(results) - len(extracted)} skipped), {rows:,} rows"
    )


if __name__ == "__main__":
    cli()
