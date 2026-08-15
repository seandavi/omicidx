"""GEO extract: a standalone scheduled EL process (#154, pattern from #153).

Partitions are calendar months (YYYY-MM). Each month gets a semaphore
under `_semaphores/geo/{YYYY-MM}.json`. By default the extractor processes
the current month every run (`rerun_current_month=True`) and skips any
historical month that already has a semaphore.

No orchestrator: run it as `python -m omicidx.prefect.flows.geo run` (that is
what `systemd/omicidx-geo-extract.service` does), logs go to stdout ->
journald, failures trip `OnFailure=ntfy-notify@%N.service`, and the semaphores
are the per-partition ledger. The module path still says `prefect` only
because the rename is #160.

`geo_rna_seq_counts_flow` is a separate, non-partitioned refresh of the
GSEs-with-RNA-seq-counts file. It is NOT part of the extract and still runs
inside `raw_extract_flow`, so it keeps its Prefect decorators for now.

## Why 2020-07 "hung" (#154)

It did not. A month is one acc.cgi fetch per accession -- 62,419 of them for
2020-07 -- and `MONTH_FETCH_CONCURRENCY` in-flight requests is the entire
throughput knob. Measured single-process against live NCBI: **~43 req/s, 22
minutes for all of 2020-07, 0.2 GB RSS, zero errors**. What actually ran in
prod was up to 13 stacked `geo-extract` flow runs x
`ProcessPoolTaskRunner(max_workers=2)` x 30-way asyncio fan-out = up to 780
concurrent requests at one NCBI endpoint. NCBI degrades under that (measured:
at 30-wide from a cold client, 10% of requests hit the 30s read timeout and
throughput fell 10x vs 8-wide), and the semaphore timeline records the result:
one month completed per **14-23 hours**, i.e. ~1.2 req/s. No month ever
finished inside a run's lifetime, so no semaphore was written, so the next
stacked run redid it -- 9 starts of 2020-07, 27 rewrites of 2020-06.

The fix is therefore the *shape* of the run, not the extractor: one process,
one month at a time (`MAX_WORKERS = 1`), fan-out only inside the month. The
guards below make a genuinely pathological month loud instead of silent.
"""

import asyncio
import gzip
import logging
import re
import shutil
import tempfile
import time
from datetime import date, datetime, timedelta

import click
import httpx
import orjson
import polars as pl
import tenacity
from dateutil.relativedelta import relativedelta
from omicidx.parsers.geo import parser as gp
from omicidx.prefect.config import get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from omicidx.prefect.source import run_extraction
from upath import UPath

from prefect import flow, get_run_logger, task

log = logging.getLogger(__name__)

#: Months extracted in parallel. **One.** Was `ProcessPoolTaskRunner(max_workers=2)`.
#: Unlike sra, a GEO partition is not one file -- it is tens of thousands of
#: requests already fanned `MONTH_FETCH_CONCURRENCY`-wide at a single NCBI
#: endpoint. A second month in flight buys no parallelism the month does not
#: already have; it only doubles the load on the endpoint that wedged (#154).
MAX_WORKERS = 1

#: In-flight acc.cgi requests within one month. 30 measured clean single-process
#: (43 req/s sustained over 62,419 accessions). Raise this and you are back in
#: the regime that produced the wedge -- it is the one knob that matters.
MONTH_FETCH_CONCURRENCY = 30

#: Wall-clock ceiling for one month's fetch. A full month measured 22 min, so
#: this is ~5x headroom: a month that blows through it is pathological and
#: should fail the process (and trip `OnFailure=`) rather than grind silently,
#: which is exactly what nobody noticed for six years' worth of months.
MONTH_TIMEOUT_SECONDS = 7200

#: Fraction of a month's accessions that may fail to fetch before the month is
#: not "done". Individual accessions do get withdrawn from GEO, so this is not
#: zero; but 2020-06 was marked done having written 68,296 of 73,548 GSMs (7%
#: silently dropped by `return_exceptions=True`), and a semaphore that says
#: "done" over a 7% hole is worse than a failure.
MAX_FETCH_FAILURE_RATE = 0.02


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_retryable(e: BaseException) -> bool:
    """Transport hiccups and NCBI backpressure -- not 4xx, which never heals."""
    if isinstance(e, httpx.HTTPStatusError):
        return e.response.status_code == 429 or 500 <= e.response.status_code < 600
    return isinstance(
        e, httpx.RemoteProtocolError | httpx.ConnectError | httpx.TimeoutException
    )


def _month_key(partition_date: date) -> str:
    return partition_date.strftime("%Y-%m")


def _month_range(partition_date: date) -> tuple[date, date]:
    start = partition_date.replace(day=1)
    end = (start + relativedelta(months=1)) - timedelta(days=1)
    return start, end


def _entrezid_to_geo(entrezid: str) -> str:
    if entrezid.startswith("2"):
        return re.sub("^20*", "GSE", entrezid)
    if entrezid.startswith("1"):
        return re.sub("^10*", "GPL", entrezid)
    if entrezid.startswith("3"):
        return re.sub("^30*", "GSM", entrezid)
    raise ValueError(f"Expected entrezid to start with 1, 2, or 3: {entrezid}")


@tenacity.retry(
    wait=tenacity.wait_exponential_jitter(2, 60),
    stop=tenacity.stop_after_attempt(8),
    retry=tenacity.retry_if_exception(_is_retryable),
)
async def _fetch_accessions(start_date: date, end_date: date) -> list[str]:
    accessions: list[str] = []
    offset = 0
    retmax = 5000

    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            response = await client.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                params={
                    "db": "gds",
                    "term": (
                        f"(GSM[etyp] OR GSE[etyp] OR GPL[etyp]) AND "
                        f'("{start_date.strftime("%Y/%m/%d")}"[Update Date] : '
                        f'"{end_date.strftime("%Y/%m/%d")}"[Update Date])'
                    ),
                    "retmode": "json",
                    "retmax": retmax,
                    "retstart": offset,
                },
            )
            response.raise_for_status()
            result = response.json()
            ids = result["esearchresult"]["idlist"]
            for eid in ids:
                accessions.append(_entrezid_to_geo(eid))
            if len(ids) < retmax:
                break
            offset += retmax
            await asyncio.sleep(0.4)

    return accessions


@tenacity.retry(
    wait=tenacity.wait_exponential_jitter(2, 30),
    stop=tenacity.stop_after_attempt(5),
    retry=tenacity.retry_if_exception(_is_retryable),
)
async def _fetch_soft(accession: str, client: httpx.AsyncClient) -> str:
    params = {
        "acc": accession,
        "targ": "self",
        "form": "text",
        "view": "quick" if accession.startswith("GSM") else "brief",
    }
    response = await client.get(
        "https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi", params=params
    )
    response.raise_for_status()
    return response.text


async def _fetch_and_parse(
    accessions: list[str],
    concurrency: int = MONTH_FETCH_CONCURRENCY,
    timeout_seconds: int = MONTH_TIMEOUT_SECONDS,
) -> tuple[dict[str, list[dict]], int]:
    """Fetch + parse every accession's SOFT record. Returns (records, n_failed).

    Failures are tolerated per-accession (accessions do get withdrawn) but
    *counted* and returned, so the caller can refuse to mark a month done over
    a large hole. They used to vanish into `return_exceptions=True`.
    """
    results: dict[str, list[dict]] = {"GSE": [], "GSM": [], "GPL": []}
    semaphore = asyncio.Semaphore(concurrency)
    failed = 0

    async def _one(acc: str, client: httpx.AsyncClient) -> None:
        nonlocal failed
        async with semaphore:
            try:
                text = await _fetch_soft(acc, client)
            except Exception as exc:  # noqa: BLE001 - counted, then reported
                failed += 1
                log.warning(
                    "geo: %s fetch failed: %s: %s", acc, type(exc).__name__, exc
                )
                return
            for entity in gp.iter_soft_entities(text):
                prefix = entity.accession[:3]
                if prefix in results:
                    results[prefix].append(entity.model_dump())

    async with httpx.AsyncClient(timeout=30) as client:
        # The deadline is the loud guard: every socket here already has a
        # timeout, so nothing blocks forever, but "62k requests at 1.2/s"
        # looks exactly like a hang from outside. This turns it into a crash.
        async with asyncio.timeout(timeout_seconds):
            await asyncio.gather(
                *(_one(acc, client) for acc in accessions), return_exceptions=True
            )

    return results, failed


def _write_ndjson_gz(records: list[dict], path: UPath) -> int:
    with tempfile.NamedTemporaryFile(suffix=".ndjson.gz", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        with gzip.open(tmp_path, "wb") as f:
            for rec in records:
                f.write(orjson.dumps(rec))
                f.write(b"\n")

        path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_path, "rb") as src, path.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    finally:
        UPath(tmp_path).unlink(missing_ok=True)

    return len(records)


# ---------------------------------------------------------------------------
# Per-month extract task
# ---------------------------------------------------------------------------


def extract_month(key: str, force: bool = False) -> dict:
    """Extract one calendar-month partition of GEO metadata.

    The current month always re-extracts (it accrues records during the month);
    a past month with a semaphore is skipped unless ``force``.
    """
    sem = SemaphoreStore("geo")

    # "Current month is volatile" is defined in two paired places: GeoSource.
    # list_partitions keeps it pending (always=[current]); this guard makes it
    # never skip on a stale semaphore. Change both together.
    if not force and key != _month_key(date.today()) and sem.exists(key):
        log.info("geo/%s: semaphore exists, skipping", key)
        return {"key": key, "skipped": True}

    partition_date = datetime.strptime(key, "%Y-%m").date()
    start_date, end_date = _month_range(partition_date)
    output_base = get_upath("geo", "raw")

    log.info("geo/%s: processing %s to %s", key, start_date, end_date)

    accessions = asyncio.run(_fetch_accessions(start_date, end_date))
    log.info("geo/%s: %d accessions", key, len(accessions))

    counts = {"GSE": 0, "GSM": 0, "GPL": 0}

    if accessions:
        started = time.monotonic()
        parsed, failed = asyncio.run(_fetch_and_parse(accessions))
        log.info(
            "geo/%s: fetched %d accessions in %.1f min (%d failed)",
            key,
            len(accessions),
            (time.monotonic() - started) / 60,
            failed,
        )
        if failed > len(accessions) * MAX_FETCH_FAILURE_RATE:
            raise RuntimeError(
                f"geo/{key}: {failed}/{len(accessions)} accessions failed to fetch "
                f"(> {MAX_FETCH_FAILURE_RATE:.0%}); refusing to mark the month done"
            )
    else:
        parsed, failed = {"GSE": [], "GSM": [], "GPL": []}, 0

    for prefix, entity in [("GSE", "gse"), ("GSM", "gsm"), ("GPL", "gpl")]:
        path = (
            output_base
            / entity
            / f"year={start_date.strftime('%Y')}"
            / f"month={start_date.strftime('%m')}"
            / "data_0.ndjson.gz"
        )
        n = _write_ndjson_gz(parsed[prefix], path)
        counts[prefix] = n
        log.info("geo/%s: wrote %d %s records to %s", key, n, prefix, path)

    sem.mark_done(
        key,
        metadata={
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "accessions": len(accessions),
            "failed": failed,
            **counts,
        },
    )
    return {"key": key, "skipped": False, "failed": failed, **counts}


# ---------------------------------------------------------------------------
# RNA-seq counts (non-partitioned)
# ---------------------------------------------------------------------------


@task(retries=2, retry_delay_seconds=30)
def fetch_rna_seq_counts() -> dict:
    log = get_run_logger()
    offset = 0
    retmax = 5000
    accessions: list[dict] = []

    with httpx.Client(timeout=60) as client:
        while True:
            response = client.get(
                "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi",
                params={
                    "db": "gds",
                    "term": '"rnaseq+counts"[filter]',
                    "retmode": "json",
                    "retmax": retmax,
                    "retstart": offset,
                },
            )
            response.raise_for_status()
            ids = response.json()["esearchresult"]["idlist"]
            for eid in ids:
                accessions.append({"accession": _entrezid_to_geo(eid)})
            if len(ids) < retmax:
                break
            offset += retmax
            time.sleep(0.5)

    df = pl.DataFrame(accessions)
    outpath = get_upath("geo", "raw", "gse_with_rna_seq_counts.parquet")
    outpath.parent.mkdir(parents=True, exist_ok=True)

    with outpath.open("wb") as f:
        df.write_parquet(f, use_pyarrow=True, compression="zstd")

    log.info(f"Wrote {len(accessions)} GSEs with RNA-seq counts to {outpath}")
    return {"row_count": len(accessions), "output_path": str(outpath)}


# ---------------------------------------------------------------------------
# Source + entry points
# ---------------------------------------------------------------------------


def _enumerate_months(start: str = "2005-01", end: str | None = None) -> list[str]:
    start_d = datetime.strptime(start, "%Y-%m").date().replace(day=1)
    end_d = (
        datetime.strptime(end, "%Y-%m").date().replace(day=1)
        if end
        else date.today().replace(day=1)
    )
    keys: list[str] = []
    cur = start_d
    while cur <= end_d:
        keys.append(_month_key(cur))
        cur = cur + relativedelta(months=1)
    return keys


class GeoSource:
    """GEO metadata, partitioned by calendar month (the ``Source`` protocol).

    Hides: the eutils accession search, per-entity SOFT fetch/parse, the
    NDJSON layout, and the "current month is never done" cursor.
    """

    name = "geo"
    extract = staticmethod(extract_month)

    def __init__(
        self,
        start_month: str = "2005-01",
        end_month: str | None = None,
        rerun_current_month: bool = True,
    ) -> None:
        self.start_month = start_month
        self.end_month = end_month
        self.rerun_current_month = rerun_current_month

    def list_partitions(self, force: bool = False) -> list[str]:
        months = _enumerate_months(start=self.start_month, end=self.end_month)
        current = _month_key(date.today())
        always = [current] if self.rerun_current_month else []
        return SemaphoreStore("geo").pending_keys(months, always=always, force=force)


def geo_extract(
    start_month: str = "2005-01",
    end_month: str | None = None,
    rerun_current_month: bool = True,
    force: bool = False,
    max_workers: int = MAX_WORKERS,
) -> list[dict]:
    """Extract GEO metadata, one monthly partition at a time.

    By default iterates from ``start_month`` to the current month, skipping
    any month whose semaphore exists. Set ``force=True`` to re-extract
    everything in the range. Set ``rerun_current_month=False`` to also
    skip the current month if its semaphore exists.
    """
    return run_extraction(
        GeoSource(
            start_month=start_month,
            end_month=end_month,
            rerun_current_month=rerun_current_month,
        ),
        force=force,
        max_workers=max_workers,
    )


@flow(name="geo-rna-seq-counts")
def geo_rna_seq_counts_flow() -> None:
    """Refresh the (small) GSE-with-RNA-seq-counts file. Not partitioned.

    Still a Prefect flow: it is not part of the extract and stays in
    `raw_extract_flow` until the downstream unit (#158) replaces it.
    """
    fetch_rna_seq_counts()


@click.group()
def cli() -> None:
    """GEO raw extraction (`python -m omicidx.prefect.flows.geo run`)."""


@cli.command("run")
@click.option("--start-month", default="2005-01", show_default=True)
@click.option("--end-month", default=None, help="Defaults to the current month.")
@click.option("--force", is_flag=True, help="Re-extract every month in the range.")
@click.option("--max-workers", default=MAX_WORKERS, show_default=True)
def run_command(
    start_month: str, end_month: str | None, force: bool, max_workers: int
) -> None:
    """Extract every pending GEO monthly partition."""
    # force=True is load-bearing, not cargo cult. This module still imports
    # `prefect` for `geo_rna_seq_counts_flow`, and that import installs a
    # PrefectConsoleHandler on the *root* logger at level WARNING -- which makes
    # a plain `basicConfig()` a silent no-op and drops every INFO line this
    # process emits. Caught only by running the entry point: a month extracted
    # correctly and logged absolutely nothing to journald. Drop the `force` when
    # geo_rna_seq_counts_flow leaves (#158) and the prefect import goes.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    # httpx logs one INFO line per request; a month is ~62k requests, so at
    # INFO this unit alone would write ~5M lines/night into journald.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    results = geo_extract(
        start_month=start_month,
        end_month=end_month,
        force=force,
        max_workers=max_workers,
    )
    extracted = [r for r in results if not r.get("skipped")]
    rows = sum(r.get("GSE", 0) + r.get("GSM", 0) + r.get("GPL", 0) for r in extracted)
    click.echo(
        f"geo: {len(extracted)} months extracted "
        f"({len(results) - len(extracted)} skipped), {rows:,} records"
    )


if __name__ == "__main__":
    cli()
