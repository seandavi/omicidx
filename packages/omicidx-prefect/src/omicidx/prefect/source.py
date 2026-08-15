"""The narrow Source extraction protocol (spec §2 "Source extractor", §4 A1).

One deep module per omicidx source hides its NCBI/EBI crawl quirks, partition
scheme, output format, and incrementality cursor behind two methods:

    list_partitions(force) -> list[str]   # opaque keys still needing extraction
    extract(key, force)    -> dict        # extract + mark one partition

`run_extraction` is the generic driver that replaces the list -> gate -> submit
-> collect loop each extract flow used to hand-roll. The driver never learns a
source's cursor or layout: it only ever sees opaque string keys. This contract
is precedent-setting (spec §5) — future omicidx sources and sibling producers
model their extractors on it.

The driver owns fan-out, retries, and logging with no orchestrator involved
(#149): a bounded ``ThreadPoolExecutor``, tenacity retries, stdlib logging. A
source is therefore runnable from a systemd unit, a Prefect flow, or a shell —
none of which it can tell apart.
"""

import concurrent.futures
import logging
from collections.abc import Callable
from typing import Protocol, runtime_checkable

import tenacity

log = logging.getLogger(__name__)

#: Fan-out width when the caller does not choose one. Small on purpose: every
#: source in this repo crawls NCBI or EBI, and stacked concurrent runs hammering
#: those mirrors is the live hypothesis for GEO's 2020-07 wedge (#154). Each
#: source's own entry point passes its number explicitly.
DEFAULT_MAX_WORKERS = 4

#: Retry policy for one partition's extract, matching what `@task(retries=2,
#: retry_delay_seconds=60)` gave us before. Module-level so a test can shrink
#: the wait; ponytail: one fixed policy for every source until one needs its own.
RETRY_ATTEMPTS = 3
RETRY_WAIT_SECONDS = 60


@runtime_checkable
class Source(Protocol):
    """A single extractable omicidx source. Deep: two members hide the crawl."""

    #: Semaphore / lake-registry namespace, e.g. "geo", "sra", "pubmed".
    name: str

    #: Plain callable ``(key: str, force: bool) -> dict``. Kept as an attribute
    #: (not a method) so a module-level function can be bound with
    #: ``staticmethod`` and stay independently callable and testable.
    extract: Callable[..., dict]

    def list_partitions(self, force: bool = False) -> list[str]:
        """Opaque keys that still need extraction this run.

        The source owns its cursor: which keys exist (crawl the mirror, the FTP
        directory, a month/day range) and which are still pending (its own
        semaphores, plus any always-rerun "current" partition). ``force=True``
        returns every live key, bypassing the source's gate for a backfill. The
        returned keys are opaque to the caller; only the source's own ``extract``
        interprets them.
        """
        ...


def run_extraction(
    source: Source,
    force: bool = False,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> list[dict]:
    """List a source's pending partitions and extract each, bounded and retried.

    Fans ``source.extract`` across the pending keys on a ``ThreadPoolExecutor``
    of exactly ``max_workers`` threads — the one lever that decides how hard we
    hit a source's mirror, so it is a named argument the caller sets, never a
    number buried here. Each partition is retried per the module's tenacity
    policy; if a partition still fails, every other partition still finishes and
    then the first failure is re-raised, so the process exits non-zero (and, on
    a timer, trips ``OnFailure=``). Results come back in key order.
    """
    keys = source.list_partitions(force=force)
    log.info(
        "%s: %d partitions pending extraction (max_workers=%d)",
        source.name,
        len(keys),
        max_workers,
    )
    if not keys:
        return []

    extract = tenacity.retry(
        stop=tenacity.stop_after_attempt(RETRY_ATTEMPTS),
        wait=tenacity.wait_fixed(RETRY_WAIT_SECONDS),
        before_sleep=tenacity.before_sleep_log(log, logging.WARNING),
        reraise=True,
    )(source.extract)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix=f"{source.name}-extract"
    ) as pool:
        futures = [pool.submit(extract, key=key, force=force) for key in keys]
        return [f.result() for f in futures]
