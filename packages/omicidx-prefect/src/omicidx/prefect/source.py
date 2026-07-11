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
"""

from typing import Protocol, runtime_checkable

from prefect import Task, get_run_logger


@runtime_checkable
class Source(Protocol):
    """A single extractable omicidx source. Deep: two members hide the crawl."""

    #: Semaphore / lake-registry namespace, e.g. "geo", "sra", "pubmed".
    name: str

    #: Prefect task with signature ``(key: str, force: bool) -> dict``. Kept as
    #: an attribute (not a method) so it stays a submittable ``Task``.
    extract: Task

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


def run_extraction(source: Source, force: bool = False) -> list[dict]:
    """List a source's pending partitions and extract each as a Prefect task.

    The task runner (thread vs process pool, concurrency) is chosen by the
    calling ``@flow``; this driver just fans ``source.extract`` out across the
    keys and waits. Returns each extract's result dict.
    """
    log = get_run_logger()
    keys = source.list_partitions(force=force)
    log.info(f"{source.name}: {len(keys)} partitions pending extraction")
    futures = [source.extract.submit(key=key, force=force) for key in keys]
    return [f.result() for f in futures]
