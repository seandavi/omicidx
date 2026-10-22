"""SRA extract: a standalone scheduled EL process (#153, the pilot for #154-#157).

Partitions are SRA mirror files identified by (entity, date, stage). Each
file gets a semaphore at `_semaphores/sra/{entity}/{date}_{stage}.json`.
The extractor fetches the current-batch listing, then fans an extract out
across (entity, date, stage) triples, skipping any with a semaphore.

No orchestrator: run it as `python -m omicidx.prefect.flows.sra run` (that is
what the systemd unit in `systemd/omicidx-sra-extract.service` does), logs go
to stdout -> journald, failures trip `OnFailure=ntfy-notify@%N.service`, and
the semaphores are the per-partition ledger. The module path still says
`prefect` only because the rename is #160.
"""

import concurrent.futures
import datetime
import functools
import gzip
import logging
import multiprocessing
import re
import shutil
import tempfile
from dataclasses import dataclass
from functools import lru_cache

import click
import pyarrow as pa
import pyarrow.parquet as pq
from omicidx.parsers.sra.parser import iter_sra_records
from omicidx.prefect.config import get_upath
from omicidx.prefect.semaphore import SemaphoreStore
from omicidx.prefect.source import run_extraction
from upath import UPath

log = logging.getLogger(__name__)

ENTITIES = ["study", "sample", "experiment", "run"]

#: Concurrent mirror files in flight. Was `ThreadPoolTaskRunner(max_workers=4)`;
#: kept at 4 so the migration changes the mechanism, not the load on NCBI.
MAX_WORKERS = 4

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


# ---------------------------------------------------------------------------
# PyArrow schemas (inlined from omicidx-etl to avoid the coupling)
# ---------------------------------------------------------------------------


def _get_pyarrow_schema(entity: str) -> pa.Schema:
    identifier_type = pa.struct(
        [("namespace", pa.string()), ("id", pa.string()), ("uuid", pa.string())]
    )
    attribute_type = pa.struct([("tag", pa.string()), ("value", pa.string())])
    xref_type = pa.struct([("db", pa.string()), ("id", pa.string())])
    file_alternative_type = pa.struct(
        [
            ("url", pa.string()),
            ("free_egress", pa.string()),
            ("access_type", pa.string()),
            ("org", pa.string()),
        ]
    )
    file_type = pa.struct(
        [
            ("cluster", pa.string()),
            ("filename", pa.string()),
            ("url", pa.string()),
            ("size", pa.int64()),
            ("date", pa.string()),
            ("md5", pa.string()),
            ("sratoolkit", pa.string()),
            ("alternatives", pa.list_(file_alternative_type)),
        ]
    )
    run_read_type = pa.struct(
        [
            ("index", pa.int64()),
            ("count", pa.int64()),
            ("mean_length", pa.float64()),
            ("sd_length", pa.float64()),
        ]
    )
    base_count_type = pa.struct([("base", pa.string()), ("count", pa.int64())])
    quality_type = pa.struct([("quality", pa.int32()), ("count", pa.int64())])
    tax_count_entry_type = pa.struct(
        [
            ("rank", pa.string()),
            ("name", pa.string()),
            ("parent", pa.int32()),
            ("total_count", pa.int64()),
            ("self_count", pa.int64()),
            ("tax_id", pa.int32()),
        ]
    )
    tax_analysis_type = pa.struct(
        [
            ("nspot_analyze", pa.int64()),
            ("total_spots", pa.int64()),
            ("mapped_spots", pa.int64()),
            ("tax_counts", pa.list_(tax_count_entry_type)),
        ]
    )
    experiment_read_type = pa.struct(
        [
            ("base_coord", pa.int64()),
            ("read_class", pa.string()),
            ("read_index", pa.int64()),
            ("read_type", pa.string()),
        ]
    )

    schemas = {
        "run": pa.schema(
            [
                ("accession", pa.string()),
                ("alias", pa.string()),
                ("experiment_accession", pa.string()),
                ("title", pa.string()),
                ("total_spots", pa.int64()),
                ("total_bases", pa.int64()),
                ("size", pa.int64()),
                ("avg_length", pa.float64()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("files", pa.list_(file_type)),
                ("reads", pa.list_(run_read_type)),
                ("base_counts", pa.list_(base_count_type)),
                ("qualities", pa.list_(quality_type)),
                ("tax_analysis", tax_analysis_type),
            ]
        ),
        "study": pa.schema(
            [
                ("accession", pa.string()),
                ("study_accession", pa.string()),
                ("alias", pa.string()),
                ("title", pa.string()),
                ("description", pa.string()),
                ("abstract", pa.string()),
                ("study_type", pa.string()),
                ("center_name", pa.string()),
                ("broker_name", pa.string()),
                ("BioProject", pa.string()),
                ("GEO", pa.string()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("xrefs", pa.list_(xref_type)),
                ("pubmed_ids", pa.list_(pa.string())),
            ]
        ),
        "sample": pa.schema(
            [
                ("accession", pa.string()),
                ("alias", pa.string()),
                ("title", pa.string()),
                ("organism", pa.string()),
                ("description", pa.string()),
                ("taxon_id", pa.int32()),
                ("geo", pa.string()),
                ("BioSample", pa.string()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("xrefs", pa.list_(xref_type)),
            ]
        ),
        "experiment": pa.schema(
            [
                ("accession", pa.string()),
                ("experiment_accession", pa.string()),
                ("alias", pa.string()),
                ("title", pa.string()),
                ("description", pa.string()),
                ("design", pa.string()),
                ("center_name", pa.string()),
                ("study_accession", pa.string()),
                ("sample_accession", pa.string()),
                ("platform", pa.string()),
                ("instrument_model", pa.string()),
                ("library_name", pa.string()),
                ("library_construction_protocol", pa.string()),
                ("library_layout", pa.string()),
                ("library_layout_orientation", pa.string()),
                ("library_layout_length", pa.string()),
                ("library_layout_sdev", pa.string()),
                ("library_strategy", pa.string()),
                ("library_source", pa.string()),
                ("library_selection", pa.string()),
                ("spot_length", pa.int64()),
                ("nreads", pa.int64()),
                ("identifiers", pa.list_(identifier_type)),
                ("attributes", pa.list_(attribute_type)),
                ("xrefs", pa.list_(xref_type)),
                ("reads", pa.list_(experiment_read_type)),
            ]
        ),
    }
    return schemas[entity]


# ---------------------------------------------------------------------------
# Mirror listing
# ---------------------------------------------------------------------------


def _parse_mirror_entry(url: str) -> dict | None:
    try:
        entity = None
        for e in ENTITIES:
            if e in url:
                entity = e
                break
        if entity is None:
            return None

        is_full = "Full" in url
        match = re.search(r"NCBI_SRA_Mirroring_(\d{8})", url)
        if not match:
            return None
        entry_date = datetime.datetime.strptime(match.group(1), "%Y%m%d").date()

        return {
            "url": url,
            "entity": entity,
            "is_full": is_full,
            "date": entry_date,
        }
    except (ValueError, AttributeError):
        return None


def _get_mirror_entries() -> list[dict]:
    up = UPath("https://ftp.ncbi.nlm.nih.gov/sra/reports/Mirroring/")
    all_files = list(reversed([str(f) for f in up.glob("**/*set.xml.gz")]))

    entries: list[dict] = []
    found_full = False
    out_of_full = False

    for url in all_files:
        entry = _parse_mirror_entry(url)
        if entry is None:
            continue

        if entry["is_full"] and not found_full:
            found_full = True

        if found_full and not entry["is_full"]:
            out_of_full = True

        entry["in_current_batch"] = not out_of_full
        entries.append(entry)

    return entries


def _partition_key(entry: dict) -> str:
    """Stable key per mirror file: '{date}_{stage}'."""
    stage = "Full" if entry["is_full"] else "Incremental"
    return f"{entry['date'].strftime('%Y-%m-%d')}_{stage}"


# ---------------------------------------------------------------------------
# Per-file extract
# ---------------------------------------------------------------------------


def _iter_records_from_url(url: str):
    up = UPath(url)
    with up.open("rb") as f_in, gzip.GzipFile(fileobj=f_in, mode="rb") as gz:
        yield from iter_sra_records(gz)


def _write_parquet_chunks(
    url: str,
    entity: str,
    out_dir: UPath,
    chunk_size: int = 500_000,
) -> tuple[int, int]:
    schema = _get_pyarrow_schema(entity)
    out_dir.mkdir(parents=True, exist_ok=True)

    buf: list[dict] = []
    part = 0
    total = 0

    def flush() -> None:
        nonlocal part
        if not buf:
            return
        table = pa.Table.from_pylist(buf, schema=schema)
        out_path = out_dir / f"data_{part:05d}.parquet"

        with tempfile.NamedTemporaryFile("wb", delete=False, suffix=".parquet") as tmp:
            tmp_path = tmp.name

        try:
            pq.write_table(table, tmp_path, compression="zstd")
            out_path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp_path, "rb") as src, out_path.open("wb") as dst:
                shutil.copyfileobj(src, dst)
        finally:
            UPath(tmp_path).unlink(missing_ok=True)

        part += 1
        buf.clear()

    log.info(f"Processing {url}")
    for rec in _iter_records_from_url(url):
        buf.append(rec)
        total += 1
        if len(buf) >= chunk_size:
            flush()
            log.info(f"  {entity}: {total:,} records processed so far")

    flush()
    return total, part


def _write_parquet_chunks_in_subprocess(
    url: str, entity: str, out_dir: UPath
) -> tuple[int, int]:
    """`_write_parquet_chunks` in a fresh process, so partitions parse in parallel.

    The parse is pure Python, so the driver's threads serialize on the GIL: on
    the 2026-10-01 Full batch the whole unit sat at one core and ``sample``
    (~45M records) needed ~11h (#188). One process per partition gives each
    mirror file its own core; the driver thread just waits on it. ``spawn``, not
    fork: forking a process that already has worker threads can deadlock.
    """
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=1,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=functools.partial(
            logging.basicConfig, level=logging.INFO, format=LOG_FORMAT
        ),
    ) as pool:
        return pool.submit(_write_parquet_chunks, url, entity, out_dir).result()


@lru_cache(maxsize=1)
def _mirror_index() -> dict[str, dict]:
    """Composite-key -> mirror entry for the current batch (listed once/process).

    The partition key is ``{entity}/{date}_{stage}`` — an opaque string to the
    driver. The entry (url, entity, date, stage) is resolved back here so
    ``extract(key, force)`` needs no URL argument. The driver's worker threads
    share this cache, so the mirror is globbed once per run.
    """
    entries = [e for e in _get_mirror_entries() if e["in_current_batch"]]
    return {f"{e['entity']}/{_partition_key(e)}": e for e in entries}


def extract_sra_partition(key: str, force: bool = False) -> dict:
    """Extract a single SRA mirror file to parquet, gated by a semaphore.

    ``key`` is ``{entity}/{date}_{stage}``; the mirror entry (url, date, stage)
    is resolved from the cached listing. Semaphores keep their per-entity
    layout at ``_semaphores/sra/{entity}/{date}_{stage}.json``. Retries are the
    driver's (`run_extraction`), not this function's.
    """
    entry = _mirror_index()[key]
    entity = entry["entity"]
    leaf = _partition_key(entry)
    sem = SemaphoreStore(f"sra/{entity}")

    if not force and sem.exists(leaf):
        log.info(f"sra/{entity}/{leaf}: semaphore exists, skipping")
        return {"key": key, "skipped": True}

    date_str = entry["date"].strftime("%Y-%m-%d")
    stage = "Full" if entry["is_full"] else "Incremental"
    out_dir = get_upath("sra", "raw", entity) / f"date={date_str}" / f"stage={stage}"
    records, parts = _write_parquet_chunks_in_subprocess(
        url=entry["url"], entity=entity, out_dir=out_dir
    )
    log.info(f"{entity} {leaf}: wrote {records:,} records in {parts} parquet parts")

    sem.mark_done(
        leaf,
        metadata={
            "row_count": records,
            "parquet_parts": parts,
            "source_url": entry["url"],
            "output_dir": str(out_dir),
        },
    )
    return {
        "key": key,
        "skipped": False,
        "row_count": records,
        "parquet_parts": parts,
    }


class SraSource:
    """NCBI SRA mirror files, partitioned by (entity, date, stage).

    Hides: the Mirroring listing crawl, the Full/Incremental batch cursor, the
    per-entity PyArrow schemas, and the hive-partitioned parquet layout. Past
    mirror files are immutable — a new batch gets new keys, so done == done.

    Partition key is the composite ``{entity}/{date}_{stage}`` (this is what
    shows up in the log lines). Its semaphore, however, keeps the
    per-entity split: to clear one by hand, run
    ``omicidx-prefect semaphores clear sra/{entity} {date}_{stage}`` — i.e. the
    text before the slash is the namespace suffix, the text after is the key.
    """

    name = "sra"
    extract = staticmethod(extract_sra_partition)

    def list_partitions(self, force: bool = False) -> list[str]:
        index = _mirror_index()
        if force:
            return list(index)
        done = {ent: set(SemaphoreStore(f"sra/{ent}").list_keys()) for ent in ENTITIES}
        return [
            k for k, e in index.items() if _partition_key(e) not in done[e["entity"]]
        ]


def sra_extract(force: bool = False, max_workers: int = MAX_WORKERS) -> list[dict]:
    """Extract every current-batch SRA mirror file to parquet.

    Each (entity, date, stage) triple is one partition. Semaphores live
    under `_semaphores/sra/{entity}/{date}_{stage}.json`. Pass force=True
    to re-extract every partition regardless.
    """
    # Fresh mirror listing per run: the lru_cache dedups the glob within a run
    # (threads share it) but must not persist across runs in a reused process.
    _mirror_index.cache_clear()
    return run_extraction(SraSource(), force=force, max_workers=max_workers)


# ---------------------------------------------------------------------------
# Raw retention (#194)
# ---------------------------------------------------------------------------

#: Complete Fulls kept per entity: the current and previous month, so a
#: monthly prune never races a long or overlapping Full extraction.
KEEP_FULLS = 2


@dataclass(frozen=True)
class RawPartition:
    """One `sra/raw/{entity}/date={date}/stage={stage}/` directory."""

    date: str  # YYYY-MM-DD
    stage: str  # "Full" | "Incremental"
    done: bool  # has a semaphore, i.e. the extract finished
    nbytes: int = 0


def select_raw_prune(
    partitions: list[RawPartition],
) -> tuple[str, list[RawPartition]]:
    """Return (cutoff, partitions to delete) for one entity's raw tree.

    Keeps the newest ``KEEP_FULLS`` *complete* Fulls (done semaphore) and
    everything dated on or after the older of them; deletes everything dated
    before it — Fulls and incrementals, with or without a semaphore. A Full
    without a semaphore is protected only if it is dated after the cutoff
    (the only place an in-progress Full can be); one older than both kept Fulls
    is dead — the extractor only lists NCBI's current batch, and the prune
    holds the extract flock. Raises ValueError if there is no complete Full.
    """
    fulls = sorted(
        {p.date for p in partitions if p.stage == "Full" and p.done}, reverse=True
    )
    if not fulls:
        raise ValueError("no complete Full; refusing to prune")
    cutoff = fulls[:KEEP_FULLS][-1]
    return cutoff, [p for p in partitions if p.date < cutoff]


def _list_raw_partitions(entity: str) -> list[RawPartition]:
    root = get_upath("sra", "raw", entity)
    done = set(SemaphoreStore(f"sra/{entity}").list_keys())
    sizes: dict[tuple[str, str], int] = {}
    for path, info in root.fs.find(root.path, detail=True).items():
        m = re.match(
            r"date=(\d{4}-\d{2}-\d{2})/stage=(Full|Incremental)/",
            path.removeprefix(root.path + "/"),
        )
        if m:  # anything off-layout is never touched
            sizes[m.groups()] = sizes.get(m.groups(), 0) + info["size"]
    for key in done:  # zero-row partitions: a semaphore, no files
        sizes.setdefault(tuple(key.split("_", 1)), 0)
    return [
        RawPartition(d, st, f"{d}_{st}" in done, n)
        for (d, st), n in sorted(sizes.items())
    ]


def prune_sra_raw(apply: bool = False) -> int:
    """Delete raw SRA partitions older than the retention window; return bytes.

    Dry run unless ``apply``. A deleted partition's semaphore goes with it:
    it is older than a kept complete Full, so it is out of NCBI's current
    batch and the extractor never lists it again; leaving the semaphore would
    make ``reproduce-from-raw`` glob a partition that no longer exists. The
    semaphore is removed first, so a failed delete leaves an orphan that the
    load ignores and the next prune removes.
    """
    verb = "deleting" if apply else "would delete"
    click.echo(
        f"sra raw retention: keep the last {KEEP_FULLS} complete Fulls per entity "
        "and everything dated on/after the older one; delete the rest. A Full "
        "without a semaphore is kept only if dated after that cutoff."
    )
    total = 0
    for entity in ENTITIES:
        partitions = _list_raw_partitions(entity)
        try:
            cutoff, doomed = select_raw_prune(partitions)
        except ValueError as e:
            click.echo(f"sra/{entity}: {e}")
            continue
        kept = [p for p in partitions if p.date >= cutoff]
        nbytes = sum(p.nbytes for p in doomed)
        total += nbytes
        click.echo(
            f"sra/{entity}: keep {len(kept)} partitions dated >= {cutoff} "
            f"({sum(p.nbytes for p in kept) / 1e9:.2f} GB); {verb} {len(doomed)} "
            f"({nbytes / 1e9:.2f} GB)"
        )
        for p in kept:
            if p.stage == "Full" and not p.done:
                click.echo(f"  keeping {p.date}_Full: no semaphore, after cutoff")
        sem = SemaphoreStore(f"sra/{entity}")
        for p in doomed:
            click.echo(
                f"  {verb} {p.date}_{p.stage} {p.nbytes / 1e6:,.1f} MB "
                f"semaphore={'yes' if p.done else 'no'}"
            )
            if apply:
                sem.clear(f"{p.date}_{p.stage}")
                part = get_upath(
                    "sra", "raw", entity, f"date={p.date}", f"stage={p.stage}"
                )
                if part.exists():  # a zero-row partition has no files
                    part.fs.rm(part.path, recursive=True)
    click.echo(f"sra raw: {verb} {total / 1e9:.2f} GB total")
    return total


@click.group()
def cli() -> None:
    """SRA raw extraction (`python -m omicidx.prefect.flows.sra run`)."""


@cli.command("run")
@click.option("--force", is_flag=True, help="Re-extract every partition.")
@click.option("--max-workers", default=MAX_WORKERS, show_default=True)
def run_command(force: bool, max_workers: int) -> None:
    """Extract every pending SRA mirror partition."""
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    results = sra_extract(force=force, max_workers=max_workers)
    extracted = [r for r in results if not r.get("skipped")]
    rows = sum(r.get("row_count", 0) for r in extracted)
    click.echo(
        f"sra: {len(extracted)} partitions extracted "
        f"({len(results) - len(extracted)} skipped), {rows:,} rows"
    )


if __name__ == "__main__":
    cli()
