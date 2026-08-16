#!/usr/bin/env python
"""Audit GEO month completeness against eutils counts — without fetching records.

READ-ONLY. Answers the open question in #174 ("how to detect it") for a few
hundred requests instead of a re-crawl.

`esearch` returns `esearchresult.count` for a query while returning zero
records, so one request per month says how many accessions GEO holds in that
month's Update Date window. What we wrote is already in the month's semaphore
metadata (`{"GSE": .., "GSM": .., "GPL": ..}`). Comparing the two costs ~185
tiny requests; re-extracting to find out costs ~68 hours.

**The comparison is one-sided, and that is what makes it sound.** Partitions are
keyed by Update Date, and an update date only moves forward: a record extracted
into 2008-06 that was touched again in 2024 has since left the 2008-06 window.
So today's count can only be <= the count at extraction time. `written < today`
is therefore proof of a hole, and the deficit is a lower bound. `written >=
today` is not proof of health — it just isn't proof of loss.

Compare like with like: the written figure sums GSE+GSM+GPL, so the query must
not be restricted by `[etyp]`. (#174's headline "68,296 of 73,548" compared
GSM-only writes against the all-entity count and so overstated 2020-06's loss
as 7%; measured like-for-like it is 4.7%.)

Usage (safe to run while an extract is in flight — see SLEEP):

    uv run python packages/omicidx-prefect/scripts/geo_audit.py
"""

import sys
import time
from datetime import datetime

import httpx
from omicidx.prefect.flows.geo import _month_range
from omicidx.prefect.semaphore import SemaphoreStore

EUTILS = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

#: Serial, and well under the 3 req/s anonymous eutils limit. This audit must
#: never become the thing that throttles a running extract — hammering one NCBI
#: endpoint is the whole subject of #154.
SLEEP = 0.5


def geo_count(client: httpx.Client, key: str) -> int:
    """How many GEO accessions currently fall in month `key`'s update window."""
    start, end = _month_range(datetime.strptime(key, "%Y-%m").date())
    r = client.get(
        EUTILS,
        params={
            "db": "gds",
            "term": (
                "(GSM[etyp] OR GSE[etyp] OR GPL[etyp]) AND "
                f'("{start.strftime("%Y/%m/%d")}"[Update Date] : '
                f'"{end.strftime("%Y/%m/%d")}"[Update Date])'
            ),
            "retmode": "json",
            "retmax": 0,
        },
    )
    r.raise_for_status()
    return int(r.json()["esearchresult"]["count"])


def main() -> None:
    store = SemaphoreStore("geo")
    keys = sorted(store.list_keys())
    print(f"auditing {len(keys)} months with semaphores", file=sys.stderr)

    short: list[tuple[str, int, int, int, float]] = []
    ok = 0
    no_counts: list[str] = []

    with httpx.Client(timeout=60) as client:
        for i, key in enumerate(keys, 1):
            meta = (store.read(key) or {}).get("metadata") or {}
            written = sum(meta.get(e, 0) for e in ("GSE", "GSM", "GPL"))
            if not written:
                # Pre-2005-05 semaphores predate the counts-in-metadata format.
                no_counts.append(key)
                continue
            try:
                have = geo_count(client, key)
            except Exception as e:
                print(f"{key}: eutils error {type(e).__name__}", file=sys.stderr)
                continue
            deficit = have - written
            if deficit > 0:
                short.append((key, written, have, deficit, deficit / have))
                print(
                    f"{key}: written {written:,} < geo {have:,} "
                    f"-> missing >= {deficit:,} ({deficit / have:.1%})",
                    flush=True,
                )
            else:
                ok += 1
            if i % 25 == 0:
                print(f"  ...{i}/{len(keys)}", file=sys.stderr, flush=True)
            time.sleep(SLEEP)

    print(f"\n=== {len(short)} short, {ok} at-or-above, {len(no_counts)} no counts ===")
    if short:
        missing = sum(s[3] for s in short)
        total = sum(s[2] for s in short)
        print(
            f"total missing across short months: {missing:,} of {total:,} "
            f"({missing / total:.1%})"
        )
        print("\nworst by rate:")
        for key, written, have, _, rate in sorted(short, key=lambda s: -s[4])[:15]:
            print(f"  {key}  {written:>8,} / {have:>8,}   -{rate:.1%}")
        print("\nre-extract list:")
        print(" ".join(s[0] for s in sorted(short)))
    if no_counts:
        print(f"\nno usable counts in semaphore metadata: {' '.join(no_counts)}")


if __name__ == "__main__":
    main()
