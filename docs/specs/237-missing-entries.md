# Spec: GEO months extracted before they ended are never re-extracted (#237)

Issue: <https://github.com/seandavi/omicidx/issues/237>. Branch:
`issue-237-missing-entries`.

Design record read: `CLAUDE.md` (there is no `AGENTS.md` in this repo; `CLAUDE.md`
plays that role), `README.md`, `docs/specs/omicidx-deliverables.md`,
`packages/omicidx-prefect/{README.md,DUCKLAKE.md}`, `systemd/README.md`. Where
this spec departs from them it says so under "Departures from the design record".

## Problem

The published `geo_series`/`geo_samples` have no rows with `last_update_date`
2026-08-18 to 2026-08-31. A GEO month partition is one eutils search by
`[Update Date]` plus one `acc.cgi` fetch per hit. `extract_month`
(`packages/omicidx-prefect/src/omicidx/prefect/flows/geo.py`) treats a month's
semaphore as proof that the month is done, except for the *current* month
(`key != _month_key(date.today())`). Once a month stops being current, its
semaphore makes it skip forever, however early in the month that semaphore was
written.

Read-only check of the live semaphores (`_semaphores/geo/*.json`, 262 keys,
2026-10-27):

| key | `completed_at` (UTC) | final? |
|---|---|---|
| 2026-07 | 2026-08-17T16:42 | yes |
| **2026-08** | **2026-08-17T16:54** | **no: written 15 days before the month ended** |
| 2026-09 | 2026-10-20T22:15 | yes |
| 2026-10 | 2026-10-27T06:52 | current month |

No other key has a semaphore written in or before its own month. So 2026-08 is
the only month with a hole today.

The hole will come back without a fix, even when the timer runs every night.
The timer fires at 06:00 UTC (+≤30 min). The host runs on America/Denver time,
so `date.today()` at that hour is the previous Denver date in winter (23:xx
MST) and the current Denver date in summer (00:xx MDT). Either way, the last
nightly run that treats a month as current runs at or before its final
midnight. Updates made late on the last day, and in summer the whole last day,
are never fetched. Any outage or backfill across a month boundary loses the
rest of the month, which is what happened to August.

## Goal

1. A GEO month's semaphore means "this month's extract is final". It is written
   only by an extract whose eutils search started after the month *settled*,
   meaning after the month ended plus a margin. Before that, the month stays
   pending every run.
2. A month that has not settled is re-extracted on every run, whether or not
   it has a semaphore. This replaces the current-month special case, and it
   covers backfills and timer outages that span a month boundary.
3. Repair the 2026-08 hole that is already published (operator step, below).

## Non-goals

- **EBI BioSample** (`flows/ebi_biosample.py`) has the same "current day is
  volatile" shape. It is out of scope because its timer fires at 02:00 UTC,
  which is 19:00/20:00 Denver on the *previous* Denver date. So each day is
  marked done after its UTC day has ended. No hole is known, and its daily
  semaphore count makes a per-key fix a separate decision. Note it in the PR
  as a possible follow-up. Do not change it here.
- No change to `SemaphoreStore` (`semaphore.py`). The fix uses its existing
  `exists`/`mark_done`/`clear`/`pending_keys`.
- No change to `scripts/geo_audit.py`. With this fix, unsettled months have no
  semaphore, so the audit simply does not see them.
- No reading of semaphore contents in `list_partitions`. Rejected alternative:
  "pending if `completed_at` < settle time". It repairs old holes on its own,
  but it costs one GET per done month. Measured: **56 s for 262 reads**,
  against 1.4 s for the single LIST that `pending_keys` was built around. The
  one old hole (2026-08) is repaired by hand instead.
- No change to the downstream chain (`ducklake_geo.py` reads the raw globs; a
  rewritten month file is picked up by the next `ducklake-load`).
- No change to the systemd units or timer schedule.

## Design

All code changes are in
`packages/omicidx-prefect/src/omicidx/prefect/flows/geo.py`. No new
dependencies: `datetime.UTC` (Python ≥ 3.11, already used by `semaphore.py`)
and `dateutil.relativedelta` (already imported in `geo.py`). Verified:
`datetime.strptime("2026-12", "%Y-%m").replace(tzinfo=UTC) +
relativedelta(months=1) + timedelta(days=1)` gives
`2027-01-02 00:00:00+00:00` in the workspace venv.

### 1. Settle constant and function (new, module level, next to `_month_range`)

```python
#: How long after a month ends before its extract is final. NCBI stamps
#: `[Update Date]` in US Eastern time and indexes into eutils with some lag, and
#: the boundary below is UTC midnight, so a search run right at it can still
#: miss the month's last day. One day clears both. With the 06:00 UTC timer
#: the previous month is re-extracted on the 1st and on the 2nd, and marked
#: done on the 2nd (#237).
MONTH_SETTLE = timedelta(days=1)


def month_settles_at(key: str) -> datetime:
    """UTC instant after which an extract of month `key` (YYYY-MM) is final."""
    start = datetime.strptime(key, "%Y-%m").replace(tzinfo=UTC)
    return start + relativedelta(months=1) + MONTH_SETTLE
```

Import change: `from datetime import UTC, date, datetime, timedelta`. `date`
and `_month_key` stay, because `_enumerate_months` still uses them.

### 2. `extract_month(key: str, force: bool = False) -> dict` (signature unchanged)

- **Skip guard.** Replace
  `if not force and key != _month_key(date.today()) and sem.exists(key):` with
  `if not force and datetime.now(UTC) >= month_settles_at(key) and sem.exists(key):`.
  Keep the log message and return value. Update the paired-place comment
  above it: the two places are now "unsettled months are always pending"
  (`GeoSource.list_partitions`) and this guard.
- **Record when the search started.** Set `queried_at = datetime.now(UTC)`
  just before `asyncio.run(_fetch_accessions(...))`. The search, not
  completion, is what decides how complete the month is.
- **Write the semaphore only for a settled month.** Write the three NDJSON
  files exactly as today, then:
  - if `queried_at >= month_settles_at(key)`: `sem.mark_done(key, metadata=...)`,
    with the metadata unchanged.
  - else: `sem.clear(key)` and log
    `"geo/%s: month not settled until %s; wrote data, no semaphore"`. The
    `clear` is required, not defensive. The live `2026-10` semaphore was
    written by the old code. Without the `clear`, once October settles that
    stale semaphore would make it skip forever: the same bug again.
- The `MAX_FETCH_FAILURE_RATE` raise stays before both branches (unchanged).
- Return value: unchanged keys. Do not add a new field.
- Docstring: "A month re-extracts every run until it settles (`month_settles_at`);
  only an extract that started after that writes the semaphore, so a semaphore
  means final. A settled month with a semaphore is skipped unless `force`."

### 3. `GeoSource` and `geo_extract`

- `GeoSource.list_partitions(self, force: bool = False) -> list[str]`:

  ```python
  months = _enumerate_months(start=self.start_month, end=self.end_month)
  now = datetime.now(UTC)
  unsettled = [k for k in months if month_settles_at(k) > now]
  return SemaphoreStore("geo").pending_keys(months, always=unsettled, force=force)
  ```

  `unsettled` is computed from the clock, so it costs no storage reads. It
  holds the current month, plus the previous month until 00:00 UTC on the 2nd.
- **Remove the `rerun_current_month` parameter** from `GeoSource.__init__` and
  `geo_extract`, and remove the `rerun_current_month=` argument passed between
  them. With this rule, `False` could only mean "trust a semaphore written
  before the month ended", which is this bug. Nothing in the repo passes it.
  The CLI never exposed it, and the only mention is
  `packages/omicidx-prefect/README.md`.
- Docstrings: `GeoSource` "Hides: … and the 'a month is never done until it
  settles' cursor". `geo_extract`: drop the `rerun_current_month` sentence.
  Module docstring lines 3–6: semaphores mark *settled* months; unsettled
  months (current, and previous until the 2nd) re-run every time.

### 4. Docs (one line each)

- `packages/omicidx-prefect/README.md` ("The current-period partition …"
  paragraph): GEO re-runs any month until a day after it ends, and only then
  writes its semaphore. Drop `rerun_current_month=False` and keep the EBI
  `rerun_current_day=False` sentence.
- `packages/omicidx-prefect/DUCKLAKE.md:60`: `current month always re-runs` →
  `re-runs until 1 day after month end; semaphore = final`.
- `docs/overview/data-sources.qmd:25`: "The current month is re-fetched every
  night because it keeps growing" → also say the previous month is fetched
  again after it ends, so its last days are complete.

### 5. Tests (`packages/omicidx-prefect/tests/test_geo.py`, reuse `_stub_env` and `_completed`)

The stubs follow the existing pattern:
`monkeypatch.setattr(geo, "_fetch_accessions", lambda start, end: _completed([]))`.
An empty accession list writes three empty NDJSON files under `tmp_path` and
needs no network. The current month (`geo._month_key(date.today())`) is
unsettled on any real clock, so none of the tests patch the clock.

- `test_month_settles_at_is_one_day_after_month_end`:
  `month_settles_at("2026-08") == datetime(2026, 9, 2, tzinfo=UTC)` and
  `month_settles_at("2026-12") == datetime(2027, 1, 2, tzinfo=UTC)` (year
  rollover).
- `test_extract_month_writes_no_semaphore_for_an_unsettled_month` (the #237
  regression): pre-seed `SemaphoreStore("geo").mark_done(current)`, as the old
  code left `2026-10`. Run `geo.extract_month(current)`. Assert it was not
  skipped (`result["skipped"] is False`), that the GSE NDJSON file exists, and
  that `not SemaphoreStore("geo").exists(current)`.
- `test_extract_month_marks_a_settled_month_done`: `geo.extract_month("2020-07")`
  with empty stubs → `SemaphoreStore("geo").exists("2020-07")`. Calling it
  again returns `{"key": "2020-07", "skipped": True}`.
- `test_geo_source_keeps_unsettled_months_pending_despite_semaphores`: mark
  `2020-01`, `2020-02` and the current month done. Then
  `GeoSource(start_month="2020-01").list_partitions()` excludes
  `2020-01`/`2020-02`, includes `2020-03`, and includes the current month.

The existing `test_flows.py::test_real_sources_conform_to_protocol` constructs
`GeoSource()` with no arguments and must still pass unchanged.

## Departures from the design record

- `packages/omicidx-prefect/README.md` and `DUCKLAKE.md` say "the current month
  always re-runs" and offer `rerun_current_month=False`. The rule becomes "a
  month re-runs until it settles; a GEO semaphore means final", and the
  parameter is removed. Both docs are updated in this change.
- `README.md` says backfills are "delete the semaphores you want to redo, then
  re-run". That still holds. For GEO, a deleted semaphore on an unsettled
  month simply stays deleted until the month settles.

## Acceptance criteria

- [ ] `month_settles_at` and `MONTH_SETTLE` exist in `flows/geo.py` with the
      signatures above. `month_settles_at("2026-08")` is
      `2026-09-02T00:00:00+00:00`.
- [ ] `extract_month` never calls `mark_done` when its eutils search started
      before `month_settles_at(key)`, and it clears any existing semaphore for
      that key.
- [ ] `extract_month` skips a settled month that has a semaphore, unless
      `force`. It never skips an unsettled month.
- [ ] `GeoSource.list_partitions()` returns every unsettled month in range,
      even when it has a semaphore, using only the existing single LIST (no
      per-key reads).
- [ ] `rerun_current_month` no longer appears anywhere in the repo, except
      `RUN-LEDGER.md` history if it mentions it.
- [ ] The four new tests pass. `uv run pytest packages/omicidx-prefect/tests/`
      passes. `uv run ruff check packages/omicidx-prefect` and
      `uv run ruff format --check packages/omicidx-prefect` are clean.
- [ ] The README, DUCKLAKE.md and data-sources.qmd lines are updated as in §4.
- [ ] The PR body carries the operator repair below, verbatim. The implementer
      does **not** run it: it writes to R2.

## Operator repair (after merge, on onclappc02; not part of the implementation)

The new code does not repair the old `2026-08` semaphore on its own, because
August has settled and still has a semaphore. Re-extract it once under the GEO
lock (`systemd/README.md`, "never stack two extracts"):

```bash
cd /home/davsean/Documents/git/omicidx
systemd-run --user --unit=omicidx-geo-reextract-2026-08 -p WorkingDirectory=$PWD \
  -p EnvironmentFile=$PWD/.env /usr/bin/flock /run/user/$UID/omicidx-geo.lock \
  ~/.local/bin/uv run python -m omicidx.prefect.flows.geo run \
  --force --start-month 2026-08 --end-month 2026-08
```

Then the next `omicidx-downstream` run (or `omicidx-prefect run ducklake-load`
→ `transform` → `parquet-export` → `publish-bundle`) republishes. Verify on
`latest/`:

```sql
SELECT last_update_date::DATE d, count(*) FROM geo_series
WHERE last_update_date BETWEEN '2026-08-15' AND '2026-09-02' GROUP BY d ORDER BY d;
```

Every day from 2026-08-18 to 2026-08-31 should be non-zero. Run the same check
on `geo_samples`. The issue's spot-check accessions (`GSE331499`, `GSE331498`,
`GSE331022`, `GSE330599`, `GSE319645`) should be present. October self-repairs
when the month rolls over: November 1 re-extracts it and clears the stale
semaphore, and November 2 writes the final one.

## Test plan

```bash
uv run pytest packages/omicidx-prefect/tests/test_geo.py -v
uv run pytest packages/omicidx-prefect/tests/
uv run ruff check packages/omicidx-prefect
uv run ruff format --check packages/omicidx-prefect
grep -rn rerun_current_month --include='*.py' --include='*.md' --include='*.qmd' . | grep -v RUN-LEDGER
```

The last command must print nothing. No test touches the network or R2.

## Risks

- **Extra NCBI load at rollover.** The previous month is re-extracted on the
  1st and the 2nd, so two month-sized runs on those nights instead of one.
  Recent months are tens of thousands of accessions, at about 22 min per 62k.
  The unit's `TimeoutStartSec=86400` and `MAX_WORKERS = 1` are unaffected.
- **No semaphore for the current month.** `omicidx-prefect semaphores
  list/show geo` and `geo_audit.py` no longer show the in-progress month. This
  is intended: its counts were never final.
- **Indexing lag over one day** would still leave a small hole. Nothing
  observed suggests it, and `MONTH_SETTLE` is the single knob if it does.
- **Old holes in other months** are not detected by the new code. The live
  check above found none apart from 2026-08.
