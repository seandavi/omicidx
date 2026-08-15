# systemd units for omicidx's scheduled jobs

Source of truth for the units; the copies under `~/.config/systemd/user/` are
deployment artifacts. Convention (and the rationale for every field) lives in
`monode/infrastructure/SCHEDULING.md`; the reference implementation is
`cdsci-lake/systemd/`.

Since #158/#160 these timers are the **whole** scheduler. There is no Prefect
server, no worker container, and no deployment manifest — `prefect.yaml`,
`Dockerfile`, and `docker-compose.yml` were deleted with the excision.

| Unit | Runs | Cadence |
|---|---|---|
| `omicidx-sra-extract` | `python -m omicidx.prefect.flows.sra run` | daily 01:00 UTC (+≤30m jitter) |
| `omicidx-ebi-biosample-extract` | `python -m omicidx.prefect.flows.ebi_biosample run` | daily 02:00 UTC (+≤30m jitter) |
| `omicidx-biosample-extract` | `python -m omicidx.prefect.flows.biosample run` | daily 04:00 UTC (+≤30m jitter) |
| `omicidx-geo-extract` | `python -m omicidx.prefect.flows.geo run` | daily 06:00 UTC (+≤30m jitter) |
| `omicidx-pubmed-extract` | `python -m omicidx.prefect.flows.pubmed run` | hourly (+≤5m jitter) |
| `omicidx-downstream` | `python -m omicidx.prefect.flows.main run` | daily 05:00 UTC (+≤15m jitter) |
| `omicidx-ducklake-maintenance` | `omicidx-prefect run ducklake-maintenance` | Sunday 14:00 UTC (+≤30m jitter) |

`omicidx-biosample-extract` covers **both** NCBI full dumps (BioSample and
BioProject) in one unit: same machinery, two URLs, both unpartitioned. It and
`omicidx-sra-extract` are the two heavy jobs and `Conflicts=` each other — which
*stops* the loser rather than queueing it, hence the three-hour gap.

`omicidx-ebi-biosample-extract` declares no `Conflicts=` and sits between them:
it crawls the EBI BioSamples HTTP API rather than pulling an NCBI bulk file, so
it does not contend for the bandwidth the two heavy jobs fight over.

`omicidx-downstream` is the entire post-extraction chain in one unit — lake load
→ transform → parquet export → postgres load → publish bundle. One unit, not
five: the stages are strictly sequential and each consumes the previous one's
output, so splitting them would only buy `After=` ordering the single process
already has. It declares no `Conflicts=` with the extracts either — the load
reads raw files that are already complete, so an extract still running at 05:00
just means its newest partition lands in tomorrow's load. **Nothing downstream
waits on an extract any more**, which is the whole point of #149: a slow or
failing extract can no longer hold the publish hostage, which is exactly what
GEO did for a month.

## GEO: the unit exists, the timer is not installed yet

`omicidx-geo-extract.{service,timer}` are committed and proven, but **not**
installed on the host, and the table above describes what the timer *will* do.
The reason is #174, not a defect: **2020-07 through 2026-08 — 74 months — has
never been extracted.** 2020-07 was never a hang (#154 measured it: 62,419
accessions, ~44 req/s, 22 minutes, zero errors); it is simply the frontier of a
six-year backlog that stacked concurrent runs had throttled to ~1.2 req/s.

That backlog is ~27h of work, so it must be burned down in the foreground
first — on a timer it would hit `TimeoutStartSec` and page for five consecutive
nights while making partial progress:

```bash
cd /home/davsean/Documents/git/omicidx
uv run python -m omicidx.prefect.flows.geo run     # resumable, ^C-safe
```

Per-month semaphores make it resumable, so an interrupted run picks up where it
stopped. Once current, a nightly run is one month and finishes in minutes —
then install and enable the timer.

Do **not** raise GEO's concurrency to speed this up. `MAX_WORKERS = 1` is the
fix, not a placeholder: a month already fans 30-wide internally, and the
measured knee is 8-wide → 61 req/s versus 30-wide → 6.5 req/s with 10% timeouts.
More outer concurrency makes GEO slower.

## Scratch space

`omicidx-downstream` sets `TMPDIR=/data/davsean/tmp`. This is not optional:
`publish-bundle` re-materializes every exported table into a local DuckLake
before uploading, so it needs more free space than the entire snapshot — 63.4 GB
on 2026-08-15 and growing, against a 30 GB `/tmp`. Run the stage by hand and you
must export `TMPDIR` yourself, or it dies with `No space left on device` after
doing all the work.

## Recovery after a failed downstream run

Per stage, not per chain — every stage is idempotent, so re-running from an
earlier one is safe, just slower:

```bash
omicidx-prefect run ducklake-load
omicidx-prefect run transform
omicidx-prefect run parquet-export
omicidx-prefect run postgres
omicidx-prefect run publish-bundle
```

`omicidx-geo-extract` also declares no `Conflicts=`, for the same reason: it is
bandwidth-light (tens of thousands of small `acc.cgi` requests, <1 GB RSS). The
only thing GEO cannot tolerate is another copy of *itself* on the same NCBI
endpoint — that is exactly what wedged it (#154) — and `Type=oneshot` already
guarantees one instance.

`ntfy-notify@.service` and `notify-failure.sh` are **not** duplicated here — the
shared template installed from `cdsci-lake/systemd/` is what every
`OnFailure=ntfy-notify@%N.service` in this directory resolves to, and one topic
(`cdsci-lake-ops`) is the whole point. Its alert title says "cdsci-lake job
failed"; the failing unit name in the body is what identifies it.

## Install (not done by CI — a live-system change, make it deliberately)

```bash
for u in sra-extract ebi-biosample-extract biosample-extract pubmed-extract \
         downstream ducklake-maintenance; do
  cp systemd/omicidx-$u.{service,timer} ~/.config/systemd/user/
done
# once, shared, if not already present:
cp ~/Documents/git/cdsci-lake/systemd/ntfy-notify@.service ~/.config/systemd/user/
systemctl --user daemon-reload
for u in sra-extract ebi-biosample-extract biosample-extract pubmed-extract \
         downstream ducklake-maintenance; do
  systemctl --user enable --now omicidx-$u.timer
done
```

Note the absent `geo-extract`: it is **not** in the loop above on purpose. See
"GEO" below — install it only after the backlog is burned down (#174).

Verify (substitute the unit you just installed):

```bash
systemctl --user list-timers 'omicidx-*'
systemctl --user start omicidx-downstream.service   # one run by hand
journalctl --user -u omicidx-downstream.service -f
```

**Start each unit once by hand before trusting it.** `systemd-analyze verify`
only checks syntax; it passes a unit whose `ExecStart=` cannot be found at all.

## The environment these units read

`EnvironmentFile=` points every unit at the repo-root `.env`, and they run on
the **host**, not in a container. That killed the old host/container split:
`POSTGRES_URI` used to say `@pg_main:5432` (a name that only resolves inside
the Docker network) because the worker read the same file from inside it.
Both `POSTGRES_URI` and `DUCKLAKE_URI` now point at `127.0.0.1:5432`, the port
`pg_main` publishes. There is no second value to keep in sync.

Extraction leaves no `lake_ops.run` row by design (#149): it writes raw files to
R2 and adds no DuckLake snapshot, so the ledger is the per-partition semaphores
(`omicidx-prefect semaphores list sra/study`, `... list biosample`), journald is
the narrative, and ntfy is the failure signal. `ops.run` stays on the load side —
i.e. on `omicidx-downstream`.

Snapshots written by the downstream chain carry `run_id` in their
`commit_extra_info`, and on a scheduled run that id is systemd's
`INVOCATION_ID` — so `journalctl _SYSTEMD_INVOCATION_ID=<id>` pulls up the log
for any snapshot you find in the lake. (Snapshots written before #158 carry
`prefect_run_id` instead; those ids are gone with the server.)

## GEO: expect a backfill before it settles

`omicidx-geo-extract` has 74 pending months when first installed — GEO has
never gotten past `2020-07`. Each month is ~22 min, so the 6h `TimeoutStartSec`
clears roughly 16 months a night and the run *will* hit that timeout (and fire
an ntfy alert) for the first four or five nights. That is expected: semaphores
make the next night resume where it stopped. Watch it drain with

```bash
omicidx-prefect semaphores list geo | head -1
```

To burn the backfill down faster, run it in the foreground with no deadline:

```bash
uv run python -m omicidx.prefect.flows.geo run   # resumable; ^C is safe
```

Copy, don't symlink: symlinked units go dangling when a repo moves, and systemd
then fails to load them silently (this is exactly how `omicidx-sql-runner` died).
