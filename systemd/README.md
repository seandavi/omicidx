# systemd units for omicidx's scheduled EL processes

Source of truth for the units; the copies under `~/.config/systemd/user/` are
deployment artifacts. Convention (and the rationale for every field) lives in
`monode/infrastructure/SCHEDULING.md`; the reference implementation is
`cdsci-lake/systemd/`.

| Unit | Runs | Cadence |
|---|---|---|
| `omicidx-sra-extract` | `python -m omicidx.prefect.flows.sra run` | daily 01:00 UTC (+≤30m jitter) |
| `omicidx-pubmed-extract` | `python -m omicidx.prefect.flows.pubmed run` | hourly (+≤5m jitter) |

`ntfy-notify@.service` and `notify-failure.sh` are **not** duplicated here — the
shared template installed from `cdsci-lake/systemd/` is what every
`OnFailure=ntfy-notify@%N.service` in this directory resolves to, and one topic
(`cdsci-lake-ops`) is the whole point. Its alert title says "cdsci-lake job
failed"; the failing unit name in the body is what identifies it.

## Install (not done by CI — a live-system change, make it deliberately)

```bash
cp systemd/omicidx-sra-extract.{service,timer} ~/.config/systemd/user/
cp systemd/omicidx-pubmed-extract.{service,timer} ~/.config/systemd/user/
# once, shared, if not already present:
cp ~/Documents/git/cdsci-lake/systemd/ntfy-notify@.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now omicidx-sra-extract.timer
systemctl --user enable --now omicidx-pubmed-extract.timer
```

Verify (substitute the unit you just installed):

```bash
systemctl --user list-timers omicidx-sra-extract.timer
systemctl --user start omicidx-sra-extract.service   # one run by hand
journalctl --user -u omicidx-sra-extract.service -f
```

### PubMed only: retire the Prefect schedule in the same change

PubMed is the one domain that had a live standalone Prefect deployment
(`pubmed-extract`, `interval: 3600`). Deleting the block from `prefect.yaml`
does **not** unregister it — the schedule stays active in the API (learned the
hard way in #145). Enable the timer and delete the schedule together, or PubMed
extracts hourly twice:

```bash
cd packages/omicidx-prefect
docker compose exec worker python - <<'PY'
import asyncio
from prefect.client.orchestration import get_client

async def main():
    async with get_client() as client:
        dep = await client.read_deployment_by_name("pubmed-extract/pubmed-extract")
        for s in dep.schedules:
            await client.delete_deployment_schedule(dep.id, s.id)
        print(f"deleted {len(dep.schedules)} schedule(s) from {dep.id}")
        await client.delete_deployment(dep.id)   # the flow is no longer a @flow
asyncio.run(main())
PY
```

Confirm with `prefect deployment ls` — no `pubmed-extract`, and
`systemctl --user list-timers omicidx-pubmed-extract.timer` shows the next fire.

Extraction leaves no `lake_ops.run` row by design (#149): it writes raw files to
R2 and adds no DuckLake snapshot, so the ledger is the per-partition semaphores
(`omicidx-prefect semaphores list sra/study`), journald is the narrative, and
ntfy is the failure signal. `ops.run` stays on the load side.

Copy, don't symlink: symlinked units go dangling when a repo moves, and systemd
then fails to load them silently (this is exactly how `omicidx-sql-runner` died).
