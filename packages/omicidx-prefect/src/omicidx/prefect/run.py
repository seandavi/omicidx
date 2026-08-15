"""Run identity and retry policy for the orchestrator-free pipeline (#158).

Two things Prefect used to supply, replaced with ~10 lines of stdlib +
tenacity when the last flow lost its decorators:

``run_id()``   was ``prefect.runtime.flow_run.get_id()`` — the id stamped into
               DuckLake snapshot ``commit_extra_info`` and cdsci-lake
               ``ops.run`` rows so a lake snapshot can be traced back to the
               process that wrote it.
``retry``      was ``@task(retries=1, retry_delay_seconds=60)`` — one retry,
               60s apart, around a load step that can lose a connection to R2
               or Postgres mid-run.

Extraction has its own (wider) policy in ``source.py``: a partition crawl is
retried 3x because a flaky NCBI mirror is routine, whereas a load step failing
twice in a row is a real fault worth paging on.
"""

import logging
import os
import uuid

import tenacity

log = logging.getLogger(__name__)

#: systemd sets INVOCATION_ID per service start, so a scheduled run's id is the
#: same one journald indexes: `journalctl _SYSTEMD_INVOCATION_ID=<id>` pulls up
#: the log for any snapshot you find in the lake. Ad-hoc runs get a uuid.
_RUN_ID = os.environ.get("INVOCATION_ID") or uuid.uuid4().hex


def run_id() -> str:
    """Stable id for this process, for lake snapshot / ops.run attribution."""
    return _RUN_ID


#: Retry one load step. Matches what `@task(retries=1, retry_delay_seconds=60)`
#: gave the stages before the excision: 2 attempts total, 60s apart.
retry = tenacity.retry(
    stop=tenacity.stop_after_attempt(2),
    wait=tenacity.wait_fixed(60),
    before_sleep=tenacity.before_sleep_log(log, logging.WARNING),
    reraise=True,
)
