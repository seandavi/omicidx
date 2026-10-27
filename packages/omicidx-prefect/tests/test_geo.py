"""Guards for the GEO EL process (#154).

The 2020-07 "hang" was throughput collapse under stacked concurrent runs, not
a code fault — but the reason nobody could see that is that every per-accession
failure vanished into `return_exceptions=True` and a month that ground for 20
hours looked identical to a month that was working. These two tests cover the
guards that make both loud.
"""

import asyncio

import pytest


def _stub_env(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    from omicidx.prefect import config

    config.settings.cache_clear()


def test_fetch_and_parse_counts_failures(monkeypatch, tmp_path):
    """Per-accession failures are counted and returned, not swallowed."""
    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.flows import geo

    async def fake_fetch(acc, client):
        if acc.endswith("3"):
            raise RuntimeError("boom")
        return f"^SERIES = {acc}\n!Series_title = t\n"

    monkeypatch.setattr(geo, "_fetch_soft", fake_fetch)
    accessions = [f"GSE{i}" for i in range(10)]

    _, failed = asyncio.run(geo._fetch_and_parse(accessions, concurrency=2))

    assert failed == 1  # GSE3


def test_extract_month_refuses_to_mark_a_holed_month_done(monkeypatch, tmp_path):
    """Too many fetch failures -> raise, and leave no semaphore behind."""
    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.flows import geo
    from omicidx.prefect.semaphore import SemaphoreStore

    accessions = [f"GSE{i}" for i in range(100)]
    monkeypatch.setattr(
        geo, "_fetch_accessions", lambda start, end: _completed(accessions)
    )
    monkeypatch.setattr(
        geo,
        "_fetch_and_parse",
        lambda accs, **kw: _completed(({"GSE": [], "GSM": [], "GPL": []}, 50)),
    )

    with pytest.raises(RuntimeError, match="refusing to mark the month done"):
        geo.extract_month("2020-07")

    assert not SemaphoreStore("geo").exists("2020-07")


async def _completed(value):
    """`extract_month` calls these under `asyncio.run`, so stubs stay awaitable."""
    return value


def test_month_deadline_scales_with_accession_count():
    """A fixed wall clock is only correct for one month size (#174)."""
    from omicidx.prefect.flows.geo import (
        MIN_FETCH_RATE,
        MONTH_TIMEOUT_FLOOR_SECONDS,
        month_deadline,
    )

    # Ordinary months stay on the floor — the guard is unchanged for them.
    assert month_deadline(46_106) == MONTH_TIMEOUT_FLOOR_SECONDS
    assert month_deadline(62_419) == MONTH_TIMEOUT_FLOOR_SECONDS

    # 2019-05 is real: 732,475 accessions, 16x its neighbours. At the old fixed
    # 2h it could not finish, and being first in the pending list it blocked the
    # whole backfill.
    monster = month_deadline(732_475)
    assert monster > MONTH_TIMEOUT_FLOOR_SECONDS
    assert monster == 732_475 / MIN_FETCH_RATE
    # Comfortably past the ~4.6h the month needs at measured throughput.
    assert monster > 4.6 * 3600


def test_readerror_is_retryable():
    """ReadError is a NetworkError sibling of ConnectError, not a timeout (#174).

    Naming only ConnectError excluded it, and it is how NCBI drops a request
    under load: 530 of 534 failures across a 6M-record backfill were ReadError,
    none retried.
    """
    import httpx
    from omicidx.prefect.flows.geo import _is_retryable

    req = httpx.Request("GET", "https://example.org")
    assert _is_retryable(httpx.ReadError("socket died", request=req))
    assert _is_retryable(httpx.ConnectError("refused", request=req))
    assert _is_retryable(httpx.ReadTimeout("slow", request=req))
    assert _is_retryable(httpx.RemoteProtocolError("bad frame", request=req))

    # 4xx never heals, so it must still not be retried.
    resp = httpx.Response(404, request=req)
    assert not _is_retryable(httpx.HTTPStatusError("nf", request=req, response=resp))
    assert _is_retryable(
        httpx.HTTPStatusError(
            "rate", request=req, response=httpx.Response(429, request=req)
        )
    )


def test_month_settles_at_is_one_day_after_month_end():
    from datetime import UTC, datetime

    from omicidx.prefect.flows.geo import month_settles_at

    assert month_settles_at("2026-08") == datetime(2026, 9, 2, tzinfo=UTC)
    assert month_settles_at("2026-12") == datetime(2027, 1, 2, tzinfo=UTC)


def test_extract_month_writes_no_semaphore_for_an_unsettled_month(
    monkeypatch, tmp_path
):
    """#237: a semaphore from older code must not outlive an unsettled extract."""
    _stub_env(monkeypatch, tmp_path)
    from datetime import date

    from omicidx.prefect.flows import geo
    from omicidx.prefect.semaphore import SemaphoreStore

    monkeypatch.setattr(geo, "_fetch_accessions", lambda start, end: _completed([]))
    current = geo._month_key(date.today())
    SemaphoreStore("geo").mark_done(current)

    result = geo.extract_month(current)

    assert result["skipped"] is False
    assert list(tmp_path.rglob("gse/**/data_0.ndjson.gz"))
    assert not SemaphoreStore("geo").exists(current)


def test_extract_month_marks_a_settled_month_done(monkeypatch, tmp_path):
    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.flows import geo
    from omicidx.prefect.semaphore import SemaphoreStore

    monkeypatch.setattr(geo, "_fetch_accessions", lambda start, end: _completed([]))

    geo.extract_month("2020-07")

    assert SemaphoreStore("geo").exists("2020-07")
    assert geo.extract_month("2020-07") == {"key": "2020-07", "skipped": True}


def test_geo_source_keeps_unsettled_months_pending_despite_semaphores(
    monkeypatch, tmp_path
):
    _stub_env(monkeypatch, tmp_path)
    from datetime import date

    from omicidx.prefect.flows.geo import GeoSource, _month_key
    from omicidx.prefect.semaphore import SemaphoreStore

    current = _month_key(date.today())
    for key in ("2020-01", "2020-02", current):
        SemaphoreStore("geo").mark_done(key)

    pending = GeoSource(start_month="2020-01").list_partitions()

    assert "2020-01" not in pending
    assert "2020-02" not in pending
    assert "2020-03" in pending
    assert current in pending


def _pin_now(monkeypatch, geo, instant):
    """Pin geo's `datetime.now()`; everything else on datetime is untouched."""
    from datetime import datetime

    class Pinned(datetime):
        @classmethod
        def now(cls, tz=None):
            return instant

    monkeypatch.setattr(geo, "datetime", Pinned)


def test_previous_month_stays_unsettled_until_the_2nd(monkeypatch, tmp_path):
    """#237 itself: on Sep 1 August is still pending and gets no semaphore."""
    _stub_env(monkeypatch, tmp_path)
    from datetime import UTC, datetime

    from omicidx.prefect.flows import geo
    from omicidx.prefect.semaphore import SemaphoreStore

    monkeypatch.setattr(geo, "_fetch_accessions", lambda start, end: _completed([]))
    source = geo.GeoSource(start_month="2026-08", end_month="2026-09")
    sem = SemaphoreStore("geo")

    _pin_now(monkeypatch, geo, datetime(2026, 9, 1, 6, tzinfo=UTC))
    sem.mark_done("2026-08")  # the semaphore the old code wrote mid-month
    assert "2026-08" in source.list_partitions()
    geo.extract_month("2026-08")
    assert not sem.exists("2026-08")

    _pin_now(monkeypatch, geo, datetime(2026, 9, 2, 6, tzinfo=UTC))
    geo.extract_month("2026-08")
    assert sem.exists("2026-08")
    assert "2026-08" not in source.list_partitions()


def test_failed_unsettled_extract_leaves_no_semaphore(monkeypatch, tmp_path):
    """#238: the stale semaphore goes even when the run raises."""
    _stub_env(monkeypatch, tmp_path)
    from datetime import UTC, datetime

    from omicidx.prefect.flows import geo
    from omicidx.prefect.semaphore import SemaphoreStore

    async def boom(start, end):
        raise RuntimeError("eutils down")

    monkeypatch.setattr(geo, "_fetch_accessions", boom)
    _pin_now(monkeypatch, geo, datetime(2026, 10, 27, tzinfo=UTC))
    SemaphoreStore("geo").mark_done("2026-10")

    with pytest.raises(RuntimeError):
        geo.extract_month("2026-10")

    assert not SemaphoreStore("geo").exists("2026-10")
