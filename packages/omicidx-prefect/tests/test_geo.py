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
