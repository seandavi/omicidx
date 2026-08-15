"""Smoke tests: every flow module imports cleanly and registers its flows.

A failure here means the worker would fail to load the deployment.
"""

import importlib

FLOW_MODULES = [
    "omicidx.prefect.flows.sra",
    "omicidx.prefect.flows.geo",
    "omicidx.prefect.flows.biosample",
    "omicidx.prefect.flows.pubmed",
    "omicidx.prefect.flows.ebi_biosample",
    "omicidx.prefect.flows.consolidate",
    "omicidx.prefect.flows.ducklake",
    "omicidx.prefect.flows.ducklake_biosample",
    "omicidx.prefect.flows.ducklake_geo",
    "omicidx.prefect.flows.ducklake_sra",
    "omicidx.prefect.flows.ducklake_pubmed",
    "omicidx.prefect.flows.ducklake_ebi_biosample",
    "omicidx.prefect.flows.ducklake_sra_accessions",
    "omicidx.prefect.flows.ducklake_geo_rnaseq_counts",
    "omicidx.prefect.flows.ducklake_load",
    "omicidx.prefect.flows.postgres",
    "omicidx.prefect.flows.sql",
    "omicidx.prefect.flows.main",
]


def test_flow_modules_import(monkeypatch):
    # Settings() reads env vars at import time via the config helpers, but
    # only when actually called. Stub the required vars so any module-level
    # config access works on import.
    monkeypatch.setenv("PUBLISH_ROOT", "s3://test-bucket")
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    for name in FLOW_MODULES:
        mod = importlib.import_module(name)
        assert mod is not None


def test_semaphore_namespace_validation():
    import pytest
    from omicidx.prefect.semaphore import SemaphoreStore

    with pytest.raises(ValueError):
        SemaphoreStore("")
    with pytest.raises(ValueError):
        SemaphoreStore("///")

    store = SemaphoreStore("sra/study")
    assert store.namespace == "sra/study"


def test_semaphore_key_validation(monkeypatch, tmp_path):
    """Keys must not contain slashes."""
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    # Bust the lru_cache so the test env sticks
    from omicidx.prefect import config

    config.settings.cache_clear()

    import pytest
    from omicidx.prefect.semaphore import SemaphoreStore

    store = SemaphoreStore("test")
    with pytest.raises(ValueError):
        store.exists("bad/key")
    with pytest.raises(ValueError):
        store.exists("")


def test_semaphore_roundtrip(monkeypatch, tmp_path):
    """Write a semaphore, read it back, list, then clear."""
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    from omicidx.prefect import config

    config.settings.cache_clear()

    from omicidx.prefect.semaphore import SemaphoreStore

    store = SemaphoreStore("sra/study")
    assert not store.exists("2024-09-13_Full")
    store.mark_done("2024-09-13_Full", metadata={"row_count": 42})
    assert store.exists("2024-09-13_Full")

    payload = store.read("2024-09-13_Full")
    assert payload["namespace"] == "sra/study"
    assert payload["key"] == "2024-09-13_Full"
    assert payload["metadata"]["row_count"] == 42
    assert "completed_at" in payload

    assert store.list_keys() == ["2024-09-13_Full"]
    assert store.clear("2024-09-13_Full") is True
    assert not store.exists("2024-09-13_Full")
    assert store.clear("2024-09-13_Full") is False


def _stub_env(monkeypatch, tmp_path):
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    from omicidx.prefect import config

    config.settings.cache_clear()


def test_real_sources_conform_to_protocol(monkeypatch, tmp_path):
    """Every extract flow exposes a Source: name + list_partitions + extract."""
    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.flows.biosample import BioprojectSource, BiosampleSource
    from omicidx.prefect.flows.ebi_biosample import EbiBiosampleSource
    from omicidx.prefect.flows.geo import GeoSource
    from omicidx.prefect.flows.pubmed import PubmedSource
    from omicidx.prefect.flows.sra import SraSource
    from omicidx.prefect.source import Source

    for cls in (
        SraSource,
        GeoSource,
        PubmedSource,
        EbiBiosampleSource,
        BiosampleSource,
        BioprojectSource,
    ):
        s = cls()
        assert isinstance(s, Source), cls.__name__
        assert isinstance(s.name, str) and s.name
        assert callable(s.extract), f"{cls.__name__}.extract is not callable"


class _FakeSource:
    """Minimal Source: 2 pending keys, 3 when forced."""

    name = "fake"

    def __init__(self, extract):
        self.extract = extract

    def list_partitions(self, force: bool = False) -> list[str]:
        return ["a", "b", "c"] if force else ["a", "b"]


def test_run_extraction_drives_every_key_with_force(monkeypatch, tmp_path):
    """The generic driver lists pending keys and extracts each, threading force."""
    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.source import Source, run_extraction

    calls: list[tuple[str, bool]] = []

    def fake_extract(key: str, force: bool = False) -> dict:
        calls.append((key, force))
        return {"key": key, "skipped": False}

    source = _FakeSource(fake_extract)
    assert isinstance(source, Source)

    results = run_extraction(source, force=False)
    assert sorted(c[0] for c in calls) == ["a", "b"]
    assert all(c[1] is False for c in calls)
    # results come back in key order, not completion order
    assert [r["key"] for r in results] == ["a", "b"]

    calls.clear()
    run_extraction(source, force=True)
    # force reaches both list_partitions (extra key "c") and each extract
    assert sorted(c[0] for c in calls) == ["a", "b", "c"]
    assert all(c[1] is True for c in calls)


def test_run_extraction_fanout_is_bounded_by_max_workers(monkeypatch, tmp_path):
    """No more than max_workers extracts are ever in flight — and more than one is."""
    import threading

    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.source import run_extraction

    lock = threading.Lock()
    state = {"in_flight": 0, "peak": 0}
    # Every worker must meet 2 others at the barrier; if the pool were serial (or
    # narrower than 2) this times out and the test fails loudly rather than flakily.
    barrier = threading.Barrier(2, timeout=10)

    def slow_extract(key: str, force: bool = False) -> dict:
        with lock:
            state["in_flight"] += 1
            state["peak"] = max(state["peak"], state["in_flight"])
        barrier.wait()
        with lock:
            state["in_flight"] -= 1
        return {"key": key}

    class Source6(_FakeSource):
        def list_partitions(self, force: bool = False) -> list[str]:
            return [f"k{i}" for i in range(6)]

    results = run_extraction(Source6(slow_extract), max_workers=2)
    assert len(results) == 6
    assert state["peak"] == 2


def test_run_extraction_retries_then_propagates(monkeypatch, tmp_path):
    """A partition is retried; a partition that keeps failing fails the whole run."""
    import pytest

    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect import source as source_mod
    from omicidx.prefect.source import run_extraction

    monkeypatch.setattr(source_mod, "RETRY_WAIT_SECONDS", 0)
    attempts: dict[str, int] = {}

    def flaky(key: str, force: bool = False) -> dict:
        attempts[key] = attempts.get(key, 0) + 1
        if key == "a" and attempts[key] < 2:
            raise RuntimeError("transient")
        if key == "c":
            raise RuntimeError("permanent")
        return {"key": key}

    # "a" recovers on its retry, "b" is untouched, so force=False (keys a, b) passes.
    assert len(run_extraction(_FakeSource(flaky), max_workers=2)) == 2
    assert attempts["a"] == 2

    # "c" never recovers: exhausts RETRY_ATTEMPTS, then the driver re-raises.
    with pytest.raises(RuntimeError, match="permanent"):
        run_extraction(_FakeSource(flaky), force=True, max_workers=2)
    assert attempts["c"] == source_mod.RETRY_ATTEMPTS


def test_semaphore_pending_keys(monkeypatch, tmp_path):
    """pending_keys filters out done partitions via a single list_keys()."""
    monkeypatch.setenv("PUBLISH_ROOT", str(tmp_path))
    monkeypatch.setenv("S3_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("S3_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("S3_ENDPOINT", "https://example.r2.cloudflarestorage.com")
    monkeypatch.setenv("S3_REGION", "auto")
    from omicidx.prefect import config

    config.settings.cache_clear()

    from omicidx.prefect.semaphore import SemaphoreStore

    store = SemaphoreStore("geo")
    store.mark_done("2005-01")
    store.mark_done("2005-02")
    candidates = ["2005-01", "2005-02", "2005-03"]

    # done keys dropped, order preserved
    assert store.pending_keys(candidates) == ["2005-03"]
    # `always` re-includes a done key (mutable "latest")
    assert store.pending_keys(candidates, always=["2005-02"]) == ["2005-02", "2005-03"]
    # force keeps everything
    assert store.pending_keys(candidates, force=True) == candidates
