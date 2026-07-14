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
        assert hasattr(s.extract, "submit"), f"{cls.__name__}.extract is not a task"


def test_run_extraction_drives_every_key_with_force(monkeypatch, tmp_path):
    """The generic driver lists pending keys and extracts each, threading force."""
    _stub_env(monkeypatch, tmp_path)
    from omicidx.prefect.source import Source, run_extraction
    from prefect import flow, task
    from prefect.testing.utilities import prefect_test_harness

    calls: list[tuple[str, bool]] = []

    @task
    def fake_extract(key: str, force: bool = False) -> dict:
        calls.append((key, force))
        return {"key": key, "skipped": False}

    class FakeSource:
        name = "fake"
        extract = staticmethod(fake_extract)

        def list_partitions(self, force: bool = False) -> list[str]:
            return ["a", "b", "c"] if force else ["a", "b"]

    assert isinstance(FakeSource(), Source)

    @flow
    def drive(force: bool = False) -> list[dict]:
        return run_extraction(FakeSource(), force=force)

    with prefect_test_harness():
        results = drive(force=False)
        assert sorted(c[0] for c in calls) == ["a", "b"]
        assert all(c[1] is False for c in calls)
        assert len(results) == 2

        calls.clear()
        drive(force=True)
        # force reaches both list_partitions (extra key "c") and each extract
        assert sorted(c[0] for c in calls) == ["a", "b", "c"]
        assert all(c[1] is True for c in calls)


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
