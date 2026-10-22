"""SRA raw retention (#194): keep the last two complete Fulls and what follows."""

import pytest
from omicidx.prefect.flows.sra import RawPartition, select_raw_prune


def P(date, stage="Incremental", done=True):
    return RawPartition(date, stage, done)


def test_keeps_two_complete_fulls_and_later_incrementals():
    parts = [
        P("2026-05-01", "Full", done=False),  # pre-semaphore, older than both
        P("2026-05-02", done=False),
        P("2026-08-01", "Full"),
        P("2026-08-02"),
        P("2026-09-01", "Full", done=False),  # dead partial, between kept Fulls
        P("2026-09-02"),
        P("2026-10-01", "Full"),
        P("2026-10-02"),
        P("2026-11-01", "Full", done=False),  # in progress: never counts
    ]
    cutoff, doomed = select_raw_prune(parts)
    assert cutoff == "2026-08-01"
    assert [(p.date, p.stage) for p in doomed] == [
        ("2026-05-01", "Full"),
        ("2026-05-02", "Incremental"),
    ]


def test_one_complete_full_keeps_it():
    cutoff, doomed = select_raw_prune([P("2026-09-30"), P("2026-10-01", "Full")])
    assert cutoff == "2026-10-01"
    assert doomed == [P("2026-09-30")]


def test_refuses_without_a_complete_full():
    with pytest.raises(ValueError):
        select_raw_prune([P("2026-10-01", "Full", done=False), P("2026-10-02")])
