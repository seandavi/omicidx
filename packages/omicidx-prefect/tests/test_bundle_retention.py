"""Public bundle retention (#258): 7 newest dailies + newest per ISO week x5."""

from datetime import date, timedelta

from omicidx.prefect.flows.parquet_export import select_dated_keep


def _days(start: str, n: int) -> list[str]:
    d0 = date.fromisoformat(start)
    return [(d0 + timedelta(days=i)).isoformat() for i in range(n)]


def test_keeps_seven_dailies_and_five_weeklies():
    dates = _days("2026-09-01", 61)  # Sep 1 .. Oct 31, every night
    keep = select_dated_keep(dates, "2026-10-31", daily=7, weekly=5)
    # Dailies: Oct 25..31. Weeklies: newest per ISO week, 5 newest weeks.
    # Oct 31 is a Saturday, so the current week's newest is Oct 31 itself and
    # the four before it end on Sundays Oct 25, 18, 11, 4.
    assert keep == set(_days("2026-10-25", 7)) | {
        "2026-10-18",
        "2026-10-11",
        "2026-10-04",
    }


def test_a_failed_sunday_falls_back_to_that_weeks_newest_build():
    dates = [d for d in _days("2026-10-01", 31) if d != "2026-10-18"]
    keep = select_dated_keep(dates, "2026-10-31", daily=7, weekly=5)
    assert "2026-10-17" in keep and "2026-10-18" not in keep


def test_counts_not_ages_and_today_always_kept():
    # Only three old folders survive a long outage; none is pruned for age.
    assert select_dated_keep(
        ["2026-01-05", "2026-01-12", "2026-01-19"], "2026-10-31", daily=7, weekly=5
    ) == {"2026-01-05", "2026-01-12", "2026-01-19", "2026-10-31"}
    assert select_dated_keep(["2026-10-30"], "2026-10-31", daily=0, weekly=0) == {
        "2026-10-31"
    }
