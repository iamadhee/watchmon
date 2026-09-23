#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["pytest>=8.0"]
# ///
"""The bounded sale-event loop.

Time is injected, so a 5.5-hour window runs in milliseconds. The behaviours
that matter are all failure-shaped: the window must close on time, a bad check
must not end it, and a cancelled loop must not cost the whole window of data.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from watchmon import config  # noqa: E402
from watchmon.loop import LoopResult, run_loop, should_continue  # noqa: E402


class FakeClock:
    """Advances only when the loop sleeps — no real time passes."""

    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def counting_check(per_call=0, fail_on=()):
    calls = {"n": 0}

    def check():
        calls["n"] += 1
        if calls["n"] in fail_on:
            raise RuntimeError("scrape blew up")
        return per_call

    check.calls = calls
    return check


# ------------------------------------------------------------- the window ---


def test_loop_fills_its_window():
    clock = FakeClock()
    check = counting_check()
    result = run_loop(check, budget=3600, interval=600, clock=clock, sleep=clock.sleep)
    # 3600 / 600 = 6 slots, minus the last which has no room to finish
    assert result.iterations == 5
    assert result.stopped_because == "no room for another iteration"


def test_loop_never_overruns_the_budget():
    """The job is killed at 6h; a loop that overruns loses the final write."""
    clock = FakeClock()
    run_loop(counting_check(), budget=1000, interval=300, clock=clock, sleep=clock.sleep)
    assert clock.now <= 1000


def test_a_single_iteration_window_still_runs_once():
    clock = FakeClock()
    result = run_loop(counting_check(), budget=700, interval=600, clock=clock, sleep=clock.sleep)
    assert result.iterations == 1


def test_zero_budget_does_nothing():
    clock = FakeClock()
    result = run_loop(counting_check(), budget=0, interval=600, clock=clock, sleep=clock.sleep)
    assert result.iterations == 0 and result.stopped_because == "window closed"


def test_should_continue_skips_a_slot_that_cannot_finish():
    assert should_continue(0, 3600, 600)[0] is True
    assert should_continue(3000, 3600, 600)[0] is False   # would end exactly at the edge
    assert should_continue(3600, 3600, 600) == (False, "window closed")


# ------------------------------------------------------------- resilience ---


def test_one_failed_check_does_not_end_the_window():
    """A single timeout mid-sale must not cost the remaining hours."""
    clock = FakeClock()
    check = counting_check(fail_on=(2,))
    result = run_loop(check, budget=3600, interval=600, clock=clock, sleep=clock.sleep)
    assert result.iterations == 5
    assert result.failures == 1


def test_every_check_failing_still_completes_cleanly():
    clock = FakeClock()
    check = counting_check(fail_on=tuple(range(1, 99)))
    result = run_loop(check, budget=1800, interval=600, clock=clock, sleep=clock.sleep)
    assert result.failures == result.iterations > 0


def test_deals_are_totalled_across_iterations():
    clock = FakeClock()
    result = run_loop(counting_check(per_call=2), budget=3600, interval=600,
                      clock=clock, sleep=clock.sleep)
    assert result.deals == 10
    assert result.per_iteration == [2, 2, 2, 2, 2]


# -------------------------------------------------------------- persisting --


def test_state_is_persisted_periodically_not_only_at_the_end():
    """A cancelled 5-hour window should cost minutes of data, not the lot."""
    clock = FakeClock()
    saves = []
    run_loop(counting_check(), persist=lambda: saves.append(clock.now),
             budget=3600, interval=600, persist_every=2, clock=clock, sleep=clock.sleep)
    assert len(saves) >= 3            # two periodic, plus the final one
    assert saves[0] < 3600            # the first landed mid-window


def test_final_persist_happens_even_with_no_iterations():
    clock = FakeClock()
    saves = []
    run_loop(counting_check(), persist=lambda: saves.append(1), budget=0,
             interval=600, clock=clock, sleep=clock.sleep)
    assert len(saves) == 1


def test_a_failing_persist_does_not_end_the_window():
    clock = FakeClock()

    def bad_persist():
        raise OSError("disk gone")

    result = run_loop(counting_check(), persist=bad_persist, budget=3600, interval=600,
                      persist_every=1, clock=clock, sleep=clock.sleep)
    assert result.iterations == 5
    assert result.persisted == 0


# ------------------------------------------------------------------ config --


def test_defaults_stay_under_the_six_hour_job_cap():
    """A hosted runner kills the job at 6h; overrunning loses the final write."""
    assert config.LOOP_MAX_SECONDS < 6 * 3600
    assert config.LOOP_INTERVAL_SEC > 0


def test_defaults_are_used_when_unspecified(monkeypatch):
    monkeypatch.setattr(config, "LOOP_MAX_SECONDS", 1800)
    monkeypatch.setattr(config, "LOOP_INTERVAL_SEC", 600)
    monkeypatch.setattr(config, "LOOP_PERSIST_EVERY", 1)
    clock = FakeClock()
    result = run_loop(counting_check(), clock=clock, sleep=clock.sleep)
    assert result.iterations == 2


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
