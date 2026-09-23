"""A bounded repeat-check loop for sale events.

Scheduled runs drift badly (2-5 hours against a 30-minute cron), which is fine
for price history — it buckets by day — but useless during a sale, where stock
moves in minutes. This runs one job that checks on a fixed interval for a fixed
window.

Bounded on purpose. A self-chaining job would be using CI as always-on compute,
which the terms do not allow, and the penalty would be losing the repository
and every price point in it.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

from . import config

log = logging.getLogger("watchmon.loop")


@dataclass
class LoopResult:
    iterations: int = 0
    deals: int = 0
    failures: int = 0
    persisted: int = 0
    stopped_because: str = ""
    per_iteration: list[int] = field(default_factory=list)


def should_continue(elapsed: float, budget: float, next_wait: float) -> tuple[bool, str]:
    """Stop before the window closes, not after.

    Starting an iteration that cannot finish wastes the run and risks the job
    being killed mid-write, so the last slot is skipped rather than squeezed.
    """
    if elapsed >= budget:
        return False, "window closed"
    if elapsed + next_wait >= budget:
        return False, "no room for another iteration"
    return True, ""


def run_loop(
    check,
    persist=None,
    budget: float | None = None,
    interval: float | None = None,
    persist_every: int | None = None,
    clock=time.monotonic,
    sleep=time.sleep,
) -> LoopResult:
    """Call `check` every `interval` until `budget` is spent.

    `check` returns the number of deals found. Everything is injected so the
    tests can run a full window in milliseconds without sleeping.
    """
    budget = config.LOOP_MAX_SECONDS if budget is None else budget
    interval = config.LOOP_INTERVAL_SEC if interval is None else interval
    persist_every = config.LOOP_PERSIST_EVERY if persist_every is None else persist_every

    started = clock()
    result = LoopResult()

    while True:
        elapsed = clock() - started
        go, why = should_continue(elapsed, budget, interval)
        if not go:
            result.stopped_because = why
            break

        result.iterations += 1
        try:
            found = check() or 0
            result.deals += found
            result.per_iteration.append(found)
        except Exception as exc:  # noqa: BLE001 - one bad check must not end the window
            result.failures += 1
            log.warning("iteration %d failed: %s", result.iterations, exc)

        # Persist periodically: a cancelled window should cost minutes, not hours.
        if persist and result.iterations % persist_every == 0:
            try:
                persist()
                result.persisted += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("persist failed: %s", exc)

        log.info(
            "loop %d done (%d deal(s), %.0f min of %.0f used) — next in %.0fs",
            result.iterations, result.deals,
            (clock() - started) / 60, budget / 60, interval,
        )
        sleep(interval)

    # Always persist on the way out, whatever the exit reason.
    if persist:
        try:
            persist()
            result.persisted += 1
        except Exception as exc:  # noqa: BLE001
            log.warning("final persist failed: %s", exc)

    log.info(
        "loop finished: %d iteration(s), %d deal(s), %d failure(s) — %s",
        result.iterations, result.deals, result.failures, result.stopped_because,
    )
    return result
