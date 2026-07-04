"""Concurrency tests for ``try_claim_atomic_service_run`` (Mongo backend).

Mirrors core orchestrator semantics against MongoDB persistence.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from pynenc.orchestrator.atomic_service import (
    AtomicServiceExecutionStatus,
    AtomicServiceRun,
)
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from pynenc import Pynenc


def _make_runner_ctx(runner_id: str) -> RunnerContext:
    return RunnerContext(
        runner_cls="TestRunner",
        runner_id=runner_id,
        pid=12345,
        hostname="test-host",
    )


def _finalize(app_instance: Pynenc, run: AtomicServiceRun) -> None:
    app_instance.orchestrator.finalize_atomic_service_execution(
        run,
        datetime.now(UTC),
        AtomicServiceExecutionStatus.COMPLETED,
    )


def test_concurrent_claims_in_same_slot_admit_only_the_assigned_runner(
    app_instance: Pynenc,
) -> None:
    """Under real scheduling, a single slot admits only its assigned runner."""
    runner_ctxs = [_make_runner_ctx(f"runner-{i:02d}") for i in range(8)]
    for ctx in runner_ctxs:
        app_instance.orchestrator.register_runner_heartbeats(
            [ctx.runner_id], can_run_atomic_service=True
        )

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            with ThreadPoolExecutor(max_workers=len(runner_ctxs)) as pool:
                futures = [
                    pool.submit(
                        app_instance.orchestrator.try_claim_atomic_service_run,
                        ctx,
                    )
                    for ctx in runner_ctxs
                ]
                results = [f.result() for f in as_completed(futures)]

        winners = [r for r in results if r is not None]
        assert len(winners) == 1, (
            f"expected exactly one assigned winner, got {len(winners)}: "
            f"{[w.runner_id for w in winners]}"
        )

        active = app_instance.orchestrator.get_active_atomic_service_executions()
        assert len(active) == 1
        assert active[0].atomic_service_run_id == winners[0].atomic_service_run_id

        for winner in winners:
            _finalize(app_instance, winner)
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )


def test_repeated_claims_across_cycles_never_overlap(
    app_instance: Pynenc,
) -> None:
    """Across many cycles, no two ``RUNNING`` records exist at the same time."""
    runner_ctxs = [_make_runner_ctx(f"runner-{i:02d}") for i in range(4)]
    for ctx in runner_ctxs:
        app_instance.orchestrator.register_runner_heartbeats(
            [ctx.runner_id], can_run_atomic_service=True
        )

    cycles = 10
    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        winners_total = 0
        slot_seconds = 60.0 * 60.0 / len(runner_ctxs)
        for cycle in range(cycles):
            t = cycle * slot_seconds
            with patch("pynenc.orchestrator.base_orchestrator.time", return_value=t):
                with ThreadPoolExecutor(max_workers=len(runner_ctxs)) as pool:
                    futures = [
                        pool.submit(
                            app_instance.orchestrator.try_claim_atomic_service_run,
                            ctx,
                        )
                        for ctx in runner_ctxs
                    ]
                    results = [f.result() for f in as_completed(futures)]

            winners = [r for r in results if r is not None]
            assert len(winners) == 1, (
                f"cycle {cycle}: expected one winner, got {len(winners)}"
            )

            active = app_instance.orchestrator.get_active_atomic_service_executions()
            assert len(active) == 1
            assert active[0].atomic_service_run_id == winners[0].atomic_service_run_id

            _finalize(app_instance, winners[0])
            winners_total += 1

        assert winners_total == cycles
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )


@pytest.mark.parametrize("burst_threads", [4, 16, 32])
def test_same_runner_repeated_claims_after_finalization_admit_next_slot(
    app_instance: Pynenc, burst_threads: int
) -> None:
    """A finalized RUNNING record releases the consensus block."""
    runner_ctxs = [_make_runner_ctx(f"runner-{i:02d}") for i in range(burst_threads)]
    for ctx in runner_ctxs:
        app_instance.orchestrator.register_runner_heartbeats(
            [ctx.runner_id], can_run_atomic_service=True
        )

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = burst_threads * 1.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        admitted = 0
        slot_seconds = 60.0
        for idx, _ctx in enumerate(runner_ctxs):
            t = idx * slot_seconds
            with patch("pynenc.orchestrator.base_orchestrator.time", return_value=t):
                with ThreadPoolExecutor(max_workers=burst_threads) as pool:
                    futures = [
                        pool.submit(
                            app_instance.orchestrator.try_claim_atomic_service_run,
                            other_ctx,
                        )
                        for other_ctx in runner_ctxs
                    ]
                    results = [f.result() for f in as_completed(futures)]

            winners = [r for r in results if r is not None]
            assert len(winners) == 1, (
                f"slot {idx}: expected one winner, got {len(winners)}: "
                f"{[w.runner_id for w in winners]}"
            )
            _finalize(app_instance, winners[0])
            admitted += 1

        assert admitted == burst_threads
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )
