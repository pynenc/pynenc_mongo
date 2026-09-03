from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

from pymongo.errors import DuplicateKeyError
from pynenc.orchestrator.atomic_service import (
    AtomicServiceExecutionStatus,
    AtomicServiceRun,
)
from pynenc_tests.unit.orchestrator.all_tests import *

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_mongo_runner_heartbeat_recovers_from_concurrent_upsert(
    app_instance: "Pynenc",
) -> None:
    """A competing first heartbeat must not stop the runner."""
    heartbeats = app_instance.orchestrator.cols.orchestrator_runner_heartbeats
    update_one = MagicMock(side_effect=[DuplicateKeyError("duplicate"), None])
    heartbeats.update_one = update_one

    app_instance.orchestrator.register_runner_heartbeats(["runner-1"])

    assert update_one.call_count == 2
    assert update_one.call_args_list[0].kwargs["upsert"] is True
    assert "upsert" not in update_one.call_args_list[1].kwargs


def test_mongo_atomic_service_start_admits_only_one_active_run(
    app_instance: "Pynenc",
) -> None:
    """Mongo's storage gate admits one concurrent RUNNING execution."""
    runs = [
        AtomicServiceRun(
            runner_id=f"runner-{index}",
            atomic_service_run_id=f"run-{index}",
        )
        for index in range(16)
    ]

    with ThreadPoolExecutor(max_workers=len(runs)) as pool:
        accepted = list(
            pool.map(
                lambda run: app_instance.orchestrator.record_atomic_service_execution_start(
                    run, None
                ),
                runs,
            )
        )

    assert accepted.count(True) == 1
    winner = runs[accepted.index(True)]
    active = app_instance.orchestrator.get_active_atomic_service_executions()
    assert [execution.atomic_service_run_id for execution in active] == [
        winner.atomic_service_run_id
    ]

    app_instance.orchestrator.finalize_atomic_service_execution(
        winner,
        datetime.now(UTC),
        AtomicServiceExecutionStatus.COMPLETED,
    )
