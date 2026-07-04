from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from pynenc.orchestrator.atomic_service import (
    AtomicServiceExecutionStatus,
    AtomicServiceRun,
)
from pynenc_tests.unit.orchestrator.all_tests import *

if TYPE_CHECKING:
    from pynenc import Pynenc


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
