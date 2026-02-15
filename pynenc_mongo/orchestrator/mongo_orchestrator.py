from collections.abc import Iterator
from datetime import UTC, datetime
from functools import cached_property
from time import sleep, time
from typing import TYPE_CHECKING

from pymongo import ReturnDocument
from pynenc.exceptions import (
    InvocationStatusError,
    InvocationStatusRaceConditionError,
)
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.invocation.status import (
    InvocationStatus,
    InvocationStatusRecord,
    get_status_definition,
    status_record_transition,
)
from pynenc.orchestrator.atomic_service import ActiveRunnerInfo
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseOrchestrator,
)

from pynenc_mongo.conf.config_orchestrator import ConfigOrchestratorMongo
from pynenc_mongo.orchestrator.mongo_orchestrator_collections import (
    OrchestratorCollections,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.call_id import CallId
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task, TaskId
    from pynenc.types import Params, Result


class MongoBlockingControl(BaseBlockingControl):
    """Blocking control for MongoOrchestrator using MongoDB for cross-process invocation dependencies."""

    def __init__(self, app: "Pynenc", cols: OrchestratorCollections) -> None:
        self.app = app
        self.cols = cols

    def waiting_for_results(
        self,
        caller_invocation_id: "InvocationId | None",
        result_invocation_ids: list["InvocationId"],
    ) -> None:
        """Notifies the system that an invocation is waiting for the results of other invocations."""
        for waited_id in result_invocation_ids:
            self.cols.orchestrator_blocking_edges.insert_or_ignore(
                {"waiter_id": caller_invocation_id, "waited_id": waited_id}
            )

    def release_waiters(self, waited_invocation_id: "InvocationId") -> None:
        """Removes an invocation from the graph, along with any dependencies related to it."""
        self.cols.orchestrator_blocking_edges.delete_many(
            {"waited_id": waited_invocation_id}
        )

    def get_blocking_invocations(
        self, max_num_invocations: int
    ) -> Iterator["InvocationId"]:
        """
        Retrieves invocations that are blocking others but are not themselves waiting for any results.

        Ensures each invocation is yielded only once.
        """
        available_statuses = InvocationStatus.get_available_for_run_statuses()
        pipeline = [
            {
                "$match": {
                    "waited_id": {
                        "$nin": list(
                            self.cols.orchestrator_blocking_edges.distinct("waiter_id")
                        )
                    }
                }
            },
            {
                "$lookup": {
                    "from": "orchestrator_invocations",
                    "localField": "waited_id",
                    "foreignField": "invocation_id",
                    "as": "invocation",
                }
            },
            {"$unwind": "$invocation"},
            {
                "$match": {
                    "invocation.status": {"$in": [s.value for s in available_statuses]}
                }
            },
            {"$project": {"waited_id": 1}},
        ]

        if max_num_invocations > 0:
            pipeline.append({"$limit": max_num_invocations})

        docs = self.cols.orchestrator_blocking_edges.aggregate(pipeline)
        seen: set[str] = set()
        for doc in docs:
            waited_id = doc["waited_id"]
            if waited_id not in seen:
                seen.add(waited_id)
                yield InvocationId(waited_id)


class MongoOrchestrator(BaseOrchestrator):
    """
    A MongoDB-based implementation of the orchestrator for cross-process coordination.

    This orchestrator uses MongoDB for persistent storage, suitable for testing process runners.
    It mirrors the functionality of SQLiteOrchestrator.

    ```{warning}
    The `MongoOrchestrator` class is designed for testing purposes only and should
    not be used in production systems. It uses MongoDB for state management.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = OrchestratorCollections(self.conf)
        self._blocking_control = MongoBlockingControl(app, self.cols)

    @cached_property
    def conf(self) -> ConfigOrchestratorMongo:
        return ConfigOrchestratorMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def blocking_control(self) -> BaseBlockingControl:
        """Return blocking control."""
        return self._blocking_control

    def _register_new_invocations(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        runner_id: str | None = None,
    ) -> InvocationStatusRecord:
        """Register new invocations with status Registered if they don't exist yet."""
        status_record = InvocationStatusRecord(InvocationStatus.REGISTERED, runner_id)
        for invocation in invocations:
            self.cols.orchestrator_invocations.insert_or_ignore(
                {
                    "invocation_id": invocation.invocation_id,
                    "task_id_key": invocation.task.task_id.key,
                    "call_id_key": invocation.call.call_id.key,
                    "status": status_record.status.value,
                    "status_runner_id": status_record.runner_id,
                    "status_timestamp": status_record.timestamp,
                    "retry_count": 0,
                    "auto_purge_timestamp": None,
                    "ownership_claims": [],
                }
            )
        return status_record

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> Iterator["InvocationId"]:
        """Get existing invocation IDs for a task, optionally filtered by arguments and statuses."""
        query: dict = {"task_id_key": task.task_id.key}
        if statuses:
            query["status"] = {"$in": [s.value for s in statuses]}

        if key_serialized_arguments:
            pipeline = [
                {"$match": query},
                {
                    "$lookup": {
                        "from": "orchestrator_invocation_args",
                        "localField": "invocation_id",
                        "foreignField": "invocation_id",
                        "as": "args",
                    }
                },
                {
                    "$match": {
                        "$and": [
                            {"args": {"$elemMatch": {"arg_key": k, "arg_value": v}}}
                            for k, v in key_serialized_arguments.items()
                        ]
                    }
                },
                {"$project": {"invocation_id": 1}},
            ]
            docs = self.cols.orchestrator_invocations.aggregate(pipeline)
        else:
            docs = self.cols.orchestrator_invocations.find(query)

        for doc in docs:
            yield InvocationId(doc["invocation_id"])

    def get_task_invocation_ids(self, task_id: "TaskId") -> Iterator["InvocationId"]:
        """Retrieves all invocation IDs for a given task ID."""
        docs = self.cols.orchestrator_invocations.find({"task_id_key": task_id.key})
        for doc in docs:
            yield InvocationId(doc["invocation_id"])

    def get_invocation_ids_paginated(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["InvocationId"]:
        """
        Retrieve invocation IDs with pagination support.

        Results are ordered by registration time (newest first).

        :param task_id: Optional task ID to filter by
        :param statuses: Optional statuses to filter by
        :param limit: Maximum number of results to return
        :param offset: Number of results to skip
        :return: List of matching invocation IDs
        """
        query: dict = {}
        if task_id:
            query["task_id_key"] = task_id.key
        if statuses:
            query["status"] = {"$in": [s.value for s in statuses]}

        docs = (
            self.cols.orchestrator_invocations.find(query, {"invocation_id": 1})
            .sort("status_timestamp", -1)
            .skip(offset)
            .limit(limit)
        )
        return [InvocationId(doc["invocation_id"]) for doc in docs]

    def count_invocations(
        self,
        task_id: "TaskId | None" = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> int:
        """
        Count invocations matching the given filters.

        :param task_id: Optional task ID to filter by
        :param statuses: Optional statuses to filter by
        :return: The total count of matching invocations
        """
        query: dict = {}
        if task_id:
            query["task_id_key"] = task_id.key
        if statuses:
            query["status"] = {"$in": [s.value for s in statuses]}

        return self.cols.orchestrator_invocations.count_documents(query)

    def get_call_invocation_ids(self, call_id: "CallId") -> Iterator["InvocationId"]:
        """Retrieves all invocation IDs for a given call ID."""
        docs = self.cols.orchestrator_invocations.find({"call_id_key": call_id})
        for doc in docs:
            yield InvocationId(doc["invocation_id"])

    def any_non_final_invocations(self, call_id: str) -> bool:
        """Checks if there are any non-final invocations for a specific call ID."""
        final_statuses = [s.value for s in InvocationStatus.get_final_statuses()]
        return (
            self.cols.orchestrator_invocations.find_one(
                {"call_id": call_id, "status": {"$nin": final_statuses}}
            )
            is not None
        )

    def _validate_ownership_acquisition(
        self, invocation_id: str, runner_id: str
    ) -> bool:
        """
        Validate ownership acquisition using time-based consensus.

        This implements a pseudo-atomic ownership protocol:
        1. Add runner_id to ownership_claims array
        2. Wait for consensus period
        3. Check if our runner_id is first in the array

        :param invocation_id: ID of the invocation to claim
        :param runner_id: ID of the worker attempting to claim ownership
        :return: True if ownership successfully acquired, False otherwise
        """
        # Step 1: Push our runner_id to the claims array
        self.cols.orchestrator_invocations.update_one(
            {"invocation_id": invocation_id}, {"$push": {"ownership_claims": runner_id}}
        )

        # Step 2: Wait for consensus period
        sleep(self.conf.ownership_consensus_wait_seconds)

        # Step 3: Check if we won the race
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )

        if not doc:
            raise KeyError(f"Invocation ID {invocation_id} not found")

        ownership_claims = doc.get("ownership_claims", [])

        # We win if we're first in the list
        return ownership_claims and ownership_claims[0] == runner_id

    def _release_ownership(self, invocation_id: str) -> None:
        """
        Release ownership by clearing the ownership_claims array.

        :param invocation_id: ID of the invocation to release
        """
        self.cols.orchestrator_invocations.update_one(
            {"invocation_id": invocation_id}, {"$set": {"ownership_claims": []}}
        )

    def _atomic_status_transition(
        self, invocation_id: str, status: InvocationStatus, runner_id: str | None = None
    ) -> InvocationStatusRecord:
        """Set the status of an invocation by ID."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            raise KeyError(f"Invocation ID {invocation_id} not found")
        prev_status_record = InvocationStatusRecord(
            InvocationStatus(doc["status"]),
            doc["status_runner_id"],
            doc["status_timestamp"],
        )

        # Validate the transition
        new_record = status_record_transition(prev_status_record, status, runner_id)

        # Check ownership acquisition if needed: pseudo-atomic protocol
        new_def = get_status_definition(status)
        if new_def.acquires_ownership:
            if not runner_id:
                raise InvocationStatusError(
                    f"Owner ID must be provided when transitioning to status {status}"
                )
            if not self._validate_ownership_acquisition(invocation_id, runner_id):
                raise InvocationStatusRaceConditionError(
                    invocation_id=invocation_id,
                    previous_status_record=prev_status_record,
                    expected_status_record=new_record,
                    actual_status_record=self.get_invocation_status_record(
                        invocation_id
                    ),
                )

        update_doc: dict = {
            "status": new_record.status.value,
            "status_runner_id": new_record.runner_id,
            "status_timestamp": new_record.timestamp,
        }

        if prev_doc_on_update := self.cols.orchestrator_invocations.find_one_and_update(
            {"invocation_id": invocation_id},
            {"$set": update_doc},
            return_document=ReturnDocument.BEFORE,
        ):
            prev_status_record_on_update = InvocationStatusRecord(
                InvocationStatus(prev_doc_on_update["status"]),
                prev_doc_on_update["status_runner_id"],
                prev_doc_on_update["status_timestamp"],
            )
            if prev_status_record != prev_status_record_on_update:
                error_msg = (
                    f"We observed a race condition when updating invocation {invocation_id} status from {prev_status_record} to {status}. "
                    f"The previous status record used for the update was {prev_status_record}, but the actual status record on update was {prev_status_record_on_update}. "
                    "Plese upgrade to a newer version of mongoDB that supports atomic updates and/or transactions. "
                )
                try:
                    _ = status_record_transition(
                        prev_status_record_on_update, status, runner_id
                    )
                    self.app.logger.warning(
                        error_msg
                        + "Continuing without raising an error, because the transition of states is still valid. "
                    )
                except InvocationStatusError as ex:
                    self.app.logger.error(
                        f"{error_msg} the trasition is invalid and we cannot continue {ex}"
                    )
                    raise InvocationStatusRaceConditionError(
                        invocation_id=invocation_id,
                        previous_status_record=prev_status_record,
                        expected_status_record=prev_status_record,
                        actual_status_record=prev_status_record_on_update,
                    ) from ex

        # Check if this transition releases ownership
        if new_def.releases_ownership:
            self._release_ownership(invocation_id)

        return new_record

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Index invocation arguments for concurrency control."""
        for key, value in invocation.call.serialized_arguments.items():
            self.cols.orchestrator_invocation_args.insert_or_ignore(
                {
                    "invocation_id": invocation.invocation_id,
                    "arg_key": key,
                    "arg_value": value,
                }
            )

    def set_up_invocation_auto_purge(self, invocation_id: "InvocationId") -> None:
        """Set up invocation for auto-purging by setting the auto_purge_timestamp."""
        self.cols.orchestrator_invocations.update_one(
            {"invocation_id": invocation_id}, {"$set": {"auto_purge_timestamp": time()}}
        )

    def auto_purge(self) -> None:
        """Auto-purge old invocations based on auto_purge_timestamp."""
        threshold = time() - self.conf.auto_final_invocation_purge_hours * 3600
        docs = self.cols.orchestrator_invocations.find(
            {"auto_purge_timestamp": {"$ne": None, "$lte": threshold}}
        )
        for doc in docs:
            invocation_id = InvocationId(doc["invocation_id"])
            self.blocking_control.release_waiters(invocation_id)
            self.cols.orchestrator_invocations.delete_one(
                {"invocation_id": invocation_id}
            )
            self.cols.orchestrator_invocation_args.delete_many(
                {"invocation_id": invocation_id}
            )

    def get_invocation_status_record(
        self, invocation_id: "InvocationId"
    ) -> InvocationStatusRecord:
        """Get the current status of an invocation by ID, handling pending timeouts."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            raise KeyError(f"Invocation ID {invocation_id} not found")
        return InvocationStatusRecord(
            InvocationStatus(doc["status"]),
            doc["status_runner_id"],
            doc["status_timestamp"],
        )

    def increment_invocation_retries(self, invocation_id: "InvocationId") -> None:
        """Increment the retry count for an invocation by ID."""
        self.cols.orchestrator_invocations.update_one(
            {"invocation_id": invocation_id}, {"$inc": {"retry_count": 1}}
        )

    def get_invocation_retries(self, invocation_id: "InvocationId") -> int:
        """Get the number of retries for an invocation by ID."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        return doc.get("retry_count", 0) if doc else 0

    def filter_by_status(
        self,
        invocation_ids: list["InvocationId"],
        status_filter: frozenset["InvocationStatus"],
    ) -> list["InvocationId"]:
        """Filter invocations by status by ID."""
        if not invocation_ids or not status_filter:
            return []
        docs = self.cols.orchestrator_invocations.find(
            {
                "invocation_id": {"$in": invocation_ids},
                "status": {"$in": [s.value for s in status_filter]},
            }
        )
        return [InvocationId(doc["invocation_id"]) for doc in docs]

    def register_runner_heartbeats(
        self,
        runner_ids: list[str],
        can_run_atomic_service: bool = False,
    ) -> None:
        """
        Register or update runners' heartbeat timestamp.

        :param list[str] runner_ids: The list of runner IDs to register.
        :param bool can_run_atomic_service: Whether these runners are eligible to run atomic services.
        """
        current_time = time()
        for runner_id in runner_ids:
            self.app.logger.debug(f"Registering heartbeat for runner: {runner_id}")
            self.cols.orchestrator_runner_heartbeats.update_one(
                {"runner_id": runner_id},
                {
                    "$set": {
                        "runner_id": runner_id,
                        "last_heartbeat": current_time,
                        "allow_to_run_atomic_service": can_run_atomic_service,
                    },
                    "$setOnInsert": {
                        "creation_timestamp": current_time,
                        "last_service_start": None,
                        "last_service_end": None,
                    },
                },
                upsert=True,
            )

    def record_atomic_service_execution(
        self, runner_id: str, start_time: datetime, end_time: datetime
    ) -> None:
        """Record the latest atomic service execution window for a runner."""
        self.cols.orchestrator_runner_heartbeats.update_one(
            {"runner_id": runner_id},
            {"$set": {"last_service_start": start_time, "last_service_end": end_time}},
        )

    def _get_active_runners(
        self, timeout_seconds: float, can_run_atomic_service: bool | None
    ) -> list["ActiveRunnerInfo"]:
        """
        Retrieve runners that are considered active based on heartbeat activity.

        A runner is considered "active" if it has sent a heartbeat within the timeout period.
        This is used for atomic service scheduling to determine which runners are eligible
        to participate in time slot distribution.

        :param float timeout_seconds: Heartbeat timeout in seconds (typically from atomic_service_runner_considered_dead_after_minutes config)
        :param bool | None can_run_atomic_service: If specified, filters runners based on their eligibility to run atomic services
        :return: List of active runners ordered by creation time (oldest first)
        :rtype: list["ActiveRunnerInfo"]
        """
        cutoff_time = time() - timeout_seconds

        query: dict = {"last_heartbeat": {"$gte": cutoff_time}}
        if can_run_atomic_service is not None:
            query["allow_to_run_atomic_service"] = can_run_atomic_service

        docs = self.cols.orchestrator_runner_heartbeats.find(query).sort(
            "creation_timestamp", 1
        )

        active_runners = []
        for doc in docs:
            # MongoDB stores datetimes as naive UTC - make them aware
            last_service_start = doc.get("last_service_start")
            if last_service_start is not None and last_service_start.tzinfo is None:
                last_service_start = last_service_start.replace(tzinfo=UTC)

            last_service_end = doc.get("last_service_end")
            if last_service_end is not None and last_service_end.tzinfo is None:
                last_service_end = last_service_end.replace(tzinfo=UTC)

            active_runners.append(
                ActiveRunnerInfo(
                    runner_id=doc["runner_id"],
                    creation_time=datetime.fromtimestamp(
                        doc["creation_timestamp"], tz=UTC
                    ),
                    last_heartbeat=datetime.fromtimestamp(
                        doc["last_heartbeat"], tz=UTC
                    ),
                    allow_to_run_atomic_service=doc["allow_to_run_atomic_service"],
                    last_service_start=last_service_start,
                    last_service_end=last_service_end,
                )
            )

        return active_runners

    def get_pending_invocations_for_recovery(self) -> Iterator["InvocationId"]:
        """
        Retrieve invocation IDs stuck in PENDING status beyond the allowed time.

        :return: Iterator of invocation IDs that have been pending longer than max_pending_seconds
        """
        max_pending_seconds = self.app.conf.max_pending_seconds
        current_time = time()
        cutoff_timestamp = current_time - max_pending_seconds
        cutoff_time = datetime.fromtimestamp(cutoff_timestamp, tz=UTC)

        docs = self.cols.orchestrator_invocations.find(
            {
                "status": InvocationStatus.PENDING.value,
                "status_timestamp": {"$lt": cutoff_time},
            }
        )

        for doc in docs:
            yield InvocationId(doc["invocation_id"])

    def _get_running_invocations_for_recovery(
        self, timeout_seconds: float
    ) -> Iterator["InvocationId"]:
        """
        Retrieve invocation IDs in RUNNING status owned by inactive runners.

        An inactive runner is one that hasn't sent a heartbeat within the
        configured timeout period. Invocations owned by such runners are
        considered stuck and need recovery.

        :param float timeout_seconds: Heartbeat timeout in seconds
        :return: Iterator of invocation IDs that need recovery.
        :rtype: Iterator[str]
        """
        cutoff_time = time() - timeout_seconds

        # Step 1: Find inactive runners (with stale heartbeats)
        inactive_runners_with_stale_heartbeat = list(
            self.cols.orchestrator_runner_heartbeats.find(
                {"last_heartbeat": {"$lt": cutoff_time}}, {"runner_id": 1}
            )
        )

        inactive_runner_ids = {
            runner["runner_id"] for runner in inactive_runners_with_stale_heartbeat
        }

        # Step 2: Get active runner IDs (with fresh heartbeats)
        active_runners = list(
            self.cols.orchestrator_runner_heartbeats.find(
                {"last_heartbeat": {"$gte": cutoff_time}}, {"runner_id": 1}
            )
        )
        active_runner_ids = {runner["runner_id"] for runner in active_runners}

        if inactive_runner_ids:
            self.app.logger.info(
                f"Inactive runners (stale heartbeat): {inactive_runner_ids}"
            )

        # Step 3: Find all RUNNING invocations
        running_invocations = self.cols.orchestrator_invocations.find(
            {
                "status": InvocationStatus.RUNNING.value,
                "status_runner_id": {"$ne": None},
            },
            {"invocation_id": 1, "status_runner_id": 1},
        )

        # Step 4: Yield invocations owned by inactive runners (no heartbeat or stale heartbeat)
        for invocation in running_invocations:
            runner_id = invocation["status_runner_id"]
            # Recovery if owner has no heartbeat OR has stale heartbeat
            if runner_id not in active_runner_ids:
                if runner_id not in inactive_runner_ids:
                    self.app.logger.info(f"Runner {runner_id} has no heartbeat record")
                self.app.logger.info(
                    f"Invocation to recover: {invocation['invocation_id']}"
                )
                yield InvocationId(invocation["invocation_id"])

    def purge(self) -> None:
        """Clear all orchestrator state."""
        self.cols.purge_all()
