from collections.abc import Callable, Iterator
from functools import cached_property
from time import time
from typing import TYPE_CHECKING

from pynenc.call import Call
from pynenc.exceptions import (
    CycleDetectedError,
    InvocationOnFinalStatusError,
    PendingInvocationLockError,
)
from pynenc.invocation.status import InvocationStatus
from pynenc.orchestrator.base_orchestrator import (
    BaseBlockingControl,
    BaseCycleControl,
    BaseOrchestrator,
)

from pynenc_mongo.conf.config_orchestrator import ConfigOrchestratorMongo
from pynenc_mongo.orchestrator.mongo_orchestrator_collections import (
    OrchestratorCollections,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task
    from pynenc.types import Params, Result


class MongoCycleControl(BaseCycleControl):
    """Cycle control for MongoOrchestrator using MongoDB for cross-process cycle detection."""

    def __init__(self, app: "Pynenc", cols: OrchestratorCollections) -> None:
        self.app = app
        self.cols = cols

    def add_call_and_check_cycles(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> None:
        """Add a call dependency and check for cycles using graph traversal."""
        # Check for direct self-cycle first
        if caller.call_id == callee.call_id:
            raise CycleDetectedError.from_cycle([caller.call])
        if cycle := self.find_cycle_caused_by_new_invocation(caller, callee):
            raise CycleDetectedError.from_cycle(cycle)

        # Add calls to tracking

        self.cols.orchestrator_cycle_calls.insert_or_ignore(
            {"call_id": caller.call_id, "call_json": caller.call.to_json()}
        )
        self.cols.orchestrator_cycle_calls.insert_or_ignore(
            {"call_id": callee.call_id, "call_json": callee.call.to_json()}
        )
        self.cols.orchestrator_cycle_edges.insert_or_ignore(
            {"caller_id": caller.call_id, "callee_id": callee.call_id}
        )

    def get_callees(self, caller_call_id: str) -> Iterator[str]:
        """Returns an iterator of direct callee call_ids for the given caller_call_id."""
        docs = self.cols.orchestrator_cycle_edges.find({"caller_id": caller_call_id})
        for doc in docs:
            yield doc["callee_id"]

    def _find_cycle_with_new_edge(
        self, caller_id: str, callee_id: str
    ) -> list[str] | None:
        """Find cycle that would be caused by adding a new edge from caller to callee."""
        visited: set[str] = set()
        path: list[str] = []

        def get_edges(call_id: str) -> list[str]:
            edges = [
                doc["callee_id"]
                for doc in self.cols.orchestrator_cycle_edges.find(
                    {"caller_id": call_id}
                )
            ]
            if call_id == caller_id and callee_id not in edges:
                edges.append(callee_id)
            return edges

        return self._find_cycle_dfs(caller_id, visited, path, get_edges)

    def _find_cycle_dfs(
        self,
        current_id: str,
        visited: set[str],
        path: list[str],
        get_edges: Callable[[str], list[str]],
    ) -> list[str] | None:
        """DFS utility to find cycles."""
        visited.add(current_id)
        path.append(current_id)

        for next_id in get_edges(current_id):
            if next_id not in visited:
                cycle = self._find_cycle_dfs(next_id, visited, path, get_edges)
                if cycle:
                    return cycle
            elif next_id in path:
                cycle_start_idx = path.index(next_id)
                return path[cycle_start_idx:]

        path.remove(current_id)
        return None

    def find_cycle_caused_by_new_invocation(
        self, caller: "DistributedInvocation", callee: "DistributedInvocation"
    ) -> list["Call"]:
        """
        Checks if adding a new call from `caller` to `callee` would create a cycle.
        :param DistributedInvocation caller: The invocation making the call.
        :param DistributedInvocation callee: The invocation being called.
        :return: List of `Call` objects forming the cycle, if a cycle is detected; otherwise, an empty list.
        """
        # Temporarily add the edge to check if it would cause a cycle
        self.cols.orchestrator_cycle_edges.insert_or_ignore(
            {"caller_id": caller.call_id, "callee_id": callee.call_id}
        )

        # Set for tracking visited nodes
        visited: set[str] = set()

        # List for tracking the nodes on the path from caller to callee
        path: list[str] = []

        cycle = self._is_cyclic_util(caller.call_id, visited, path)

        # Remove the temporarily added edge
        self.cols.orchestrator_cycle_edges.delete_one(
            {"caller_id": caller.call_id, "callee_id": callee.call_id}
        )

        return cycle

    def _is_cyclic_util(
        self,
        current_call_id: str,
        visited: set[str],
        path: list[str],
    ) -> list["Call"]:
        """
        A utility function for cycle detection.
        :param str current_call_id: The current call ID being examined.
        :param set[str] visited: A set of visited call IDs for cycle detection.
        :param list[str] path: A list representing the current path of call IDs.
        :return: List of `Call` objects forming a cycle, if a cycle is detected; otherwise, an empty list.
        """
        visited.add(current_call_id)
        path.append(current_call_id)

        call_cycle = []
        for edge in self.cols.orchestrator_cycle_edges.find(
            {"caller_id": current_call_id}
        ):
            neighbour_call_id = edge["callee_id"]
            if neighbour_call_id not in visited:
                cycle = self._is_cyclic_util(neighbour_call_id, visited, path)
                if cycle:
                    return cycle
            elif neighbour_call_id in path:
                cycle_start_index = path.index(neighbour_call_id)
                for _id in path[cycle_start_index:]:
                    if doc := self.cols.orchestrator_cycle_calls.find_one(
                        {"call_id": _id}
                    ):
                        call_cycle.append(Call.from_json(self.app, doc["call_json"]))
        path.pop()
        return call_cycle

    def clean_up_invocation_cycles(self, invocation_id: str) -> None:
        """Clean up cycle tracking data for a completed invocation."""
        call_id = self.app.orchestrator.get_invocation_call_id(invocation_id)
        if not self.app.orchestrator.any_non_final_invocations(call_id):
            self.cols.orchestrator_cycle_calls.delete_one({"call_id": call_id})
            self.cols.orchestrator_cycle_edges.delete_many(
                {"$or": [{"caller_id": call_id}, {"callee_id": call_id}]}
            )


class MongoBlockingControl(BaseBlockingControl):
    """Blocking control for MongoOrchestrator using MongoDB for cross-process invocation dependencies."""

    def __init__(self, app: "Pynenc", cols: OrchestratorCollections) -> None:
        self.app = app
        self.cols = cols

    def waiting_for_results(
        self, caller_invocation_id: str, result_invocation_ids: list[str]
    ) -> None:
        """Notifies the system that an invocation is waiting for the results of other invocations."""
        for waited_id in result_invocation_ids:
            self.cols.orchestrator_blocking_edges.insert_or_ignore(
                {"waiter_id": caller_invocation_id, "waited_id": waited_id}
            )

    def release_waiters(self, waited_invocation_id: str) -> None:
        """Removes an invocation from the graph, along with any dependencies related to it."""
        self.cols.orchestrator_blocking_edges.delete_many(
            {"waited_id": waited_invocation_id}
        )

    def get_blocking_invocations(self, max_num_invocations: int) -> Iterator[str]:
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
                yield waited_id


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
        self._cycle_control = MongoCycleControl(app, self.cols)
        self._blocking_control = MongoBlockingControl(app, self.cols)

    @cached_property
    def conf(self) -> ConfigOrchestratorMongo:
        return ConfigOrchestratorMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def cycle_control(self) -> BaseCycleControl:
        """Return cycle control."""
        return self._cycle_control

    @property
    def blocking_control(self) -> BaseBlockingControl:
        """Return blocking control."""
        return self._blocking_control

    def _register_new_invocations(
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """Register new invocations with status Registered if they don't exist yet."""
        for invocation in invocations:
            self.cols.orchestrator_invocations.insert_or_ignore(
                {
                    "invocation_id": invocation.invocation_id,
                    "task_id": invocation.task.task_id,
                    "call_id": invocation.call_id,
                    "status": InvocationStatus.REGISTERED.value,
                    "retry_count": 0,
                    "pending_start_time": None,
                    "pre_pending_status": None,
                    "auto_purge_timestamp": None,
                }
            )

    def get_existing_invocations(
        self,
        task: "Task[Params, Result]",
        key_serialized_arguments: dict[str, str] | None = None,
        statuses: list[InvocationStatus] | None = None,
    ) -> Iterator[str]:
        """Get existing invocation IDs for a task, optionally filtered by arguments and statuses."""
        query: dict = {"task_id": task.task_id}
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
            yield doc["invocation_id"]

    def get_task_invocation_ids(self, task_id: str) -> Iterator[str]:
        """Retrieves all invocation IDs for a given task ID."""
        docs = self.cols.orchestrator_invocations.find({"task_id": task_id})
        for doc in docs:
            yield doc["invocation_id"]

    def get_call_invocation_ids(self, call_id: str) -> Iterator[str]:
        """Retrieves all invocation IDs for a given call ID."""
        docs = self.cols.orchestrator_invocations.find({"call_id": call_id})
        for doc in docs:
            yield doc["invocation_id"]

    def get_invocation_call_id(self, invocation_id: str) -> str:
        """Retrieves the call ID associated with a specific invocation ID."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            raise KeyError(f"Invocation ID {invocation_id} not found")
        return doc["call_id"]

    def any_non_final_invocations(self, call_id: str) -> bool:
        """Checks if there are any non-final invocations for a specific call ID."""
        final_statuses = [s.value for s in InvocationStatus.get_final_statuses()]
        return (
            self.cols.orchestrator_invocations.find_one(
                {"call_id": call_id, "status": {"$nin": final_statuses}}
            )
            is not None
        )

    def _set_invocation_status(
        self,
        invocation_id: str,
        status: InvocationStatus,
    ) -> None:
        """Set the status of an invocation by ID."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            raise KeyError(f"Invocation ID {invocation_id} not found")
        prev_status = InvocationStatus(doc["status"])
        if prev_status == status:
            if prev_status == InvocationStatus.PENDING:
                raise PendingInvocationLockError(invocation_id)
            self.app.logger.debug(
                f"Invocation {invocation_id} already in status {status}, no change"
            )
            return
        if prev_status.is_final():
            raise InvocationOnFinalStatusError(invocation_id, prev_status, status)

        update_doc: dict = {"status": status.value}
        if status == InvocationStatus.PENDING:
            update_doc["pre_pending_status"] = prev_status.value
            update_doc["pending_start_time"] = time()
        else:
            update_doc["pre_pending_status"] = None
            update_doc["pending_start_time"] = None

        self.cols.orchestrator_invocations.update_one(
            {"invocation_id": invocation_id}, {"$set": update_doc}
        )

    def _set_invocation_pending_status(self, invocation_id: str) -> None:
        """Set an invocation to pending status."""
        self._set_invocation_status(invocation_id, InvocationStatus.PENDING)

    def index_arguments_for_concurrency_control(
        self,
        invocation: "DistributedInvocation[Params, Result]",
    ) -> None:
        """Index invocation arguments for concurrency control."""
        for key, value in invocation.serialized_arguments.items():
            self.cols.orchestrator_invocation_args.insert_or_ignore(
                {
                    "invocation_id": invocation.invocation_id,
                    "arg_key": key,
                    "arg_value": value,
                }
            )

    def get_invocation_pending_timer(self, invocation_id: str) -> float | None:
        """Retrieves the pending timer for a specific invocation."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        return doc.get("pending_start_time") if doc else None

    def set_up_invocation_auto_purge(self, invocation_id: str) -> None:
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
            invocation_id = doc["invocation_id"]
            self.cycle_control.clean_up_invocation_cycles(invocation_id)
            self.blocking_control.release_waiters(invocation_id)
            self.cols.orchestrator_invocations.delete_one(
                {"invocation_id": invocation_id}
            )
            self.cols.orchestrator_invocation_args.delete_many(
                {"invocation_id": invocation_id}
            )

    def get_invocation_status(self, invocation_id: str) -> InvocationStatus:
        """Get the current status of an invocation by ID, handling pending timeouts."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            raise KeyError(f"Invocation ID {invocation_id} not found")
        status = InvocationStatus(doc["status"])
        if status == InvocationStatus.PENDING:
            pending_start_time = doc.get("pending_start_time")
            pre_pending_status = doc.get("pre_pending_status")
            if pending_start_time and pre_pending_status:
                elapsed = time() - pending_start_time
                if elapsed > self.app.conf.max_pending_seconds:
                    self.cols.orchestrator_invocations.update_one(
                        {"invocation_id": invocation_id},
                        {
                            "$set": {
                                "status": pre_pending_status,
                                "pending_start_time": None,
                            }
                        },
                    )
                    return InvocationStatus(pre_pending_status)
        return status

    def increment_invocation_retries(self, invocation_id: str) -> None:
        """Increment the retry count for an invocation by ID."""
        self.cols.orchestrator_invocations.update_one(
            {"invocation_id": invocation_id}, {"$inc": {"retry_count": 1}}
        )

    def get_invocation_retries(self, invocation_id: str) -> int:
        """Get the number of retries for an invocation by ID."""
        doc = self.cols.orchestrator_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        return doc.get("retry_count", 0) if doc else 0

    def filter_by_status(
        self, invocation_ids: list[str], status_filter: set["InvocationStatus"]
    ) -> list[str]:
        """Filter invocations by status by ID."""
        if not invocation_ids or not status_filter:
            return []
        docs = self.cols.orchestrator_invocations.find(
            {
                "invocation_id": {"$in": invocation_ids},
                "status": {"$in": [s.value for s in status_filter]},
            }
        )
        return [doc["invocation_id"] for doc in docs]

    def purge(self) -> None:
        """Clear all orchestrator state."""
        self.cols.purge_all()
