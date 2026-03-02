import logging
from collections.abc import Iterator
from datetime import datetime
from enum import StrEnum, auto
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError
from pynenc.app_info import AppInfo
from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.identifiers.task_id import TaskId
from pynenc.invocation.dist_invocation import InvocationDTO
from pynenc.models.call_dto import CallDTO
from pynenc.runner.runner_context import RunnerContext
from pynenc.state_backend.base_state_backend import BaseStateBackend, InvocationHistory
from pynenc.types import Params, Result
from pynenc.workflow import WorkflowIdentity

from pynenc_mongo.conf.config_state_backend import ConfigStateBackendMongo
from pynenc_mongo.state_backend.mongo_state_backend_collections import (
    StateBackendCollections,
)
from pynenc_mongo.util.mongo_chunk_data import (
    build_chunk_key,
    prepare_chunk_storage,
    retrieve_chunk_storage,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc

logger = logging.getLogger(__name__)


class ChunkPrefix(StrEnum):
    """Key prefixes for different data types in chunk storage."""

    ARGS = auto()  # Task invocation arguments
    RESULT = auto()  # Task invocation result
    EXCEPTION = auto()  # Task invocation exception


class MongoStateBackend(BaseStateBackend[Params, Result]):
    """
    A MongoDB-based implementation of the state backend for cross-process testing.

    Stores invocation data, history, results, and exceptions in MongoDB collections,
    allowing state sharing between processes. Suitable for testing process runners.

    ```{warning}
    The `MongoStateBackend` class is designed for testing purposes only and should
    not be used in production systems. It uses MongoDB for state management.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = StateBackendCollections(self.conf)

    @cached_property
    def conf(self) -> ConfigStateBackendMongo:
        return ConfigStateBackendMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def store_app_info(self, app_info: "AppInfo") -> None:
        """Store app info."""
        self.cols.state_backend_app_info.upsert_document(
            {"app_id": app_info.app_id},
            {"app_id": app_info.app_id, "app_info_json": app_info.to_json()},
        )

    def get_app_info(self) -> "AppInfo":
        """Retrieve app info for the current app."""
        doc = self.cols.state_backend_app_info.find_one({"app_id": self.app.app_id})
        if doc:
            return AppInfo.from_json(doc["app_info_json"])
        raise KeyError(f"App info for {self.app.app_id} not found")

    @staticmethod
    def discover_app_infos() -> dict[str, "AppInfo"]:
        """Retrieve all app information registered in this state backend."""
        default_conf = ConfigStateBackendMongo()
        cols = StateBackendCollections(default_conf)
        apps = {}
        docs = cols.state_backend_app_info.find({})
        for doc in docs:
            apps[doc["app_id"]] = AppInfo.from_json(doc["app_info_json"])
        return apps

    def store_workflow_run(self, workflow_identity: "WorkflowIdentity") -> None:
        """Store a workflow run for tracking and monitoring."""
        w_id = workflow_identity
        self.cols.state_backend_workflows.insert_or_ignore(
            {
                "workflow_id": w_id.workflow_id,
                "workflow_type_key": w_id.workflow_type.key,
                "parent_workflow_id": w_id.parent_workflow_id,
            }
        )

    def _upsert_invocations(
        self, entries: list[tuple["InvocationDTO", "CallDTO"]]
    ) -> None:
        """Store invocations with automatic argument chunking when needed."""
        for inv_dto, call_dto in entries:
            # Prepare arguments storage (inline or chunked)
            args_storage = prepare_chunk_storage(
                self.cols.state_backend_chunks,
                build_chunk_key(
                    invocation_id=inv_dto.invocation_id, prefix=ChunkPrefix.ARGS.value
                ),
                call_dto.serialized_arguments,
                self.conf.chunk_threshold,
            )
            inv_dict = {
                "invocation_id": inv_dto.invocation_id,
                "call_id_key": inv_dto.call_id.key,
                "task_id_key": inv_dto.call_id.task_id.key,
                "workflow_id": inv_dto.workflow.workflow_id,
                "workflow_type_key": inv_dto.workflow.workflow_type.key,
                "parent_workflow_id": inv_dto.workflow.parent_workflow_id,
                "parent_invocation_id": inv_dto.parent_invocation_id,
                "arguments": args_storage,
            }
            self.cols.state_backend_invocations.insert_or_ignore(inv_dict)

    def _get_invocation(
        self, invocation_id: str
    ) -> tuple["InvocationDTO", "CallDTO"] | None:
        """Retrieve invocation, reconstructing arguments from chunks if needed."""
        doc = self.cols.state_backend_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            return None

        # Retrieve arguments (inline or from chunks).
        # "arguments" absent means the document was created by an older schema version
        # that pre-dates chunk storage. insert_or_ignore silently skips re-inserting, so
        # stale documents are never updated. Running with empty args would cause silent
        # data corruption, so we raise a clear error for operators to investigate.
        if "arguments" not in doc:
            doc_keys = [k for k in doc if k != "_id"]
            raise KeyError(
                f"Invocation {invocation_id} document is missing the 'arguments' field. "
                f"This indicates a schema mismatch: the document was likely written by an "
                f"older pynenc_mongo version. Present keys: {doc_keys}"
            )
        serialized_arguments = retrieve_chunk_storage(
            self.cols.state_backend_chunks,
            build_chunk_key(invocation_id=invocation_id, prefix=ChunkPrefix.ARGS.value),
            doc["arguments"],
        )
        assert isinstance(serialized_arguments, dict)
        p_w_id = doc["parent_workflow_id"]
        workflow = WorkflowIdentity(
            workflow_id=InvocationId(doc["workflow_id"]),
            workflow_type=TaskId.from_key(doc["workflow_type_key"]),
            parent_workflow_id=(InvocationId(p_w_id) if p_w_id else None),
        )
        p_i_id = doc["parent_invocation_id"]
        inv_dto = InvocationDTO(
            invocation_id=InvocationId(invocation_id),
            call_id=CallId.from_key(doc["call_id_key"]),
            workflow=workflow,
            parent_invocation_id=(InvocationId(p_i_id) if p_i_id else None),
        )
        call_dto = CallDTO(inv_dto.call_id, serialized_arguments)
        return inv_dto, call_dto

    def _add_histories(
        self,
        invocation_ids: list["InvocationId"],
        invocation_history: "InvocationHistory",
    ) -> None:
        """Adds the same history record for a list of invocations."""
        for invocation_id in invocation_ids:
            try:
                self.cols.state_backend_history.insert_or_ignore(
                    {
                        "invocation_id": invocation_id,
                        "history_timestamp": invocation_history._timestamp,
                        "history_status": invocation_history.status_record.status,
                        "history_json": invocation_history.to_json(),
                    }
                )
            except DuplicateKeyError as e:
                self.app.logger.debug(
                    f"Error adding {invocation_history=} already exists: {e}"
                )

    def _get_history(self, invocation_id: "InvocationId") -> list["InvocationHistory"]:
        """Retrieves the history of an invocation ordered by timestamp."""
        docs = self.cols.state_backend_history.find(
            {"invocation_id": invocation_id}
        ).sort("history_timestamp", ASCENDING)
        return [InvocationHistory.from_json(doc["history_json"]) for doc in docs]

    def iter_invocations_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationId"]]:
        """
        Iterate over invocation IDs that have history within time range.

        Uses MongoDB's native cursor with batch_size for efficient pagination.

        :param start_time: Start of time range (inclusive)
        :param end_time: End of time range (inclusive)
        :param batch_size: Number of invocation IDs per batch
        :return: Iterator yielding batches of invocation IDs
        """
        # Use aggregation to get distinct invocation_ids with pagination
        pipeline = [
            {
                "$match": {
                    "history_timestamp": {
                        "$gte": start_time,
                        "$lte": end_time,
                    }
                }
            },
            {"$group": {"_id": "$invocation_id"}},
            {"$sort": {"_id": ASCENDING}},
        ]

        cursor = self.cols.state_backend_history.aggregate(pipeline)
        batch: list["InvocationId"] = []

        for doc in cursor:
            batch.append(InvocationId(doc["_id"]))
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def iter_history_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationHistory"]]:
        """
        Iterate over history entries within time range.

        Uses MongoDB's native cursor with batch_size for efficient iteration.

        :param start_time: Start of time range (inclusive)
        :param end_time: End of time range (inclusive)
        :param batch_size: Number of history entries per batch
        :return: Iterator yielding batches of InvocationHistory objects
        """
        cursor = (
            self.cols.state_backend_history.find(
                {
                    "history_timestamp": {
                        "$gte": start_time,
                        "$lte": end_time,
                    }
                }
            )
            .sort("history_timestamp", ASCENDING)
            .batch_size(batch_size)
        )

        batch: list[InvocationHistory] = []
        for doc in cursor:
            batch.append(InvocationHistory.from_json(doc["history_json"]))
            if len(batch) >= batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _get_result(self, invocation_id: "InvocationId") -> str:
        """Retrieve result, decompressing from chunks if needed."""
        doc = self.cols.state_backend_results.find_one(
            {"invocation_id": InvocationId(invocation_id)}
        )
        if not doc:
            raise KeyError(f"Result for invocation {invocation_id} not found")
        # Unwrap from fake sub-document {"result": value}
        result_dict = retrieve_chunk_storage(
            self.cols.state_backend_chunks,
            build_chunk_key(
                invocation_id=invocation_id, prefix=ChunkPrefix.RESULT.value
            ),
            doc,
        )
        assert isinstance(result_dict, dict)
        return result_dict[ChunkPrefix.RESULT.value]

    def _set_result(
        self, invocation_id: "InvocationId", serialized_result: str
    ) -> None:
        """Store result, chunking if it exceeds BSON limits."""
        # Wrap in fake sub-document to use unified storage API
        result_storage = prepare_chunk_storage(
            self.cols.state_backend_chunks,
            build_chunk_key(
                invocation_id=invocation_id, prefix=ChunkPrefix.RESULT.value
            ),
            {ChunkPrefix.RESULT.value: serialized_result},
            self.conf.chunk_threshold,
        )
        self.cols.state_backend_results.insert_one(
            {"invocation_id": invocation_id, **result_storage}
        )

    def _get_exception(self, invocation_id: "InvocationId") -> str:
        """Retrieve exception, decompressing from chunks if needed."""
        doc = self.cols.state_backend_exceptions.find_one(
            {"invocation_id": invocation_id}
        )
        if not doc:
            raise KeyError(f"Exception for invocation {invocation_id} not found")
        # Unwrap from fake sub-document {"exception": value}
        exc_dict = retrieve_chunk_storage(
            self.cols.state_backend_chunks,
            build_chunk_key(
                invocation_id=invocation_id, prefix=ChunkPrefix.EXCEPTION.value
            ),
            doc,
        )
        assert isinstance(exc_dict, dict)
        return exc_dict[ChunkPrefix.EXCEPTION.value]

    def _set_exception(
        self, invocation_id: "InvocationId", serialized_exception: str
    ) -> None:
        """Store exception, chunking if it exceeds BSON limits."""
        # Wrap in fake sub-document to use unified storage API
        exc_storage = prepare_chunk_storage(
            self.cols.state_backend_chunks,
            build_chunk_key(
                invocation_id=invocation_id, prefix=ChunkPrefix.EXCEPTION.value
            ),
            {ChunkPrefix.EXCEPTION.value: serialized_exception},
            self.conf.chunk_threshold,
        )
        self.cols.state_backend_exceptions.insert_one(
            {"invocation_id": invocation_id, **exc_storage}
        )

    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """Set workflow data."""
        serialized_value = self.app.client_data_store.serialize(value)
        self.cols.state_backend_workflow_data.upsert_document(
            {"workflow_id": workflow_identity.workflow_id, "data_key": key},
            {
                "workflow_id": workflow_identity.workflow_id,
                "data_key": key,
                "data_value": serialized_value,
            },
        )

    def get_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, default: Any = None
    ) -> Any:
        """Get workflow data."""
        doc = self.cols.state_backend_workflow_data.find_one(
            {"workflow_id": workflow_identity.workflow_id, "data_key": key}
        )
        if doc:
            return self.app.client_data_store.deserialize(doc["data_value"])
        return default

    def get_all_workflow_types(self) -> Iterator["TaskId"]:
        """Retrieve all workflow types."""
        types = self.cols.state_backend_workflows.distinct("workflow_type_key")
        for workflow_type in types:
            yield TaskId.from_key(workflow_type)

    def get_all_workflow_runs(self) -> Iterator["WorkflowIdentity"]:
        """Retrieve all stored workflows."""
        docs = self.cols.state_backend_workflows.find({})
        for doc in docs:
            yield WorkflowIdentity(
                workflow_id=InvocationId(doc["workflow_id"]),
                workflow_type=TaskId.from_key(doc["workflow_type_key"]),
                parent_workflow_id=InvocationId(doc["parent_workflow_id"])
                if doc["parent_workflow_id"]
                else None,
            )

    def get_workflow_runs(
        self, workflow_type: "TaskId"
    ) -> Iterator["WorkflowIdentity"]:
        """Retrieve workflow runs for a specific workflow type."""
        docs = self.cols.state_backend_workflows.find(
            {"workflow_type_key": workflow_type.key}
        )
        for doc in docs:
            yield WorkflowIdentity(
                workflow_id=InvocationId(doc["workflow_id"]),
                workflow_type=TaskId.from_key(doc["workflow_type_key"]),
                parent_workflow_id=InvocationId(doc["parent_workflow_id"])
                if doc["parent_workflow_id"]
                else None,
            )

    def store_workflow_sub_invocation(
        self, parent_workflow_id: "InvocationId", sub_invocation_id: "InvocationId"
    ) -> None:
        """Store workflow sub-invocation relationship."""
        self.cols.state_backend_workflow_sub_invocations.insert_or_ignore(
            {
                "parent_workflow_id": parent_workflow_id,
                "sub_invocation_id": sub_invocation_id,
            }
        )

    def get_workflow_sub_invocations(
        self, workflow_id: "InvocationId"
    ) -> Iterator["InvocationId"]:
        """Get workflow sub-invocations."""
        docs = self.cols.state_backend_workflow_sub_invocations.find(
            {"parent_workflow_id": workflow_id}
        )
        for doc in docs:
            yield InvocationId(doc["sub_invocation_id"])

    def get_child_invocations(
        self, parent_invocation_id: "InvocationId"
    ) -> Iterator["InvocationId"]:
        """
        Return IDs of all invocations directly spawned by the given parent.

        Queries the invocations collection using the indexed ``parent_invocation_id``
        field for efficient family-tree traversal.

        :param parent_invocation_id: The invocation ID to find children for.
        :return: Iterator of child invocation IDs (may be empty).
        """
        docs = self.cols.state_backend_invocations.find(
            {"parent_invocation_id": parent_invocation_id}
        )
        for doc in docs:
            yield InvocationId(doc["invocation_id"])

    def _store_runner_context(self, runner_context: "RunnerContext") -> None:
        """
        Store a runner context in MongoDB.

        :param RunnerContext runner_context: The context to store
        """
        self.cols.state_backend_runner_contexts.insert_or_ignore(
            {
                "runner_id": runner_context.runner_id,
                "context_json": runner_context.to_json(),
            }
        )

    def _get_runner_context(self, runner_id: str) -> "RunnerContext | None":
        """
        Retrieve a runner context by runner_id from MongoDB.

        :param str runner_id: The runner's unique identifier
        :return: The stored RunnerContext or None if not found
        """
        doc = self.cols.state_backend_runner_contexts.find_one({"runner_id": runner_id})
        if doc:
            return RunnerContext.from_json(doc["context_json"])
        return None

    def _get_runner_contexts(self, runner_ids: list[str]) -> list["RunnerContext"]:
        """
        Retrieve multiple runner contexts by their IDs.

        :param list[str] runner_ids: List of runner unique identifiers
        :return: list["RunnerContext"] of the stored RunnerContexts
        """
        contexts: list[RunnerContext] = []
        docs = self.cols.state_backend_runner_contexts.find(
            {"runner_id": {"$in": runner_ids}}
        )
        for doc in docs:
            contexts.append(RunnerContext.from_json(doc["context_json"]))
        return contexts

    def purge(self) -> None:
        """Clear all state backend data including chunked storage."""
        self.cols.state_backend_chunks.delete_many({})
        self.cols.purge_all()
