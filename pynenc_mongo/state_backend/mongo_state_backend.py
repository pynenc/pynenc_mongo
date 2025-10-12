from collections.abc import Iterator
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError
from pynenc.app_info import AppInfo
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.state_backend.base_state_backend import BaseStateBackend, InvocationHistory
from pynenc.types import Params, Result
from pynenc.workflow import WorkflowIdentity

from pynenc_mongo.conf.config_state_backend import ConfigStateBackendMongo
from pynenc_mongo.state_backend.mongo_state_backend_collections import (
    StateBackendCollections,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc


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
        self.app.logger.warning(
            f"Using MongoDB database {self.conf.mongo_db} for state backend."
        )

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
                "workflow_type": w_id.workflow_type,
                "workflow_json": w_id.to_json(),
            }
        )

    def _upsert_invocations(self, invocations: list["DistributedInvocation"]) -> None:
        """Updates or inserts multiple invocations."""
        for invocation in invocations:
            self.cols.state_backend_invocations.insert_or_ignore(
                {
                    "invocation_id": invocation.invocation_id,
                    "invocation_json": invocation.to_json(),
                }
            )

    def _get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        """Retrieves an invocation by its ID."""
        doc = self.cols.state_backend_invocations.find_one(
            {"invocation_id": invocation_id}
        )
        if doc:
            return DistributedInvocation.from_json(self.app, doc["invocation_json"])
        return None

    def _add_histories(
        self, invocation_ids: list[str], invocation_history: "InvocationHistory"
    ) -> None:
        """Adds the same history record for a list of invocations."""
        for invocation_id in invocation_ids:
            try:
                self.cols.state_backend_history.insert_or_ignore(
                    {
                        "invocation_id": invocation_id,
                        "history_timestamp": invocation_history._timestamp,
                        "history_status": invocation_history.status,
                        "history_json": invocation_history.to_json(),
                    }
                )
            except DuplicateKeyError as e:
                self.app.logger.debug(
                    f"Error adding {invocation_history=} already exists: {e}"
                )

    def _get_history(self, invocation_id: str) -> list["InvocationHistory"]:
        """Retrieves the history of an invocation ordered by timestamp."""
        docs = self.cols.state_backend_history.find(
            {"invocation_id": invocation_id}
        ).sort("history_timestamp", ASCENDING)
        return [InvocationHistory.from_json(doc["history_json"]) for doc in docs]

    def _get_result(self, invocation_id: str) -> Result:
        """Retrieves the result of an invocation by ID."""
        doc = self.cols.state_backend_results.find_one({"invocation_id": invocation_id})
        if doc:
            return self.app.serializer.deserialize(doc["result_data"])
        raise KeyError(f"Result for invocation {invocation_id} not found")

    def _set_result(self, invocation_id: str, result: Result) -> None:
        """Sets the result of an invocation by ID."""
        self.cols.state_backend_results.insert_one(
            {
                "invocation_id": invocation_id,
                "result_data": self.app.serializer.serialize(result),
            }
        )

    def _get_exception(self, invocation_id: str) -> Exception:
        """Retrieves the exception of an invocation by ID."""
        doc = self.cols.state_backend_exceptions.find_one(
            {"invocation_id": invocation_id}
        )
        if doc:
            return self.deserialize_exception(doc["exception_data"])
        raise KeyError(f"Exception for invocation {invocation_id} not found")

    def _set_exception(self, invocation_id: str, exception: Exception) -> None:
        """Sets the raised exception by invocation ID."""
        self.cols.state_backend_exceptions.insert_one(
            {
                "invocation_id": invocation_id,
                "exception_data": self.serialize_exception(exception),
            }
        )

    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """Set workflow data."""
        serialized_value = self.app.serializer.serialize(value)
        self.cols.state_backend_workflow_data.upsert_document(
            {"workflow_id": workflow_identity.workflow_invocation_id, "data_key": key},
            {
                "workflow_id": workflow_identity.workflow_invocation_id,
                "data_key": key,
                "data_value": serialized_value,
            },
        )

    def get_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, default: Any = None
    ) -> Any:
        """Get workflow data."""
        doc = self.cols.state_backend_workflow_data.find_one(
            {"workflow_id": workflow_identity.workflow_invocation_id, "data_key": key}
        )
        if doc:
            return self.app.serializer.deserialize(doc["data_value"])
        return default

    def get_all_workflow_types(self) -> Iterator[str]:
        """Retrieve all workflow types."""
        types = self.cols.state_backend_workflows.distinct("workflow_type")
        yield from types

    def get_all_workflow_runs(self) -> Iterator["WorkflowIdentity"]:
        """Retrieve all stored workflows."""
        docs = self.cols.state_backend_workflows.find({})
        for doc in docs:
            yield WorkflowIdentity.from_json(doc["workflow_json"])

    def get_workflow_runs(self, workflow_type: str) -> Iterator["WorkflowIdentity"]:
        """Retrieve workflow runs for a specific workflow type."""
        docs = self.cols.state_backend_workflows.find({"workflow_type": workflow_type})
        for doc in docs:
            yield WorkflowIdentity.from_json(doc["workflow_json"])

    def store_workflow_sub_invocation(
        self, parent_workflow_id: str, sub_invocation_id: str
    ) -> None:
        """Store workflow sub-invocation relationship."""
        self.cols.state_backend_workflow_sub_invocations.insert_or_ignore(
            {
                "parent_workflow_id": parent_workflow_id,
                "sub_invocation_id": sub_invocation_id,
            }
        )

    def get_workflow_sub_invocations(self, workflow_id: str) -> Iterator[str]:
        """Get workflow sub-invocations."""
        docs = self.cols.state_backend_workflow_sub_invocations.find(
            {"parent_workflow_id": workflow_id}
        )
        for doc in docs:
            yield doc["sub_invocation_id"]

    def purge(self) -> None:
        """Clear all state backend data."""
        self.cols.purge_all()
