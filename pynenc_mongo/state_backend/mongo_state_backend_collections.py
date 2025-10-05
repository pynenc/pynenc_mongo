from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel

from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection


class StateBackendCollections(MongoCollections):
    """Collections specific to MongoStateBackend with prefix state_backend_."""

    def __init__(self, conf: "ConfigMongo"):
        super().__init__(conf, prefix="state_backend_")

    @cached_property
    def state_backend_results(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_results",
            indexes=[
                IndexModel([("invocation_id", ASCENDING)], unique=True),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_exceptions(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_exceptions",
            indexes=[
                IndexModel([("invocation_id", ASCENDING)], unique=True),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_invocations(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_invocations",
            indexes=[
                IndexModel([("invocation_id", ASCENDING)], unique=True),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_history(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_history",
            indexes=[
                IndexModel(
                    [
                        ("invocation_id", ASCENDING),
                        ("history_timestamp", ASCENDING),
                        ("history_status", ASCENDING),
                    ],
                    unique=True,
                ),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_workflows(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_workflows",
            indexes=[
                IndexModel([("workflow_id", ASCENDING)], unique=True),
                IndexModel([("workflow_type", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_app_info(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_app_info",
            indexes=[
                IndexModel([("app_id", ASCENDING)], unique=True),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_workflow_data(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_workflow_data",
            indexes=[
                IndexModel(
                    [("workflow_id", ASCENDING), ("data_key", ASCENDING)], unique=True
                ),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def state_backend_workflow_sub_invocations(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="state_backend_workflow_sub_invocations",
            indexes=[
                IndexModel(
                    [
                        ("parent_workflow_id", ASCENDING),
                        ("sub_invocation_id", ASCENDING),
                    ],
                    unique=True,
                ),
            ],
        )
        return self.instantiate_retriable_coll(spec)
