from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel

from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection


class OrchestratorCollections(MongoCollections):
    """Collections specific to MongoOrchestrator with prefix orchestrator_."""

    def __init__(self, conf: "ConfigMongo"):
        super().__init__(conf, prefix="orchestrator_")

    @cached_property
    def orchestrator_invocations(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="orchestrator_invocations",
            indexes=[
                IndexModel([("invocation_id", ASCENDING)], unique=True),
                IndexModel([("task_id", ASCENDING)]),
                IndexModel([("call_id", ASCENDING)]),
                IndexModel([("status", ASCENDING)]),
                IndexModel([("auto_purge_timestamp", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def orchestrator_invocation_args(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="orchestrator_invocation_args",
            indexes=[
                IndexModel(
                    [("invocation_id", ASCENDING), ("arg_key", ASCENDING)], unique=True
                ),
                IndexModel([("arg_key", ASCENDING), ("arg_value", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def orchestrator_cycle_calls(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="orchestrator_cycle_calls",
            indexes=[
                IndexModel([("call_id", ASCENDING)], unique=True),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def orchestrator_cycle_edges(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="orchestrator_cycle_edges",
            indexes=[
                IndexModel(
                    [("caller_id", ASCENDING), ("callee_id", ASCENDING)], unique=True
                ),
                IndexModel([("callee_id", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def orchestrator_blocking_edges(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="orchestrator_blocking_edges",
            indexes=[
                IndexModel(
                    [("waiter_id", ASCENDING), ("waited_id", ASCENDING)], unique=True
                ),
                IndexModel([("waited_id", ASCENDING)]),
                IndexModel([("waiter_id", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)
