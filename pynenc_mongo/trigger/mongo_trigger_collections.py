from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, DESCENDING, IndexModel

from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection


class TriggerCollections(MongoCollections):
    """Collections specific to MongoTrigger with prefix trg_."""

    def __init__(self, conf: "ConfigMongo", app_id: str) -> None:
        super().__init__(conf, prefix="trg_", app_id=app_id)

    @cached_property
    def trg_conditions(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_conditions",
            indexes=[IndexModel([("condition_id", ASCENDING)], unique=True)],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_triggers(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_triggers",
            indexes=[IndexModel([("trigger_id", ASCENDING)], unique=True)],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_condition_triggers(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_condition_triggers",
            indexes=[
                IndexModel(
                    [("condition_id", ASCENDING), ("trigger_id", ASCENDING)],
                    unique=True,
                ),
                IndexModel([("condition_id", ASCENDING)]),
                IndexModel([("trigger_id", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_valid_conditions(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_valid_conditions",
            indexes=[IndexModel([("valid_condition_id", ASCENDING)], unique=True)],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_source_task_conditions(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_source_task_conditions",
            indexes=[
                IndexModel(
                    [("task_id_key", ASCENDING), ("condition_id", ASCENDING)],
                    unique=True,
                ),
                IndexModel([("task_id_key", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_execution_claims(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_execution_claims",
            indexes=[
                IndexModel([("claim_key", ASCENDING)], unique=True),
                IndexModel([("expiration", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_trigger_run_claims(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_trigger_run_claims",
            indexes=[
                IndexModel([("trigger_run_id", ASCENDING)], unique=True),
                IndexModel([("expiration", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_events(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_events",
            indexes=[
                IndexModel([("event_id", ASCENDING)], unique=True),
                IndexModel([("timestamp", DESCENDING)]),
                IndexModel([("event_code", ASCENDING), ("timestamp", DESCENDING)]),
                IndexModel([("matched", ASCENDING)]),
                IndexModel([("triggered", ASCENDING)]),
                IndexModel([("emitted_by_invocation_id", ASCENDING)]),
                IndexModel([("emitted_by_task_id", ASCENDING)]),
                IndexModel([("triggered_invocation_ids", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)

    @cached_property
    def trg_trigger_runs(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="trg_trigger_runs",
            indexes=[
                IndexModel([("trigger_run_id", ASCENDING)], unique=True),
                IndexModel([("sort_time", DESCENDING)]),
                IndexModel([("event_ids", ASCENDING)]),
                IndexModel([("event_codes", ASCENDING), ("sort_time", DESCENDING)]),
                IndexModel([("source_invocation_ids", ASCENDING)]),
                IndexModel([("triggered_invocation_id", ASCENDING)]),
                IndexModel([("valid_condition_ids", ASCENDING)]),
                IndexModel([("task_id_key", ASCENDING), ("sort_time", DESCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)
