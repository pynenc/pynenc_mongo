from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Optional

from pymongo.errors import DuplicateKeyError
from pymongo.operations import ReplaceOne
from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import ConditionContext, TriggerCondition, ValidCondition

from pynenc_mongo.conf.config_trigger import ConfigTriggerMongo
from pynenc_mongo.trigger.mongo_trigger_collections import TriggerCollections

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.trigger_definitions import TriggerDefinition


class MongoTrigger(BaseTrigger):
    """
    MongoDB-based implementation of the Pynenc trigger system.

    Stores all trigger, condition, and claim data in MongoDB for cross-process safety.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = TriggerCollections(self.conf)

    @cached_property
    def conf(self) -> ConfigTriggerMongo:
        return ConfigTriggerMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _register_condition(self, condition: TriggerCondition) -> None:
        self.cols.trg_conditions.replace_one(
            {"condition_id": condition.condition_id},
            {
                "condition_id": condition.condition_id,
                "condition_json": condition.to_json(self.app),
                "last_cron_execution": None,
            },
            upsert=True,
        )

    def get_condition(self, condition_id: str) -> TriggerCondition | None:
        doc = self.cols.trg_conditions.find_one({"condition_id": condition_id})
        if doc:
            return TriggerCondition.from_json(doc["condition_json"], self.app)
        return None

    def register_trigger(self, trigger: "TriggerDefinition") -> None:
        self.cols.trg_triggers.insert_or_ignore(
            {
                "trigger_id": trigger.trigger_id,
                "trigger_json": trigger.to_json(self.app),
            }
        )
        for condition_id in trigger.condition_ids:
            self.cols.trg_condition_triggers.insert_or_ignore(
                {"condition_id": condition_id, "trigger_id": trigger.trigger_id}
            )

    def get_trigger(self, trigger_id: str) -> Optional["TriggerDefinition"]:
        doc = self.cols.trg_triggers.find_one({"trigger_id": trigger_id})
        if doc:
            from pynenc.trigger.trigger_definitions import TriggerDefinition

            return TriggerDefinition.from_json(doc["trigger_json"], self.app)
        return None

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinition"]:
        trigger_docs = list(
            self.cols.trg_condition_triggers.find({"condition_id": condition_id})
        )
        triggers = []
        for doc in trigger_docs:
            trigger = self.get_trigger(doc["trigger_id"])
            if trigger:
                triggers.append(trigger)
            else:
                self.app.logger.warning(
                    f"Trigger {doc['trigger_id']} not found for condition {condition_id}"
                )
        return triggers

    def record_valid_condition(self, valid_condition: ValidCondition) -> None:
        self.cols.trg_valid_conditions.insert_or_ignore(
            {
                "valid_condition_id": valid_condition.valid_condition_id,
                "valid_condition_json": valid_condition.to_json(self.app),
            }
        )

    def record_valid_conditions(self, valid_conditions: list[ValidCondition]) -> None:
        if not valid_conditions:
            return
        bulk_ops = [
            ReplaceOne(
                {"valid_condition_id": vc.valid_condition_id},
                {
                    "valid_condition_id": vc.valid_condition_id,
                    "valid_condition_json": vc.to_json(self.app),
                },
                upsert=True,
            )
            for vc in valid_conditions
        ]
        self.cols.trg_valid_conditions.bulk_write(bulk_ops)

    def get_valid_conditions(self) -> dict[str, ValidCondition]:
        conditions = {}
        for doc in self.cols.trg_valid_conditions.find():
            vc = ValidCondition.from_json(doc["valid_condition_json"], self.app)
            conditions[doc["valid_condition_id"]] = vc
        return conditions

    def clear_valid_conditions(self, conditions: Iterable[ValidCondition]) -> None:
        ids_to_delete = [c.valid_condition_id for c in conditions]
        if ids_to_delete:
            self.cols.trg_valid_conditions.delete_many(
                {"valid_condition_id": {"$in": ids_to_delete}}
            )

    def _get_all_conditions(self) -> list[TriggerCondition]:
        conditions = []
        for doc in self.cols.trg_conditions.find():
            conditions.append(
                TriggerCondition.from_json(doc["condition_json"], self.app)
            )
        return conditions

    def get_last_cron_execution(self, condition_id: str) -> datetime | None:
        doc = self.cols.trg_conditions.find_one({"condition_id": condition_id})
        if doc and doc.get("last_cron_execution"):
            return doc["last_cron_execution"]
        return None

    def store_last_cron_execution(
        self,
        condition_id: str,
        execution_time: datetime,
        expected_last_execution: datetime | None = None,
    ) -> bool:
        filter_doc: dict = {"condition_id": condition_id}
        if expected_last_execution is not None:
            filter_doc["last_cron_execution"] = expected_last_execution
        else:
            filter_doc["$or"] = [
                {"last_cron_execution": None},
                {"last_cron_execution": {"$exists": False}},
            ]
        result = self.cols.trg_conditions.update_one(
            filter_doc, {"$set": {"last_cron_execution": execution_time}}
        )
        return result.modified_count > 0

    def _register_source_task_condition(self, task_id: str, condition_id: str) -> None:
        self.cols.trg_source_task_conditions.insert_or_ignore(
            {"task_id": task_id, "condition_id": condition_id}
        )

    def get_conditions_sourced_from_task(
        self, task_id: str, context_type: type[ConditionContext] | None = None
    ) -> list[TriggerCondition]:
        condition_ids = [
            doc["condition_id"]
            for doc in self.cols.trg_source_task_conditions.find({"task_id": task_id})
        ]
        conditions = [self.get_condition(cid) for cid in condition_ids]
        conditions = [c for c in conditions if c]
        if context_type is not None:
            conditions = [c for c in conditions if c.context_type == context_type]
        return conditions

    def claim_trigger_execution(
        self, trigger_id: str, valid_condition_id: str, expiration_seconds: int = 60
    ) -> bool:
        claim_key = f"{trigger_id}:{valid_condition_id}"
        now = datetime.now(UTC)
        expiration = now + timedelta(seconds=expiration_seconds)

        try:
            self.cols.trg_execution_claims._collection.find_one_and_update(
                {
                    "claim_key": claim_key,
                    "$or": [
                        {"expiration": {"$lte": now}},
                        {"expiration": {"$exists": False}},
                    ],
                },
                {"$set": {"expiration": expiration, "claimed_at": now}},
                upsert=True,
            )
            return True
        except DuplicateKeyError:
            # Another worker claimed it concurrently
            return False
        except Exception as e:
            # Log other errors but treat as claim failure
            self.app.logger.error(f"Claim failed for {claim_key}: {e}")
            return False

    def claim_trigger_run(
        self, trigger_run_id: str, expiration_seconds: int = 60
    ) -> bool:
        now = datetime.now(UTC)
        expiration = now + timedelta(seconds=expiration_seconds)

        try:
            self.cols.trg_trigger_run_claims._collection.find_one_and_update(
                {
                    "trigger_run_id": trigger_run_id,
                    "$or": [
                        {"expiration": {"$lte": now}},
                        {"expiration": {"$exists": False}},
                    ],
                },
                {"$set": {"expiration": expiration, "claimed_at": now}},
                upsert=True,
            )
            return True
        except DuplicateKeyError:
            return False
        except Exception as e:
            self.app.logger.error(f"Claim failed for {trigger_run_id}: {e}")
            return False

    def clean_task_trigger_definitions(self, task_id: str) -> None:
        trigger_docs = self.cols.trg_triggers.find(
            {"trigger_json": {"$regex": f'"task_id": "{task_id}"'}}
        )
        trigger_ids = [doc["trigger_id"] for doc in trigger_docs]
        if trigger_ids:
            self.cols.trg_triggers.delete_many({"trigger_id": {"$in": trigger_ids}})
            self.cols.trg_condition_triggers.delete_many(
                {"trigger_id": {"$in": trigger_ids}}
            )

    def _purge(self) -> None:
        self.cols.purge_all()
