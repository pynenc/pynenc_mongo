from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pymongo.errors import DuplicateKeyError
from pymongo.operations import ReplaceOne
from pynenc.identifiers.task_id import TaskId
from pynenc.models.trigger_definition_dto import TriggerDefinitionDTO
from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import (
    CompositeLogic,
    ConditionContext,
    TriggerCondition,
    ValidCondition,
)

from pynenc_mongo.conf.config_trigger import ConfigTriggerMongo
from pynenc_mongo.trigger.mongo_trigger_collections import TriggerCollections

if TYPE_CHECKING:
    from pynenc.app import Pynenc


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

    def register_trigger(self, trigger: "TriggerDefinitionDTO") -> None:
        self.cols.trg_triggers.insert_or_ignore(
            {
                "trigger_id": trigger.trigger_id,
                "task_id_key": trigger.task_id.key,
                "condition_ids": trigger.condition_ids,
                "logic_value": trigger.logic.value,
                "argument_provider_json": trigger.argument_provider_json,
            }
        )
        for condition_id in trigger.condition_ids:
            self.cols.trg_condition_triggers.insert_or_ignore(
                {"condition_id": condition_id, "trigger_id": trigger.trigger_id}
            )

    def _get_trigger(self, trigger_id: str) -> Optional["TriggerDefinitionDTO"]:
        doc = self.cols.trg_triggers.find_one({"trigger_id": trigger_id})
        if doc:
            return self._parse_trigger_dto(doc)
        return None

    def _parse_trigger_dto(
        self, trigger_dict: dict[str, Any]
    ) -> "TriggerDefinitionDTO":
        return TriggerDefinitionDTO(
            trigger_id=trigger_dict["trigger_id"],
            task_id=TaskId.from_key(trigger_dict["task_id_key"]),
            condition_ids=trigger_dict["condition_ids"],
            logic=CompositeLogic(trigger_dict["logic_value"]),
            argument_provider_json=trigger_dict.get("argument_provider_json"),
        )

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinitionDTO"]:
        trigger_docs = list(
            self.cols.trg_triggers.find({"condition_ids": condition_id})
        )
        return [self._parse_trigger_dto(doc) for doc in trigger_docs]

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
        """
        Get the last execution time for a cron condition.

        :param condition_id: ID of the condition to check
        :return: Last execution time in UTC, or None if never executed
        """
        doc = self.cols.trg_conditions.find_one({"condition_id": condition_id})
        if doc and doc.get("last_cron_execution"):
            dt = doc["last_cron_execution"]
            # Ensure datetime is UTC-aware
            if dt.tzinfo is None:
                # Naive datetime - assume it's UTC and make it aware
                return dt.replace(tzinfo=UTC)
            else:
                # Already aware - convert to UTC
                return dt.astimezone(UTC)
        return None

    def store_last_cron_execution(
        self,
        condition_id: str,
        execution_time: datetime,
        expected_last_execution: datetime | None = None,
    ) -> bool:
        """
        Store the last execution time for a cron condition with optimistic locking.

        :param condition_id: ID of the condition
        :param execution_time: Time of execution in UTC
        :param expected_last_execution: Expected current value for optimistic locking
        :return: True if update succeeded, False if another process won the race
        """
        filter_doc: dict = {"condition_id": condition_id}
        if expected_last_execution is not None:
            # Ensure expected_last_execution is UTC-aware for comparison
            if expected_last_execution.tzinfo is None:
                expected_last_execution = expected_last_execution.replace(tzinfo=UTC)
            else:
                expected_last_execution = expected_last_execution.astimezone(UTC)
            filter_doc["last_cron_execution"] = expected_last_execution
        else:
            filter_doc["$or"] = [
                {"last_cron_execution": None},
                {"last_cron_execution": {"$exists": False}},
            ]

        # Ensure execution_time is UTC-aware
        if execution_time.tzinfo is None:
            execution_time = execution_time.replace(tzinfo=UTC)
        else:
            execution_time = execution_time.astimezone(UTC)

        result = self.cols.trg_conditions.update_one(
            filter_doc, {"$set": {"last_cron_execution": execution_time}}
        )
        return result.modified_count > 0

    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: str
    ) -> None:
        self.cols.trg_source_task_conditions.insert_or_ignore(
            {"task_id_key": task_id.key, "condition_id": condition_id}
        )

    def get_conditions_sourced_from_task(
        self, task_id: "TaskId", context_type: type[ConditionContext] | None = None
    ) -> list[TriggerCondition]:
        condition_ids = [
            doc["condition_id"]
            for doc in self.cols.trg_source_task_conditions.find(
                {"task_id_key": task_id.key}
            )
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

    def clean_task_trigger_definitions(self, task_id: "TaskId") -> None:
        trigger_docs = self.cols.trg_triggers.find(
            {"trigger_json": {"$regex": f'"task_id_key": "{task_id.key}"'}}
        )
        trigger_ids = [doc["trigger_id"] for doc in trigger_docs]
        if trigger_ids:
            self.cols.trg_triggers.delete_many({"trigger_id": {"$in": trigger_ids}})
            self.cols.trg_condition_triggers.delete_many(
                {"trigger_id": {"$in": trigger_ids}}
            )

    def _purge(self) -> None:
        self.cols.purge_all()
