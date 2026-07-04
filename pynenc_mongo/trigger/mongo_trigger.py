from collections.abc import Iterable
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

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
from pynenc.trigger.monitoring import (
    EventMarker,
    EventMarkerPage,
    EventRecord,
    TriggerRunRecord,
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
        self.cols = TriggerCollections(self.conf, app_id=self.app.app_id)

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

    def _get_trigger(self, trigger_id: str) -> "TriggerDefinitionDTO | None":
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
        # Resolve trigger ids through the condition->trigger index then load
        # the trigger documents in one query, instead of scanning the
        # ``condition_ids`` array on every trigger document.
        trigger_ids = [
            doc["trigger_id"]
            for doc in self.cols.trg_condition_triggers.find(
                {"condition_id": condition_id}, {"trigger_id": 1}
            )
        ]
        if not trigger_ids:
            return []
        trigger_docs = list(
            self.cols.trg_triggers.find({"trigger_id": {"$in": trigger_ids}})
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
            {"task_id_key": task_id.key}, {"trigger_id": 1}
        )
        trigger_ids = [doc["trigger_id"] for doc in trigger_docs]
        if trigger_ids:
            self.cols.trg_triggers.delete_many({"trigger_id": {"$in": trigger_ids}})
            self.cols.trg_condition_triggers.delete_many(
                {"trigger_id": {"$in": trigger_ids}}
            )

    def _purge(self) -> None:
        self.cols.purge_all()

    # ── Monitoring API (events + trigger runs) ─────────────────────────
    @staticmethod
    def _to_aware_utc(value: datetime) -> datetime:
        """Return ``value`` as a UTC-aware datetime."""
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def _event_to_doc(self, event: EventRecord) -> dict[str, Any]:
        """Serialize an ``EventRecord`` into a MongoDB document."""
        return {
            "event_id": event.event_id,
            "event_code": event.event_code,
            "timestamp": self._to_aware_utc(event.timestamp),
            "matched": event.matched,
            "triggered": event.triggered,
            "triggered_invocation_ids": list(event.triggered_invocation_ids),
            "emitted_by_invocation_id": event.emitted_by_invocation_id,
            "emitted_by_task_id": event.emitted_by_task_id,
            "emitted_by_runner_context_id": event.emitted_by_runner_context_id,
            "payload_json": event.to_json(self.app),
        }

    def store_event(self, event: EventRecord) -> None:
        """Persist or replace a single event document."""
        self.cols.trg_events.replace_one(
            {"event_id": event.event_id},
            self._event_to_doc(event),
            upsert=True,
        )

    def get_event(self, event_id: str) -> "EventRecord | None":
        doc = self.cols.trg_events.find_one({"event_id": event_id})
        if not doc:
            return None
        return self._event_from_doc(doc)

    def _event_from_doc(self, doc: dict[str, Any]) -> EventRecord:
        """Deserialize an event and hydrate backend-indexed relations."""
        record = EventRecord.from_json(doc["payload_json"], self.app)
        record.triggered_invocation_ids = list(
            doc.get("triggered_invocation_ids") or []
        )
        return record

    def get_events(
        self,
        *,
        event_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        matched: bool | None = None,
        triggered: bool | None = None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EventRecord]:
        query = self._build_event_query(
            event_code,
            start_time,
            end_time,
            matched,
            triggered,
            emitted_by_invocation_id,
            emitted_by_task_id,
        )
        cursor = (
            self.cols.trg_events.find(query)
            .sort("timestamp", -1)
            .skip(offset)
            .limit(limit)
        )
        return [self._event_from_doc(doc) for doc in cursor]

    def count_events(
        self,
        *,
        event_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        matched: bool | None = None,
        triggered: bool | None = None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
    ) -> int:
        query = self._build_event_query(
            event_code,
            start_time,
            end_time,
            matched,
            triggered,
            emitted_by_invocation_id,
            emitted_by_task_id,
        )
        return self.cols.trg_events.count_documents(query)

    def _build_event_query(
        self,
        event_code: str | None,
        start_time: datetime | None,
        end_time: datetime | None,
        matched: bool | None,
        triggered: bool | None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
    ) -> dict[str, Any]:
        """Translate event filters into a MongoDB query document."""
        query: dict[str, Any] = {}
        if event_code is not None:
            query["event_code"] = event_code
        ts_clause: dict[str, datetime] = {}
        if start_time is not None:
            ts_clause["$gte"] = self._to_aware_utc(start_time)
        if end_time is not None:
            ts_clause["$lte"] = self._to_aware_utc(end_time)
        if ts_clause:
            query["timestamp"] = ts_clause
        if matched is not None:
            query["matched"] = matched
        if triggered is not None:
            query["triggered"] = triggered
        if emitted_by_invocation_id is not None:
            query["emitted_by_invocation_id"] = emitted_by_invocation_id
        if emitted_by_task_id is not None:
            query["emitted_by_task_id"] = emitted_by_task_id
        return query

    def list_event_codes(self) -> list[str]:
        return sorted(self.cols.trg_events.distinct("event_code"))

    def get_event_markers_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        state: str = "all",
        limit: int = 1000,
        offset: int = 0,
    ) -> EventMarkerPage:
        query = self._build_event_query(
            event_code,
            start_time,
            end_time,
            matched=None,
            triggered=None,
        )
        if state == "matched":
            query["matched"] = True
        elif state == "unmatched":
            query["matched"] = False
        elif state == "triggered":
            query["triggered"] = True
        elif state == "untriggered":
            query["triggered"] = False
        total = self.cols.trg_events.count_documents(query)
        cursor = (
            self.cols.trg_events.find(
                query,
                {
                    "event_id": 1,
                    "event_code": 1,
                    "timestamp": 1,
                    "matched": 1,
                    "triggered": 1,
                    "emitted_by_invocation_id": 1,
                    "emitted_by_runner_context_id": 1,
                    "payload_json": 1,
                },
            )
            .sort("timestamp", 1)
            .skip(offset)
            .limit(limit)
        )
        markers = [
            EventMarker(
                event_id=doc["event_id"],
                event_code=doc["event_code"],
                timestamp=self._to_aware_utc(doc["timestamp"]),
                matched=bool(doc.get("matched")),
                triggered=bool(doc.get("triggered")),
                emitted_by_invocation_id=doc.get("emitted_by_invocation_id"),
                emitted_by_runner_context_id=doc.get("emitted_by_runner_context_id"),
            )
            for doc in cursor
        ]
        return EventMarkerPage(
            markers=markers,
            total=total,
            truncated=offset + len(markers) < total,
        )

    def link_trigger_run_to_events(
        self,
        event_ids: list[str],
        invocation_id: str,
        *,
        trigger_run_id: str,
    ) -> None:
        if not event_ids:
            return
        self.cols.trg_events.update_many(
            {"event_id": {"$in": event_ids}},
            {
                "$addToSet": {"triggered_invocation_ids": invocation_id},
                "$set": {"triggered": True},
            },
        )

    def get_invocations_triggered_by_event(self, event_id: str) -> list[str]:
        doc = self.cols.trg_events.find_one(
            {"event_id": event_id}, {"triggered_invocation_ids": 1}
        )
        if not doc:
            return []
        return list(doc.get("triggered_invocation_ids") or [])

    def _run_to_doc(self, run: TriggerRunRecord) -> dict[str, Any]:
        """Serialize a ``TriggerRunRecord`` into a MongoDB document."""
        sort_time = run.executed_at or run.claimed_at or datetime.now(UTC)
        event_codes = self._event_codes_for(run.event_ids)
        valid_condition_ids = self._valid_condition_ids_for_run(run)
        return {
            "trigger_run_id": run.trigger_run_id,
            "trigger_id": run.trigger_id,
            "task_id_key": run.task_id_key,
            "logic_value": run.logic_value,
            "event_ids": list(run.event_ids),
            "event_codes": event_codes,
            "source_invocation_ids": list(run.source_invocation_ids),
            "valid_condition_ids": sorted(valid_condition_ids),
            "triggered_invocation_id": run.triggered_invocation_id,
            "sort_time": self._to_aware_utc(sort_time),
            "payload_json": run.to_json(),
        }

    @staticmethod
    def _valid_condition_ids_for_run(run: TriggerRunRecord) -> set[str]:
        ids = set(run.valid_condition_ids)
        for participant in run.participants or []:
            if participant.valid_condition_id:
                ids.add(participant.valid_condition_id)
        return ids

    def _event_codes_for(self, event_ids: Iterable[str]) -> list[str]:
        """Return the distinct event codes for the given event ids."""
        ids = list(event_ids)
        if not ids:
            return []
        codes = self.cols.trg_events.distinct("event_code", {"event_id": {"$in": ids}})
        return sorted(codes)

    def store_trigger_run(self, run: TriggerRunRecord) -> None:
        self.cols.trg_trigger_runs.replace_one(
            {"trigger_run_id": run.trigger_run_id},
            self._run_to_doc(run),
            upsert=True,
        )

    def get_trigger_run(self, trigger_run_id: str) -> "TriggerRunRecord | None":
        doc = self.cols.trg_trigger_runs.find_one({"trigger_run_id": trigger_run_id})
        if not doc:
            return None
        return TriggerRunRecord.from_json(doc["payload_json"])

    def get_trigger_runs_for_event(self, event_id: str) -> list[TriggerRunRecord]:
        cursor = self.cols.trg_trigger_runs.find({"event_ids": event_id}).sort(
            "sort_time", -1
        )
        return [TriggerRunRecord.from_json(doc["payload_json"]) for doc in cursor]

    def get_trigger_runs_for_invocation(
        self, invocation_id: str
    ) -> list[TriggerRunRecord]:
        cursor = self.cols.trg_trigger_runs.find(
            {"triggered_invocation_id": invocation_id}
        ).sort("sort_time", -1)
        return [TriggerRunRecord.from_json(doc["payload_json"]) for doc in cursor]

    def get_trigger_runs_sourced_by_invocation(
        self, invocation_id: str
    ) -> list[TriggerRunRecord]:
        cursor = self.cols.trg_trigger_runs.find(
            {"source_invocation_ids": invocation_id}
        ).sort("sort_time", -1)
        return [TriggerRunRecord.from_json(doc["payload_json"]) for doc in cursor]

    def get_trigger_runs_for_valid_condition(
        self, valid_condition_id: str
    ) -> list[TriggerRunRecord]:
        cursor = self.cols.trg_trigger_runs.find(
            {"valid_condition_ids": valid_condition_id}
        ).sort("sort_time", -1)
        return [TriggerRunRecord.from_json(doc["payload_json"]) for doc in cursor]

    def get_trigger_runs_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        task_id_key: str | None = None,
        limit: int | None = None,
    ) -> list[TriggerRunRecord]:
        query: dict[str, Any] = {
            "sort_time": {
                "$gte": self._to_aware_utc(start_time),
                "$lte": self._to_aware_utc(end_time),
            }
        }
        if task_id_key is not None:
            query["task_id_key"] = task_id_key
        if event_code is not None:
            query["event_codes"] = event_code
        cursor = self.cols.trg_trigger_runs.find(query).sort("sort_time", -1)
        if limit is not None:
            cursor = cursor.limit(limit)
        return [TriggerRunRecord.from_json(doc["payload_json"]) for doc in cursor]

    # ── Auto-purge (events + trigger runs) ─────────────────────────────
    # The driving algorithm lives in BaseTrigger._auto_purge_events; this
    # class supplies the Mongo primitives.

    def _cascade_delete_runs_for_events(self, event_ids: list[str]) -> int:
        """Delete trigger runs that reference any of ``event_ids``."""
        if not event_ids:
            return 0
        result = self.cols.trg_trigger_runs.delete_many(
            {"event_ids": {"$in": list(set(event_ids))}}
        )
        return int(getattr(result, "deleted_count", 0) or 0)

    def _age_purge_events(self, threshold: datetime) -> list[str]:
        ids = [
            doc["event_id"]
            for doc in self.cols.trg_events.find(
                {"timestamp": {"$lt": threshold}}, {"event_id": 1}
            )
        ]
        if not ids:
            return []
        self.cols.trg_events.delete_many({"event_id": {"$in": ids}})
        return ids

    def _cap_purge_events(self) -> list[str]:
        max_records = self.conf.event_max_records
        if max_records <= 0:
            return []
        total = self.cols.trg_events.count_documents({})
        excess = total - max_records
        if excess <= 0:
            return []
        old_ids = [
            doc["event_id"]
            for doc in self.cols.trg_events.find({}, {"event_id": 1})
            .sort("timestamp", 1)
            .limit(excess)
        ]
        if not old_ids:
            return []
        self.cols.trg_events.delete_many({"event_id": {"$in": old_ids}})
        return old_ids

    def _age_purge_trigger_runs(self, threshold: datetime) -> int:
        result = self.cols.trg_trigger_runs.delete_many(
            {"sort_time": {"$lt": threshold}}
        )
        return int(getattr(result, "deleted_count", 0) or 0)

    def _cap_purge_trigger_runs(self) -> int:
        max_records = self.conf.trigger_run_max_records
        if max_records <= 0:
            return 0
        total = self.cols.trg_trigger_runs.count_documents({})
        excess = total - max_records
        if excess <= 0:
            return 0
        old_ids = [
            doc["trigger_run_id"]
            for doc in self.cols.trg_trigger_runs.find({}, {"trigger_run_id": 1})
            .sort("sort_time", 1)
            .limit(excess)
        ]
        if not old_ids:
            return 0
        result = self.cols.trg_trigger_runs.delete_many(
            {"trigger_run_id": {"$in": old_ids}}
        )
        return int(getattr(result, "deleted_count", 0) or 0)
