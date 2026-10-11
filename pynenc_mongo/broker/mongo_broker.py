from collections.abc import Sequence
from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, DESCENDING, IndexModel
from pynenc.broker.base_broker import BaseBroker
from pynenc.identifiers.invocation_id import InvocationId

from pynenc_mongo.conf.config_broker import ConfigBrokerMongo
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc.app import Pynenc

    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection


class BrokerCollections(MongoCollections):
    """MongoDB collections for the broker message queue."""

    def __init__(self, conf: "ConfigMongo", app_id: str):
        super().__init__(conf, prefix="broker", app_id=app_id)

    @cached_property
    def broker_message_queue(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="broker_message_queue",
            indexes=[
                IndexModel(
                    [
                        ("queue_name", ASCENDING),
                        ("priority", DESCENDING),
                        ("created_at", ASCENDING),
                    ]
                )
            ],
        )
        return self.instantiate_retriable_coll(spec)


class MongoBroker(BaseBroker):
    """
    A MongoDB-based implementation of the broker for cross-process coordination.

    Uses MongoDB for cross-process message queue coordination and implements
    all required abstract methods from BaseBroker. Routes invocation IDs through
    a persistent FIFO queue stored in MongoDB.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = BrokerCollections(self.conf, app_id=self.app.app_id)

    @cached_property
    def conf(self) -> ConfigBrokerMongo:
        return ConfigBrokerMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _route_invocation(
        self, invocation_id: "InvocationId", queue_name: str, priority: float
    ) -> None:
        """
        Route a single invocation ID to the message queue.

        :param invocation_id: The invocation ID to queue.
        :param queue_name: The logical broker queue.
        :param priority: The invocation priority within the queue.
        """
        self.cols.broker_message_queue.insert_one(
            {
                "invocation_id": str(invocation_id),
                "queue_name": queue_name,
                "priority": priority,
                "created_at": datetime.now(UTC),
            }
        )

    def _route_invocations(
        self,
        invocation_ids: Sequence["InvocationId"],
        queue_name: str,
        priority: float,
    ) -> None:
        """
        Route multiple invocation IDs to the message queue.

        :param invocation_ids: Invocation IDs to queue.
        :param queue_name: The logical broker queue.
        :param priority: The invocation priority within the queue.
        """
        if not invocation_ids:
            return

        now = datetime.now(UTC)
        documents = []
        for invocation_id in invocation_ids:
            documents.append(
                {
                    "invocation_id": str(invocation_id),
                    "queue_name": queue_name,
                    "priority": priority,
                    "created_at": now,
                }
            )
        self.cols.broker_message_queue.insert_many(documents)

    def retrieve_invocation(
        self, queue_name: str | None = None
    ) -> "InvocationId | None":
        """
        Atomically retrieve and remove a single invocation ID from the queue.

        Ensures that no two processes can retrieve the same invocation.

        :return: The next invocation ID in the queue, or None if empty
        """
        queue = self.conf.queues[0] if queue_name is None else queue_name
        self._validate_queue_names((queue,))
        document = self.cols.broker_message_queue.find_one_and_delete(
            {"queue_name": queue},
            sort=[("priority", -1), ("created_at", 1), ("_id", 1)],
        )
        if document:
            return InvocationId(document["invocation_id"])
        return None

    def count_invocations(self, queue_names: Sequence[str] | None = None) -> int:
        """
        Count the number of invocations in the queue.

        :return: Number of pending invocations
        """
        queues = self.conf.queues if queue_names is None else tuple(queue_names)
        self._validate_queue_names(queues)
        return self.cols.broker_message_queue.count_documents(
            {"queue_name": {"$in": queues}}
        )

    def purge(self) -> None:
        """Clear all messages from the queue."""
        self.cols.purge_all()
