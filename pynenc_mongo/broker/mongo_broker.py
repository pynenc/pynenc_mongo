from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel
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

    def __init__(self, conf: "ConfigMongo"):
        super().__init__(conf, prefix="broker")

    @cached_property
    def broker_message_queue(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="broker_message_queue",
            indexes=[IndexModel([("created_at", ASCENDING)])],
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
        self.cols = BrokerCollections(self.conf)

    @cached_property
    def conf(self) -> ConfigBrokerMongo:
        return ConfigBrokerMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def route_invocation(self, invocation_id: str) -> None:
        """
        Route a single invocation ID to the message queue.

        :param invocation_id: The invocation ID to route
        """
        self.cols.broker_message_queue.insert_one(
            {
                "invocation_id": invocation_id,
                "created_at": datetime.now(UTC),
            }
        )

    def route_invocations(self, invocation_ids: list["InvocationId"]) -> None:
        """
        Route multiple invocation IDs to the message queue.

        :param invocation_ids: List of invocation IDs to route
        """
        if not invocation_ids:
            return

        documents = [
            {
                "invocation_id": inv_id,
                "created_at": datetime.now(UTC),
            }
            for inv_id in invocation_ids
        ]
        self.cols.broker_message_queue.insert_many(documents)

    def retrieve_invocation(self) -> "InvocationId | None":
        """
        Atomically retrieve and remove a single invocation ID from the queue.

        Ensures that no two processes can retrieve the same invocation.

        :return: The next invocation ID in the queue, or None if empty
        """
        # Atomically find and delete the oldest message
        document = self.cols.broker_message_queue.find_one_and_delete(
            {}, sort=[("created_at", 1)]
        )

        if document:
            return InvocationId(document["invocation_id"])
        return None

    def count_invocations(self) -> int:
        """
        Count the number of invocations in the queue.

        :return: Number of pending invocations
        """
        return self.cols.broker_message_queue.count_documents({})

    def purge(self) -> None:
        """Clear all messages from the queue."""
        self.cols.purge_all()
