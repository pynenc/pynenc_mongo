from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel
from pynenc.broker.base_broker import BaseBroker

from pynenc_mongo.conf.config_broker import ConfigBrokerMongo
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation

    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection


class BrokerCollections(MongoCollections):
    """MongoDB collections for the trigger system."""

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
    all required abstract methods from BaseBroker.
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

    def send_message(self, invocation: "DistributedInvocation") -> None:
        """Send a message (invocation) to the queue."""
        self.cols.broker_message_queue.insert_one(
            {
                "invocation_id": invocation.invocation_id,
                "invocation_json": invocation.to_json(),
                "created_at": datetime.now(UTC),
            }
        )

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        """Route a single invocation by sending it to the message queue."""
        self.send_message(invocation)

    def route_invocations(self, invocations: list["DistributedInvocation"]) -> None:
        """Route multiple invocations by sending them to the message queue."""
        if not invocations:
            return

        documents = [
            {
                "invocation_id": inv.invocation_id,
                "invocation_json": inv.to_json(),
                "created_at": datetime.now(UTC),
            }
            for inv in invocations
        ]
        inv_ids = [inv.invocation_id for inv in invocations]
        self.app.logger.warning(f"Routing {len(invocations)} invocations: {inv_ids}")
        self.cols.broker_message_queue.insert_many(documents)

    def retrieve_invocation(self) -> "DistributedInvocation | None":
        """
        Atomically retrieve and remove a single invocation from the queue.
        Ensures that no two processes can retrieve the same invocation.
        :return: The next DistributedInvocation in the queue, or None if empty.
        """
        from pynenc.invocation.dist_invocation import DistributedInvocation

        # Atomically find and delete the oldest message
        document = self.cols.broker_message_queue.find_one_and_delete(
            {}, sort=[("created_at", 1)]
        )

        if document:
            self.app.logger.warning(
                f"Retrieved (and deleted) invocation from queue: {document['invocation_id']}"
            )
            return DistributedInvocation.from_json(
                self.app, document["invocation_json"]
            )
        return None

    def count_invocations(self) -> int:
        """Count the number of invocations in the queue."""
        return self.cols.broker_message_queue.count_documents({})

    def purge(self) -> None:
        """Clear all cached data."""
        self.cols.purge_all()
