# mongo_collection_specs.py
"""
Collection specifications for all MongoDB-based Pynenc components.
Centralized location for all collection definitions and indexes.
"""

from dataclasses import dataclass, field

from pymongo import IndexModel

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_client import PynencMongoClient, RetryableCollection


@dataclass
class CollectionSpec:
    """Specification for a collection with its indexes"""

    name: str
    indexes: list[IndexModel] = field(default_factory=list)


class MongoCollections:
    """Abstract base class for MongoDB collections with prefix enforcement."""

    def __init__(self, conf: ConfigMongo, prefix: str):
        self.conf = conf
        self.prefix = prefix.rstrip("_") + "_"  # Normalize prefix

    def instantiate_retriable_coll(
        self, spec: "CollectionSpec"
    ) -> "RetryableCollection":
        """
        Instantiate a RetryableCollection for the given CollectionSpec.

        :param spec: Specification for the collection
        :return: RetryableCollection instance
        """
        client = PynencMongoClient.get_instance(self.conf)
        return client.get_collection(spec)

    def purge_all(self) -> None:
        """Purge all collections."""
        for attr_name in dir(self):
            if attr_name.startswith("_"):
                continue
            col = getattr(self, attr_name)
            if isinstance(col, RetryableCollection):
                col.delete_many({})
