import pickle
from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pymongo import ASCENDING, IndexModel
from pynenc.arg_cache.base_arg_cache import BaseArgCache

from pynenc_mongo.conf.config_arg_cache import ConfigArgCacheMongo
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc.app import Pynenc

    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection


class TriggerCollections(MongoCollections):
    """MongoDB collections for the trigger system."""

    def __init__(self, conf: "ConfigMongo"):
        super().__init__(conf, prefix="arg")

    @cached_property
    def arg_cache(self) -> "RetryableCollection":
        spec = CollectionSpec(
            name="arg_cache",
            indexes=[
                IndexModel([("cache_key", ASCENDING)], unique=True),
                IndexModel([("created_at", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)


class MongoArgCache(BaseArgCache):
    """
    A MongoDB-based implementation of the argument cache for cross-process coordination.

    Uses MongoDB for cross-process argument caching and implements
    all required abstract methods from BaseArgCache.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = TriggerCollections(self.conf)

    @cached_property
    def conf(self) -> ConfigArgCacheMongo:
        return ConfigArgCacheMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def store(self, cache_key: str, value: Any) -> None:
        """Store a value in the cache."""
        self.cols.arg_cache.replace_one(
            {"cache_key": cache_key},
            {
                "cache_key": cache_key,
                "cached_data": pickle.dumps(value),
                "created_at": datetime.now(UTC),
            },
            upsert=True,
        )

    def retrieve(self, cache_key: str) -> Any:
        """Retrieve a value from the cache."""
        doc = self.cols.arg_cache.find_one({"cache_key": cache_key})
        if doc:
            return pickle.loads(doc["cached_data"])
        raise KeyError(f"Cache key {cache_key} not found")

    def exists(self, cache_key: str) -> bool:
        """Check if a cache key exists."""
        return (
            self.cols.arg_cache.count_documents({"cache_key": cache_key}, limit=1) > 0
        )

    def _store(self, key: str, value: str) -> None:
        """
        Store a key value pair in the cache.

        :param str key: The cache key
        :param str value: The string value to cache
        """
        self.cols.arg_cache.replace_one(
            {"cache_key": key},
            {
                "cache_key": key,
                "cached_data": value.encode("utf-8"),
                "created_at": datetime.now(UTC),
            },
            upsert=True,
        )

    def _retrieve(self, key: str) -> str:
        """
        Retrieve a serialized value from the cache by its key.

        :param str key: The cache key
        :return: The cached serialized value
        :raises KeyError: If the key is not found
        """
        doc = self.cols.arg_cache.find_one({"cache_key": key})
        if doc:
            return doc["cached_data"].decode("utf-8")
        raise KeyError(f"Cache key {key} not found")

    def _purge(self) -> None:
        """Clear all cached data."""
        self.cols.purge_all()
