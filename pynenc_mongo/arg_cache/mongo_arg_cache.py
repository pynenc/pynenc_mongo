import logging
from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel
from pynenc.arg_cache.base_arg_cache import BaseArgCache

from pynenc_mongo.conf.config_arg_cache import ConfigArgCacheMongo
from pynenc_mongo.util.chunked_data import exceeds_bson_threshold
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections
from pynenc_mongo.util.mongo_utils import (
    purge_chunked,
    retrieve_chunked,
    store_chunked,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc

    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection

logger = logging.getLogger(__name__)


class ArgCacheCollections(MongoCollections):
    """MongoDB collections for the argument cache system."""

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

    @cached_property
    def arg_cache_chunks(self) -> "RetryableCollection":
        """Collection for storing chunked arg cache values that exceed BSON limits."""
        spec = CollectionSpec(
            name="arg_cache_chunks",
            indexes=[
                IndexModel([("chunk_key", ASCENDING), ("seq", ASCENDING)], unique=True),
                IndexModel([("chunk_key", ASCENDING)]),
            ],
        )
        return self.instantiate_retriable_coll(spec)


class MongoArgCache(BaseArgCache):
    """
    A MongoDB-based implementation of the argument cache for cross-process coordination.

    Uses MongoDB for cross-process argument caching and implements
    all required abstract methods from BaseArgCache. Large values exceeding
    the chunk threshold are compressed and stored across multiple documents
    in a dedicated chunks collection to avoid BSON size limits.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = ArgCacheCollections(self.conf)

    @cached_property
    def conf(self) -> ConfigArgCacheMongo:
        return ConfigArgCacheMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _store(self, key: str, value: str) -> None:
        """
        Store a key value pair in the cache.

        Large values exceeding the chunk threshold are compressed and stored
        across multiple documents to avoid BSON size limits.

        :param key: The cache key
        :param value: The string value to cache
        """
        if exceeds_bson_threshold(value, self.conf.chunk_threshold):
            logger.warning(
                f"Storing large arg cache value ({len(value)} bytes) as chunks: {key}"
            )
            store_chunked(self.cols.arg_cache_chunks, key, value)
            self.cols.arg_cache.replace_one(
                {"cache_key": key},
                {
                    "cache_key": key,
                    "chunked": True,
                    "created_at": datetime.now(UTC),
                },
                upsert=True,
            )
        else:
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

        :param key: The cache key
        :return: The cached serialized value
        :raises KeyError: If the key is not found
        """
        doc = self.cols.arg_cache.find_one({"cache_key": key})
        if doc:
            if doc.get("chunked"):
                return retrieve_chunked(self.cols.arg_cache_chunks, key)
            return doc["cached_data"].decode("utf-8")
        raise KeyError(f"Cache key {key} not found")

    def _purge(self) -> None:
        """Clear all cached data including chunked storage."""
        purge_chunked(self.cols.arg_cache_chunks)
        self.cols.purge_all()
