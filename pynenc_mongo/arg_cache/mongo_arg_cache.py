import logging
from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel
from pynenc.arg_cache.base_arg_cache import BaseArgCache

from pynenc_mongo.conf.config_arg_cache import ConfigArgCacheMongo
from pynenc_mongo.util.mongo_client import GridFSStorage
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

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


class MongoArgCache(BaseArgCache):
    """
    A MongoDB-based implementation of the argument cache for cross-process coordination.

    Uses MongoDB for cross-process argument caching and implements
    all required abstract methods from BaseArgCache. Large values exceeding
    the GridFS threshold are stored in GridFS to avoid BSON size limits.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = ArgCacheCollections(self.conf)
        self._gridfs = GridFSStorage(self.conf, collection_prefix="arg_cache_gridfs")

    @cached_property
    def conf(self) -> ConfigArgCacheMongo:
        return ConfigArgCacheMongo(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _store(self, key: str, value: str) -> None:
        """
        Store a key value pair in the cache.

        Uses GridFS for values exceeding the threshold to avoid BSON size limits.

        :param str key: The cache key
        :param str value: The string value to cache
        """
        if self._gridfs.should_use_gridfs(value):
            logger.warning(
                f"Storing large arg cache value ({len(value)} bytes) in GridFS: {key}"
            )
            gridfs_key = self._gridfs.store(key, value)
            # Store reference in collection for index/metadata
            self.cols.arg_cache.replace_one(
                {"cache_key": key},
                {
                    "cache_key": key,
                    "gridfs_key": gridfs_key,
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

        :param str key: The cache key
        :return: The cached serialized value
        :raises KeyError: If the key is not found
        """
        doc = self.cols.arg_cache.find_one({"cache_key": key})
        if doc:
            if gridfs_key := doc.get("gridfs_key"):
                return self._gridfs.retrieve(gridfs_key)
            return doc["cached_data"].decode("utf-8")
        raise KeyError(f"Cache key {key} not found")

    def _purge(self) -> None:
        """Clear all cached data."""
        self._gridfs.purge()
        self.cols.purge_all()
