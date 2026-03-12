import logging
from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING

from pymongo import ASCENDING, IndexModel
from pynenc.client_data_store.base_client_data_store import BaseClientDataStore

from pynenc_mongo.conf.config_client_data_store import ConfigClientDataStoreMongo
from pynenc_mongo.util.mongo_chunk_data import (
    build_chunk_key,
    prepare_chunk_storage,
    retrieve_chunk_storage,
)
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections

if TYPE_CHECKING:
    from pynenc.app import Pynenc

    from pynenc_mongo.conf.config_mongo import ConfigMongo
    from pynenc_mongo.util.mongo_client import RetryableCollection

logger = logging.getLogger(__name__)


class ArgCacheCollections(MongoCollections):
    """MongoDB collections for the argument cache system."""

    def __init__(self, conf: "ConfigMongo", app_id: str):
        super().__init__(conf, prefix="arg", app_id=app_id)

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


class MongoClientDataStore(BaseClientDataStore):
    """
    A MongoDB-based implementation of the argument cache for cross-process coordination.

    Uses MongoDB for cross-process argument caching and implements
    all required abstract methods from BaseClientDataStore. Large values exceeding
    the chunk threshold are compressed and stored across multiple documents
    in a dedicated chunks collection to avoid BSON size limits.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.cols = ArgCacheCollections(self.conf, app_id=self.app.app_id)

    @cached_property
    def conf(self) -> ConfigClientDataStoreMongo:
        return ConfigClientDataStoreMongo(
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
        # Wrap single value in dict for unified storage API
        storage_doc = prepare_chunk_storage(
            self.cols.arg_cache_chunks,
            build_chunk_key(cache_key=key),
            {"data": value},  # Wrap value in dict
            self.conf.chunk_threshold,
        )
        self.cols.arg_cache.replace_one(
            {"cache_key": key},
            {
                "cache_key": key,
                "created_at": datetime.now(UTC),
                **storage_doc,
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
        if not doc:
            raise KeyError(f"Cache key {key} not found")
        # Retrieve and unwrap value from dict
        result = retrieve_chunk_storage(
            self.cols.arg_cache_chunks,
            build_chunk_key(cache_key=key),
            doc,
        )
        return result["data"]

    def _purge(self) -> None:
        """Clear all cached data including chunked storage."""
        self.cols.arg_cache_chunks.delete_many({})
        self.cols.purge_all()
