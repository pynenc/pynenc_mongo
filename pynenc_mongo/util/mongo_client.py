import threading
import time
from collections.abc import Callable
from enum import StrEnum, auto
from functools import wraps
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from bson.objectid import ObjectId
from pymongo import MongoClient as PyMongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import AutoReconnect, CursorNotFound, DuplicateKeyError

from pynenc_mongo.conf.config_mongo import ConfigMongo

if TYPE_CHECKING:
    from pynenc_mongo.util.mongo_collections import CollectionSpec


class UpsertOutcome(StrEnum):
    """Outcome of an upsert operation."""

    INSERTED = auto()
    UPDATED = auto()
    DUPLICATE = auto()


class UpsertResult(NamedTuple):
    """Result of an upsert operation."""

    outcome: UpsertOutcome
    matched_count: int = 0
    modified_count: int = 0
    upserted_id: ObjectId | None = None

    @property
    def success(self) -> bool:
        """Whether the operation succeeded."""
        return self.outcome in (UpsertOutcome.INSERTED, UpsertOutcome.UPDATED)

    @property
    def changed(self) -> bool:
        """Whether any document was inserted or modified."""
        return self.modified_count > 0 or self.upserted_id is not None


class RetryableCollection:
    """Proxy for Collection that adds automatic retry to all operations and stores spec."""

    def __init__(
        self,
        collection: Collection,
        spec: "CollectionSpec",
        max_retries: int = 3,
        base_delay: float = 0.1,
    ) -> None:
        self._collection = collection
        self._spec = spec
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = 10.0

    @property
    def spec(self) -> "CollectionSpec":
        """Return the stored CollectionSpec."""
        return self._spec

    def __getattr__(self, name: str) -> Any:
        """Proxy all collection methods with retry logic."""
        attr = getattr(self._collection, name)
        if callable(attr):
            return self._wrap_with_retry(attr)
        return attr

    def _wrap_with_retry(self, func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = self._base_delay
            for attempt in range(self._max_retries):
                try:
                    return func(*args, **kwargs)
                except (AutoReconnect, CursorNotFound):
                    if attempt == self._max_retries - 1:
                        raise
                    time.sleep(min(delay, self._max_delay))
                    delay *= 2
            return func(*args, **kwargs)

        return wrapper

    def upsert_document(
        self, filter: dict[str, Any], update: dict[str, Any]
    ) -> UpsertResult:
        """
        Upsert a document in the collection.

        :param filter: The filter to match the document
        :param update: The document fields to update (will be set)
        :return: UpsertResult with operation details
        """
        try:
            result = self._collection.update_one(filter, {"$set": update}, upsert=True)
            return UpsertResult(
                outcome=UpsertOutcome.INSERTED
                if result.upserted_id
                else UpsertOutcome.UPDATED,
                matched_count=result.matched_count,
                modified_count=result.modified_count,
                upserted_id=result.upserted_id,
            )
        except DuplicateKeyError:
            return UpsertResult(outcome=UpsertOutcome.DUPLICATE)

    def insert_or_ignore(self, document: dict[str, Any]) -> None:
        """
        Insert a document if it does not already exist based on the unique index.

        :param document: The document to insert
        """
        try:
            self._collection.insert_one(document)
        except DuplicateKeyError:
            pass


class PynencMongoClient:
    """Singleton MongoDB client for Pynenc, managing connections and collections with retry logic."""

    _instances: dict[str, "PynencMongoClient"] = {}
    _lock = threading.RLock()

    def __init__(self, conf: "ConfigMongo") -> None:
        self.conf = conf
        self._validated_collections: set = set()
        self._client: PyMongoClient = get_mongo_client(conf)

    @classmethod
    def get_instance(cls, conf: "ConfigMongo") -> "PynencMongoClient":
        """Get or create a singleton instance for the given configuration."""
        key = get_conn_key(conf)
        with cls._lock:
            if key not in cls._instances:
                cls._instances[key] = cls(conf)
            return cls._instances[key]

    def get_collection(self, spec: "CollectionSpec") -> RetryableCollection:
        """Returns a retryable collection proxy with stored spec."""
        db = cast(Database, self._client[self.conf.mongo_db])
        collection_key = f"{self.conf.mongo_db}.{spec.name}"

        # Ensure indexes exist (only once per collection)
        if collection_key not in self._validated_collections:
            with self._lock:
                if collection_key not in self._validated_collections:
                    collection = db[spec.name]
                    for index in spec.indexes:
                        collection.create_indexes([index])
                    self._validated_collections.add(collection_key)

        return RetryableCollection(
            collection=db[spec.name],
            spec=spec,
            max_retries=self.conf.max_retries,
            base_delay=self.conf.retry_base_delay,
        )


def get_conn_key(conf: "ConfigMongo") -> str:
    """Generate a unique connection key based on configuration."""
    conn_args = get_conn_args(conf)
    key_parts = [f"{k}={v}" for k, v in sorted(conn_args.items())]
    return "&".join(key_parts)


def get_conn_args(conf: "ConfigMongo") -> dict[str, str | int | None]:
    """Generate connection arguments for MongoClient based on configuration."""
    args = {}
    if conf.mongo_url:
        args["host"] = conf.mongo_url
    else:
        args["host"] = conf.mongo_host
        args["port"] = conf.mongo_port
        if conf.mongo_username:
            args["username"] = conf.mongo_username
        if conf.mongo_password:
            args["password"] = conf.mongo_password
        if conf.mongo_auth_source:
            args["authSource"] = conf.mongo_auth_source
    return args


def get_mongo_client(conf: "ConfigMongo") -> PyMongoClient:
    """
    Initialize the MongoDB client using configuration.

    Uses mongo_url if defined, otherwise falls back to host/port/username/password/authSource.
    """
    client_args = {
        "maxPoolSize": conf.mongo_pool_max_connections,
        "socketTimeoutMS": conf.socket_timeout * 1000,
        "connectTimeoutMS": conf.socket_connect_timeout * 1000,
        "retryWrites": True,
    }

    return PyMongoClient(**client_args | get_conn_args(conf))
