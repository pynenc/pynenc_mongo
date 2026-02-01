import logging
import threading
import time
from collections.abc import Callable
from enum import StrEnum, auto
from functools import wraps
from typing import TYPE_CHECKING, Any, NamedTuple

from bson.objectid import ObjectId
from gridfs import GridFS
from pymongo import MongoClient as PyMongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    CursorNotFound,
    DuplicateKeyError,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from pynenc_mongo.conf.config_mongo import ConfigMongo

if TYPE_CHECKING:
    from pynenc_mongo.util.mongo_collections import CollectionSpec

logger = logging.getLogger(__name__)

# Exceptions that indicate transient connection issues and should be retried
RETRYABLE_EXCEPTIONS = (
    AutoReconnect,
    ConnectionFailure,
    CursorNotFound,
    NetworkTimeout,
    NotPrimaryError,
    ServerSelectionTimeoutError,
)

# MongoDB server error codes that indicate transient failures and should be retried.
# These codes are not exposed as constants by pymongo, so we define them here.
# Reference: https://github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.yml
MONGO_ERROR_INTERRUPTED = 11600  # Operation was interrupted
MONGO_ERROR_INTERRUPTED_AT_SHUTDOWN = 11602  # Interrupted due to server shutdown
MONGO_ERROR_NOT_PRIMARY_NO_SECONDARY_OK = 13435  # Not primary and secondaryOk=false
MONGO_ERROR_NOT_PRIMARY_OR_SECONDARY = 13436  # Node is neither primary nor secondary

RETRYABLE_OPERATION_FAILURE_CODES = (
    MONGO_ERROR_INTERRUPTED,
    MONGO_ERROR_INTERRUPTED_AT_SHUTDOWN,
    MONGO_ERROR_NOT_PRIMARY_NO_SECONDARY_OK,
    MONGO_ERROR_NOT_PRIMARY_OR_SECONDARY,
)


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
        max_delay: float = 60.0,
        max_time: float = 300.0,
        retry_indefinitely: bool = False,
    ) -> None:
        self._collection = collection
        self._spec = spec
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._max_time = max_time
        self._retry_indefinitely = retry_indefinitely

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

    def _should_stop_retrying(self, attempt: int, elapsed: float) -> bool:
        """Check if retry loop should stop based on attempts and time."""
        if self._retry_indefinitely:
            return False
        return attempt >= self._max_retries or elapsed >= self._max_time

    def _log_retry(
        self, func_name: str, attempt: int, delay: float, error: Exception
    ) -> None:
        """Log a warning message for retry attempts."""
        logger.warning(
            f"MongoDB connection error during '{func_name}' "
            f"(attempt {attempt}): {error}. Retrying in {delay:.1f}s..."
        )

    def _log_failure(
        self, func_name: str, attempt: int, elapsed: float, error: Exception
    ) -> None:
        """Log an error message when retries are exhausted."""
        logger.error(
            f"MongoDB operation '{func_name}' failed after "
            f"{attempt} attempts over {elapsed:.1f}s: {error}"
        )

    def _wrap_with_retry(self, func: Callable[..., Any]) -> Callable[..., Any]:
        # Capture method references before creating wrapper to avoid __getattr__ proxy
        should_stop = self._should_stop_retrying
        log_failure = self._log_failure
        log_retry = self._log_retry
        base_delay = self._base_delay
        max_delay = self._max_delay

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            delay = base_delay
            start_time = time.monotonic()
            attempt = 0

            while True:
                try:
                    return func(*args, **kwargs)
                except (*RETRYABLE_EXCEPTIONS, OperationFailure) as e:
                    # Only check error codes for exact OperationFailure (not subclasses like CursorNotFound)
                    if (
                        type(e) is OperationFailure
                        and e.code not in RETRYABLE_OPERATION_FAILURE_CODES
                    ):
                        raise

                    attempt += 1
                    elapsed = time.monotonic() - start_time

                    if should_stop(attempt, elapsed):
                        log_failure(func.__name__, attempt, elapsed, e)
                        raise

                    current_delay = min(delay, max_delay)
                    log_retry(func.__name__, attempt, current_delay, e)
                    time.sleep(current_delay)
                    delay *= 2

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
        db: Database = self._client[self.conf.mongo_db]
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
            max_delay=self.conf.retry_max_delay,
            max_time=self.conf.retry_max_time,
            retry_indefinitely=self.conf.retry_indefinitely,
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


class GridFSStorage:
    """
    Utility for storing and retrieving large documents using GridFS.

    Provides a simple key-value interface for storing data that may exceed
    MongoDB's 16MB BSON document limit.
    """

    GRIDFS_PREFIX = "__gridfs__:"

    def __init__(self, conf: "ConfigMongo", collection_prefix: str = "pynenc") -> None:
        self.conf = conf
        self._collection_prefix = collection_prefix
        self._client = PynencMongoClient.get_instance(conf)
        self._gridfs: GridFS | None = None

    @property
    def gridfs(self) -> GridFS:
        """Get or create the GridFS instance."""
        if self._gridfs is None:
            db: Database = self._client._client[self.conf.mongo_db]
            self._gridfs = GridFS(db, collection=self._collection_prefix)
        return self._gridfs

    def should_use_gridfs(self, data: str | bytes) -> bool:
        """Check if data exceeds the threshold for GridFS storage."""
        size = len(data.encode("utf-8")) if isinstance(data, str) else len(data)
        return size >= self.conf.gridfs_threshold

    def store(self, key: str, data: str | bytes) -> str:
        """
        Store data in GridFS.

        :param key: Unique identifier for the data
        :param data: Data to store (string or bytes)
        :return: The key prefixed with GridFS marker
        """
        data_bytes = data.encode("utf-8") if isinstance(data, str) else data
        # Delete existing file with this key if present
        existing = self.gridfs.find_one({"filename": key})
        if existing:
            self.gridfs.delete(existing._id)
        self.gridfs.put(data_bytes, filename=key)
        logger.debug(f"Stored {len(data_bytes)} bytes in GridFS with key: {key}")
        return f"{self.GRIDFS_PREFIX}{key}"

    def retrieve(self, key: str) -> str:
        """
        Retrieve data from GridFS.

        :param key: The key (with or without GridFS prefix)
        :return: The stored data as string
        :raises KeyError: If key not found
        """
        actual_key = (
            key[len(self.GRIDFS_PREFIX) :]
            if key.startswith(self.GRIDFS_PREFIX)
            else key
        )
        grid_out = self.gridfs.find_one({"filename": actual_key})
        if grid_out is None:
            raise KeyError(f"GridFS key {actual_key} not found")
        return grid_out.read().decode("utf-8")

    def delete(self, key: str) -> None:
        """Delete data from GridFS."""
        actual_key = (
            key[len(self.GRIDFS_PREFIX) :]
            if key.startswith(self.GRIDFS_PREFIX)
            else key
        )
        existing = self.gridfs.find_one({"filename": actual_key})
        if existing:
            self.gridfs.delete(existing._id)

    def purge(self) -> None:
        """Delete all files in this GridFS collection."""
        for grid_file in self.gridfs.find():
            self.gridfs.delete(grid_file._id)

    @classmethod
    def is_gridfs_key(cls, key: str) -> bool:
        """Check if a key indicates GridFS storage."""
        return key.startswith(cls.GRIDFS_PREFIX)
