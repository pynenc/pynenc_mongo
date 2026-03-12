import hashlib
import logging
import threading
import time
from collections.abc import Callable
from enum import StrEnum, auto
from functools import wraps
from typing import TYPE_CHECKING, Any, NamedTuple

from bson.objectid import ObjectId
from pymongo import IndexModel
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

# MongoDB 3.6 limits the fully qualified index namespace
# ("db.collection.$index_name") to 127 bytes.
_MAX_INDEX_NS_BYTES = 127

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
                except DuplicateKeyError:
                    raise
                except (*RETRYABLE_EXCEPTIONS, OperationFailure) as e:
                    # Only retry exact OperationFailure with known transient codes;
                    # subclasses in RETRYABLE_EXCEPTIONS (e.g. CursorNotFound) are always retried.
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

    @property
    def db(self) -> Database:
        """Return the configured database."""
        return self._client[self.conf.mongo_db]

    def list_collection_names(self) -> list[str]:
        """List all collection names in the configured database."""
        return self.db.list_collection_names()

    def get_collection(self, spec: "CollectionSpec") -> RetryableCollection:
        """Returns a retryable collection proxy with stored spec."""
        db: Database = self.db
        collection_key = f"{self.conf.mongo_db}.{spec.name}"

        # Ensure indexes exist (only once per collection)
        if collection_key not in self._validated_collections:
            with self._lock:
                if collection_key not in self._validated_collections:
                    collection = db[spec.name]
                    for index in spec.indexes:
                        safe = _shorten_index_if_needed(
                            index, self.conf.mongo_db, spec.name
                        )
                        collection.create_indexes([safe])
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


def _shorten_index_if_needed(
    index: IndexModel, db_name: str, collection_name: str
) -> IndexModel:
    """Shorten index name if namespace would exceed MongoDB's byte limit.

    MongoDB 3.6 limits fully qualified index namespaces
    (``db.collection.$index_name``) to 127 bytes. When the auto-generated
    name would exceed this, a deterministic hash-based name is substituted.

    :param index: The original IndexModel
    :param db_name: The database name
    :param collection_name: The collection name
    :return: The original index or a new IndexModel with a shortened name
    """
    idx_doc = index.document
    idx_name = idx_doc.get("name", "")
    ns = f"{db_name}.{collection_name}.${idx_name}"
    if len(ns.encode("utf-8")) <= _MAX_INDEX_NS_BYTES:
        return index
    short_name = "idx_" + hashlib.sha256(idx_name.encode()).hexdigest()[:8]
    kwargs = {k: v for k, v in idx_doc.items() if k not in ("key", "name")}
    kwargs["name"] = short_name
    return IndexModel(list(idx_doc["key"].items()), **kwargs)


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
