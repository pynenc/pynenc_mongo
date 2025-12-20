from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import mongomock
import pytest
from pymongo import ASCENDING, IndexModel
from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    CursorNotFound,
    NetworkTimeout,
    NotPrimaryError,
    OperationFailure,
    ServerSelectionTimeoutError,
)

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_client import (
    MONGO_ERROR_INTERRUPTED,
    MONGO_ERROR_INTERRUPTED_AT_SHUTDOWN,
    MONGO_ERROR_NOT_PRIMARY_NO_SECONDARY_OK,
    MONGO_ERROR_NOT_PRIMARY_OR_SECONDARY,
    PynencMongoClient,
    RetryableCollection,
)
from pynenc_mongo.util.mongo_collections import CollectionSpec

if TYPE_CHECKING:
    pass


@pytest.fixture
def mongo_conf() -> ConfigMongo:
    """Fixture for a sample ConfigMongo instance."""
    return ConfigMongo({"mongo_url": "mongodb://localhost:27017/test"})


@pytest.fixture
def collection_spec() -> CollectionSpec:
    """Fixture for a sample CollectionSpec."""
    return CollectionSpec(
        name="test_collection",
        indexes=[IndexModel([("test_field", ASCENDING)], unique=True)],
    )


def test_singleton_instance(mongo_conf: ConfigMongo) -> None:
    """Test that get_instance returns the same instance for the same config."""
    client1 = PynencMongoClient.get_instance(mongo_conf)
    client2 = PynencMongoClient.get_instance(mongo_conf)
    assert client1 is client2, "Singleton instance should be the same"
    assert client1.conf == mongo_conf, "Config should match"


def test_different_config_instances(mongo_conf: ConfigMongo) -> None:
    """Test that different configs create different instances."""
    other_conf = ConfigMongo({"mongo_url": "mongodb://localhost:27017/other"})
    client1 = PynencMongoClient.get_instance(mongo_conf)
    client2 = PynencMongoClient.get_instance(other_conf)
    assert client1 is not client2, "Different configs should create different instances"


def test_get_collection_should_use_mongomock_client(
    mongo_conf: ConfigMongo, patch_mongo_client: None
) -> None:
    """
    Test that get_collection uses mongomock.MongoClient as the underlying client when patched.
    Ensures that the MongoClient instance is a mongomock.MongoClient, not pymongo.MongoClient.
    """
    client = PynencMongoClient.get_instance(mongo_conf)
    assert isinstance(client._client, mongomock.MongoClient), (
        "MongoClient should be mocked with mongomock"
    )


def test_get_collection_with_indexes(
    mongo_conf: ConfigMongo, collection_spec: CollectionSpec, patch_mongo_client: None
) -> None:
    """Test that get_collection creates a collection with correct indexes."""
    client = PynencMongoClient.get_instance(mongo_conf)
    retryable_collection = client.get_collection(collection_spec)

    assert isinstance(retryable_collection, RetryableCollection)
    assert retryable_collection.spec == collection_spec

    # Verify index creation
    collection = retryable_collection._collection
    indexes = collection.index_information()
    assert "test_field_1" in indexes, "Index should be created"
    assert indexes["test_field_1"]["unique"], "Index should be unique"


def test_index_creation_once(
    mongo_conf: ConfigMongo, collection_spec: CollectionSpec, patch_mongo_client: None
) -> None:
    """Test that indexes are created only once."""
    client = PynencMongoClient.get_instance(mongo_conf)
    collection1 = client.get_collection(collection_spec)
    collection2 = client.get_collection(collection_spec)

    # Verify indexes are not recreated
    collection_key = f"{mongo_conf.mongo_db}.{collection_spec.name}"
    assert collection_key in client._validated_collections, (
        "Collection should be marked as validated"
    )
    assert collection1._collection is collection2._collection, (
        "Same collection object should be returned"
    )


def test_retryable_collection_retry(
    mongo_conf: ConfigMongo, collection_spec: CollectionSpec, patch_mongo_client: None
) -> None:
    """Test that RetryableCollection retries on AutoReconnect errors."""
    client = PynencMongoClient.get_instance(mongo_conf)
    retryable_collection = client.get_collection(collection_spec)

    # Mock a method to raise AutoReconnect twice, then succeed
    call_count = 0

    def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            raise AutoReconnect("Connection error")
        return [{"result": "success"}]

    with patch.object(retryable_collection._collection, "find", mock_find):
        result = retryable_collection.find()[0]
        assert call_count == 3, "Should retry twice before succeeding"
        assert result == {"result": "success"}, "Should return successful result"


def test_get_mongo_client_config(mongo_conf: ConfigMongo) -> None:
    """
    Test that get_mongo_client uses correct configuration.
    """
    with patch(
        "pynenc_mongo.util.mongo_client.PyMongoClient", new_callable=MagicMock
    ) as mock_client:
        PynencMongoClient.get_instance(mongo_conf)
        mock_client.assert_called_once_with(
            host="mongodb://localhost:27017/test",
            maxPoolSize=mongo_conf.mongo_pool_max_connections,
            socketTimeoutMS=mongo_conf.socket_timeout * 1000,
            connectTimeoutMS=mongo_conf.socket_connect_timeout * 1000,
            retryWrites=True,
        )


# ============================================================================
# Retry functionality tests
# ============================================================================


class TestRetryableExceptions:
    """Tests for retry behavior with different exception types."""

    @pytest.fixture
    def retryable_collection(
        self,
        mongo_conf: ConfigMongo,
        collection_spec: CollectionSpec,
        patch_mongo_client: None,
    ) -> RetryableCollection:
        """Create a RetryableCollection for testing."""
        client = PynencMongoClient.get_instance(mongo_conf)
        return client.get_collection(collection_spec)

    @pytest.mark.parametrize(
        "exception_class",
        [
            AutoReconnect,
            ConnectionFailure,
            CursorNotFound,
            NetworkTimeout,
            NotPrimaryError,
            ServerSelectionTimeoutError,
        ],
    )
    def test_retries_on_transient_exceptions(
        self,
        retryable_collection: RetryableCollection,
        exception_class: type[Exception],
    ) -> None:
        """Test that all transient exceptions trigger retry behavior."""
        call_count = 0

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise exception_class("Transient error")
            return [{"result": "success"}]

        with patch.object(retryable_collection._collection, "find", mock_find):
            result = retryable_collection.find()[0]
            assert call_count == 3, f"Should retry on {exception_class.__name__}"
            assert result == {"result": "success"}

    @pytest.mark.parametrize(
        "error_code,error_name",
        [
            (MONGO_ERROR_INTERRUPTED, "Interrupted"),
            (MONGO_ERROR_INTERRUPTED_AT_SHUTDOWN, "InterruptedAtShutdown"),
            (MONGO_ERROR_NOT_PRIMARY_NO_SECONDARY_OK, "NotPrimaryNoSecondaryOk"),
            (MONGO_ERROR_NOT_PRIMARY_OR_SECONDARY, "NotPrimaryOrSecondary"),
        ],
    )
    def test_retries_on_retryable_operation_failure_codes(
        self,
        retryable_collection: RetryableCollection,
        error_code: int,
        error_name: str,
    ) -> None:
        """Test that OperationFailure with retryable codes triggers retry."""
        call_count = 0

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise OperationFailure(error_name, code=error_code)
            return [{"result": "success"}]

        with patch.object(retryable_collection._collection, "find", mock_find):
            result = retryable_collection.find()[0]
            assert call_count == 3, (
                f"Should retry on OperationFailure code {error_code}"
            )
            assert result == {"result": "success"}

    def test_does_not_retry_on_non_retryable_operation_failure(
        self, retryable_collection: RetryableCollection
    ) -> None:
        """Test that OperationFailure with non-retryable codes raises immediately."""
        call_count = 0

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            # Code 2 is BadValue - not retryable
            raise OperationFailure("BadValue", code=2)

        with patch.object(retryable_collection._collection, "find", mock_find):
            with pytest.raises(OperationFailure) as exc_info:
                retryable_collection.find()
            assert call_count == 1, "Should not retry on non-retryable code"
            assert exc_info.value.code == 2


class TestRetryLimits:
    """Tests for retry limit enforcement."""

    @pytest.fixture
    def collection_spec(self) -> CollectionSpec:
        """Fixture for a sample CollectionSpec."""
        return CollectionSpec(
            name="test_retry_limits",
            indexes=[IndexModel([("test_field", ASCENDING)], unique=True)],
        )

    def test_stops_after_max_retries(
        self, collection_spec: CollectionSpec, patch_mongo_client: None
    ) -> None:
        """Test that retry stops after max_retries is reached."""
        conf = ConfigMongo(
            {
                "mongo_url": "mongodb://localhost:27017/test",
                "max_retries": 3,
            }
        )
        client = PynencMongoClient.get_instance(conf)
        retryable_collection = client.get_collection(collection_spec)

        call_count = 0

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            raise AutoReconnect("Always fails")

        with patch.object(retryable_collection._collection, "find", mock_find):
            with pytest.raises(AutoReconnect):
                retryable_collection.find()
            # Initial call + 3 retries = 4 total, but we stop at max_retries
            assert call_count == 3, "Should stop after max_retries attempts"

    def test_stops_after_max_time(
        self, collection_spec: CollectionSpec, patch_mongo_client: None
    ) -> None:
        """Test that retry stops after max_time is exceeded."""
        conf = ConfigMongo(
            {
                "mongo_url": "mongodb://localhost:27017/test_max_time",
                "max_retries": 100,  # High limit
                "retry_max_time": 0.5,  # Short time limit
                "retry_base_delay": 0.2,  # Delay between retries
            }
        )
        client = PynencMongoClient.get_instance(conf)
        retryable_collection = client.get_collection(collection_spec)

        call_count = 0

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            raise AutoReconnect("Always fails")

        with patch.object(retryable_collection._collection, "find", mock_find):
            with pytest.raises(AutoReconnect):
                retryable_collection.find()
            # Should stop due to time limit, not retry count
            assert call_count < 100, "Should stop due to max_time"
            assert call_count >= 2, "Should have made at least 2 attempts"

    def test_retry_indefinitely(
        self, collection_spec: CollectionSpec, patch_mongo_client: None
    ) -> None:
        """Test that retry_indefinitely ignores max_retries and max_time."""
        conf = ConfigMongo(
            {
                "mongo_url": "mongodb://localhost:27017/test_indefinite",
                "max_retries": 2,  # Low limit
                "retry_max_time": 0.1,  # Short time limit
                "retry_indefinitely": True,
                "retry_base_delay": 0.01,  # Fast retries for test
            }
        )
        client = PynencMongoClient.get_instance(conf)
        retryable_collection = client.get_collection(collection_spec)

        call_count = 0
        succeed_after = 5  # Succeed after more attempts than max_retries

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            if call_count < succeed_after:
                raise AutoReconnect("Temporary failure")
            return [{"result": "success"}]

        with patch.object(retryable_collection._collection, "find", mock_find):
            result = retryable_collection.find()[0]
            assert call_count == succeed_after, (
                "Should retry indefinitely until success"
            )
            assert result == {"result": "success"}


class TestExponentialBackoff:
    """Tests for exponential backoff behavior."""

    def test_exponential_delay_capped_by_max_delay(
        self,
        mongo_conf: ConfigMongo,
        collection_spec: CollectionSpec,
        patch_mongo_client: None,
    ) -> None:
        """Test that delay grows exponentially but is capped by max_delay."""
        conf = ConfigMongo(
            {
                "mongo_url": "mongodb://localhost:27017/test_backoff",
                "max_retries": 10,
                "retry_base_delay": 0.01,
                "retry_max_delay": 0.05,  # Cap at 0.05
            }
        )
        client = PynencMongoClient.get_instance(conf)
        retryable_collection = client.get_collection(collection_spec)

        delays: list[float] = []
        call_count = 0

        _ = __import__("time").sleep

        def mock_sleep(seconds: float) -> None:
            delays.append(seconds)

        def mock_find(*args: Any, **kwargs: Any) -> list[dict[str, str]]:
            nonlocal call_count
            call_count += 1
            if call_count <= 5:
                raise AutoReconnect("Temporary failure")
            return [{"result": "success"}]

        with (
            patch.object(retryable_collection._collection, "find", mock_find),
            patch("time.sleep", mock_sleep),
        ):
            retryable_collection.find()

        # Verify exponential growth capped at max_delay
        assert len(delays) == 5, "Should have 5 retry delays"
        assert delays[0] == 0.01, "First delay should be base_delay"
        assert delays[1] == 0.02, "Second delay should be 2x base"
        assert delays[2] == 0.04, "Third delay should be 4x base"
        # Delays 3 and 4 should be capped at max_delay
        assert delays[3] == 0.05, "Fourth delay should be capped at max_delay"
        assert delays[4] == 0.05, "Fifth delay should be capped at max_delay"
