"""Tests for retry behavior with different exception types."""

from typing import Any
from unittest.mock import patch

import pytest
from pymongo import ASCENDING, IndexModel
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
from pynenc_mongo.util.mongo_client import (
    MONGO_ERROR_INTERRUPTED,
    MONGO_ERROR_INTERRUPTED_AT_SHUTDOWN,
    MONGO_ERROR_NOT_PRIMARY_NO_SECONDARY_OK,
    MONGO_ERROR_NOT_PRIMARY_OR_SECONDARY,
    PynencMongoClient,
    RetryableCollection,
)
from pynenc_mongo.util.mongo_collections import CollectionSpec


@pytest.fixture
def mongo_conf() -> ConfigMongo:
    """Fixture for a sample ConfigMongo instance."""
    return ConfigMongo({"mongo_url": "mongodb://localhost:27017/test_retry"})


@pytest.fixture
def collection_spec() -> CollectionSpec:
    """Fixture for a sample CollectionSpec."""
    return CollectionSpec(
        name="test_retry_exceptions",
        indexes=[IndexModel([("test_field", ASCENDING)], unique=True)],
    )


@pytest.fixture
def retryable_collection(
    mongo_conf: ConfigMongo, collection_spec: CollectionSpec, patch_mongo_client: None
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
    retryable_collection: RetryableCollection, exception_class: type[Exception]
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
    retryable_collection: RetryableCollection, error_code: int, error_name: str
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
        assert call_count == 3, f"Should retry on OperationFailure code {error_code}"
        assert result == {"result": "success"}


def test_does_not_retry_on_non_retryable_operation_failure(
    retryable_collection: RetryableCollection,
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


def test_does_not_retry_on_duplicate_key_error(
    retryable_collection: RetryableCollection,
) -> None:
    """Test that DuplicateKeyError raises immediately without retries."""
    call_count = 0

    def mock_insert(*args: Any, **kwargs: Any) -> None:
        nonlocal call_count
        call_count += 1
        raise DuplicateKeyError("duplicate key", code=11000)

    with patch.object(retryable_collection._collection, "insert_one", mock_insert):
        with pytest.raises(DuplicateKeyError):
            retryable_collection.insert_one({"test": "data"})
        assert call_count == 1, "Should not retry on DuplicateKeyError"
