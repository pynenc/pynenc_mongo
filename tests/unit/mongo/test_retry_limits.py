"""Tests for retry limit enforcement."""

from typing import Any
from unittest.mock import patch

import pytest
from pymongo import ASCENDING, IndexModel
from pymongo.errors import AutoReconnect

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_client import PynencMongoClient
from pynenc_mongo.util.mongo_collections import CollectionSpec


@pytest.fixture
def collection_spec() -> CollectionSpec:
    """Fixture for a sample CollectionSpec."""
    return CollectionSpec(
        name="test_retry_limits",
        indexes=[IndexModel([("test_field", ASCENDING)], unique=True)],
    )


def test_stops_after_max_retries(
    collection_spec: CollectionSpec, patch_mongo_client: None
) -> None:
    """Test that retry stops after max_retries is reached."""
    conf = ConfigMongo(
        {
            "mongo_url": "mongodb://localhost:27017/test_max_retries",
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
    collection_spec: CollectionSpec, patch_mongo_client: None
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
    collection_spec: CollectionSpec, patch_mongo_client: None
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
        assert call_count == succeed_after, "Should retry indefinitely until success"
        assert result == {"result": "success"}
