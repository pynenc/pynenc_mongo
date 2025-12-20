"""Tests for exponential backoff behavior."""

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
        name="test_backoff",
        indexes=[IndexModel([("test_field", ASCENDING)], unique=True)],
    )


def test_exponential_delay_capped_by_max_delay(
    collection_spec: CollectionSpec, patch_mongo_client: None
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
