from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import mongomock
import pytest
from pymongo import ASCENDING, IndexModel
from pymongo.errors import AutoReconnect

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_client import PynencMongoClient, RetryableCollection
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
