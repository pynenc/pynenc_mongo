"""Unit tests for MongoDB chunked storage utilities.

Uses mongomock to test store/retrieve/delete/purge operations.
"""

from collections.abc import Generator
from unittest.mock import patch

import mongomock
import pytest

from pynenc_mongo.util.mongo_client import PynencMongoClient
from pynenc_mongo.util.mongo_utils import (
    delete_chunked,
    purge_chunked,
    retrieve_chunked,
    store_chunked,
)


@pytest.fixture(autouse=True)
def clear_singleton_cache() -> Generator[None, None, None]:
    """Clear PynencMongoClient singleton between tests."""
    PynencMongoClient._instances.clear()
    yield
    PynencMongoClient._instances.clear()


@pytest.fixture
def chunks_collection() -> mongomock.Collection:
    """Provide a mongomock collection for chunk storage tests."""
    client = mongomock.MongoClient()
    db = client["test_db"]
    return db["test_chunks"]


def test_store_and_retrieve_small(chunks_collection: mongomock.Collection) -> None:
    """Store and retrieve a small string that fits in one chunk."""
    original = "hello world"
    num_chunks = store_chunked(chunks_collection, "key1", original)
    assert num_chunks >= 1

    result = retrieve_chunked(chunks_collection, "key1")
    assert result == original


def test_store_and_retrieve_large(chunks_collection: mongomock.Collection) -> None:
    """Store and retrieve data that splits into multiple chunks."""
    # Use a small chunk size to force splitting
    original = "x" * 10_000

    with patch("pynenc_mongo.util.chunked_data.DEFAULT_CHUNK_BYTES", 100):
        store_chunked(chunks_collection, "big_key", original)
        result = retrieve_chunked(chunks_collection, "big_key")
        assert result == original


def test_store_replaces_existing(chunks_collection: mongomock.Collection) -> None:
    """Storing with the same key should replace previous chunks."""
    store_chunked(chunks_collection, "key1", "first value")
    store_chunked(chunks_collection, "key1", "second value")

    result = retrieve_chunked(chunks_collection, "key1")
    assert result == "second value"


def test_retrieve_missing_key(chunks_collection: mongomock.Collection) -> None:
    """Retrieving a non-existent key should raise KeyError."""
    with pytest.raises(KeyError, match="No chunks found"):
        retrieve_chunked(chunks_collection, "missing_key")


def test_delete_chunked(chunks_collection: mongomock.Collection) -> None:
    """Delete should remove all chunks for a key."""
    store_chunked(chunks_collection, "key1", "some data")
    delete_chunked(chunks_collection, "key1")

    with pytest.raises(KeyError):
        retrieve_chunked(chunks_collection, "key1")


def test_delete_only_target_key(chunks_collection: mongomock.Collection) -> None:
    """Delete should only remove chunks for the specified key."""
    store_chunked(chunks_collection, "key1", "data one")
    store_chunked(chunks_collection, "key2", "data two")

    delete_chunked(chunks_collection, "key1")

    with pytest.raises(KeyError):
        retrieve_chunked(chunks_collection, "key1")
    assert retrieve_chunked(chunks_collection, "key2") == "data two"


def test_purge_chunked(chunks_collection: mongomock.Collection) -> None:
    """Purge should remove all chunks from the collection."""
    store_chunked(chunks_collection, "key1", "data one")
    store_chunked(chunks_collection, "key2", "data two")

    purge_chunked(chunks_collection)

    with pytest.raises(KeyError):
        retrieve_chunked(chunks_collection, "key1")
    with pytest.raises(KeyError):
        retrieve_chunked(chunks_collection, "key2")


def test_unicode_roundtrip(chunks_collection: mongomock.Collection) -> None:
    """Store and retrieve unicode data correctly."""
    original = "日本語テスト🎉" * 100
    store_chunked(chunks_collection, "unicode_key", original)
    result = retrieve_chunked(chunks_collection, "unicode_key")
    assert result == original


def test_empty_string(chunks_collection: mongomock.Collection) -> None:
    """Store and retrieve an empty string."""
    store_chunked(chunks_collection, "empty", "")
    result = retrieve_chunked(chunks_collection, "empty")
    assert result == ""
