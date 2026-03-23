"""Unit tests for MongoDB chunked storage with prepare_chunk_storage/retrieve_chunk_storage.

Uses mongomock to test the storage format that handles inline and chunked data.
"""

from collections.abc import Generator

import mongomock
import pytest

from pynenc_mongo.util.mongo_chunk_data import (
    StorageKey,
    prepare_chunk_storage,
    retrieve_chunk_storage,
)
from pynenc_mongo.util.mongo_client import PynencMongoClient


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


def test_prepare_retrieve_small_dict_inline(
    chunks_collection: mongomock.Collection,
) -> None:
    """Small dicts should be stored inline without chunking."""
    original = {"key1": "value1", "key2": "value2"}
    base_key = "test_key"
    threshold = 10_000

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should store inline
    assert StorageKey.INLINE.value in storage
    assert storage[StorageKey.INLINE.value] == original

    # Retrieve should return original
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original


def test_prepare_retrieve_large_value_chunked(
    chunks_collection: mongomock.Collection,
) -> None:
    """Dicts with large values should chunk those values."""
    original = {"data": "x" * 10_000}
    base_key = "big_key"
    threshold = 1_000  # Small threshold to force chunking

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should have chunked the large value
    assert StorageKey.CHUNKED.value in storage
    assert "data" in storage[StorageKey.CHUNKED.value]

    # Should have created chunks in collection
    assert chunks_collection.count_documents({"chunk_key": f"{base_key}:data"}) > 0

    # Retrieve should return original
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original


def test_prepare_retrieve_dict_all_inline(
    chunks_collection: mongomock.Collection,
) -> None:
    """Dict with small values should store all inline."""
    original = {"key1": "value1", "key2": "value2", "key3": "value3"}
    base_key = "dict_key"
    threshold = 10_000

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should store all inline
    assert StorageKey.INLINE.value in storage
    assert storage[StorageKey.INLINE.value] == original
    assert StorageKey.CHUNKED.value not in storage

    # Retrieve should return original
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original


def test_prepare_retrieve_dict_mixed(chunks_collection: mongomock.Collection) -> None:
    """Dict with mixed small/large values should chunk only large ones."""
    original = {
        "small1": "tiny",
        "small2": "also tiny",
        "large": "x" * 5_000,
    }
    base_key = "mixed_key"
    threshold = 1_000

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should have inline and chunked keys
    assert StorageKey.INLINE.value in storage
    assert StorageKey.CHUNKED.value in storage
    assert "small1" in storage[StorageKey.INLINE.value]
    assert "small2" in storage[StorageKey.INLINE.value]
    assert "large" in storage[StorageKey.CHUNKED.value]

    # Should have created chunks for large value
    assert chunks_collection.count_documents({"chunk_key": f"{base_key}:large"}) > 0

    # Retrieve should return original
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original


def test_prepare_retrieve_dict_all_chunked(
    chunks_collection: mongomock.Collection,
) -> None:
    """Dict with many items should chunk entire dict if metadata is too large."""
    # Create a dict with many items that individually fit but metadata doesn't
    original = {f"key{i}": f"value{i}" * 100 for i in range(100)}
    base_key = "all_chunked_key"
    threshold = 1_000  # Small threshold

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should have chunked entire dict
    assert storage.get(StorageKey.ALL_CHUNKED.value) is True

    # Should have created chunks
    assert chunks_collection.count_documents({"chunk_key": base_key}) > 0

    # Retrieve should return original
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original


def test_store_replaces_existing_chunks(
    chunks_collection: mongomock.Collection,
) -> None:
    """Storing with the same key should replace previous chunks."""
    base_key = "replace_key"
    threshold = 1_000

    # Store first value
    storage1 = prepare_chunk_storage(
        chunks_collection, base_key, {"data": "x" * 5_000}, threshold
    )
    result1 = retrieve_chunk_storage(chunks_collection, base_key, storage1)
    assert result1 == {"data": "x" * 5_000}

    # Store second value with same key
    storage2 = prepare_chunk_storage(
        chunks_collection, base_key, {"data": "y" * 5_000}, threshold
    )
    result2 = retrieve_chunk_storage(chunks_collection, base_key, storage2)
    assert result2 == {"data": "y" * 5_000}


def test_retrieve_missing_chunks(chunks_collection: mongomock.Collection) -> None:
    """Retrieving chunks that don't exist should raise KeyError."""
    storage = {StorageKey.ALL_CHUNKED.value: True}

    with pytest.raises(KeyError, match="No chunks"):
        retrieve_chunk_storage(chunks_collection, "missing_key", storage)


def test_unicode_roundtrip(chunks_collection: mongomock.Collection) -> None:
    """Unicode data should be preserved through chunk roundtrip."""
    original = {"text": "日本語テスト🎉" * 100}
    base_key = "unicode_key"
    threshold = 1_000

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original


def test_empty_dict(chunks_collection: mongomock.Collection) -> None:
    """Empty dict should be stored inline."""
    original: dict[str, str] = {}
    base_key = "empty_dict_key"
    threshold = 1_000

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should store inline
    assert storage[StorageKey.INLINE.value] == {}

    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == {}


def test_full_pipeline_with_compression(
    chunks_collection: mongomock.Collection,
) -> None:
    """End-to-end test with compression and chunking."""
    # Use random-like data that doesn't compress well to ensure multiple chunks
    import hashlib

    original = {
        "data": "".join(
            hashlib.sha256(str(i).encode()).hexdigest() for i in range(160)
        )  # 160 * 64 chars = 10,240 bytes
    }
    base_key = "pipeline_key"
    threshold = 1_000  # Small threshold to force chunking

    storage = prepare_chunk_storage(chunks_collection, base_key, original, threshold)

    # Should have chunked the large value
    assert StorageKey.CHUNKED.value in storage
    assert "data" in storage[StorageKey.CHUNKED.value]

    # Should have chunks in collection
    chunk_count = chunks_collection.count_documents({"chunk_key": f"{base_key}:data"})
    assert chunk_count >= 1  # At least one chunk exists

    # Retrieve should return original
    result = retrieve_chunk_storage(chunks_collection, base_key, storage)
    assert result == original
