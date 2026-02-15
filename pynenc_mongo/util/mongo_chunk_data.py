"""MongoDB utilities for storing large data exceeding BSON limits.

Compresses data with zlib and splits across multiple chunk documents when needed.
Chunk keys: "{invocation_id}:{key_prefix}[:{item}]"
"""

import json
import logging
import zlib
from enum import StrEnum, auto
from typing import TYPE_CHECKING, Any

from bson import BSON
from pymongo import ASCENDING

if TYPE_CHECKING:
    from pynenc_mongo.util.mongo_client import RetryableCollection

logger = logging.getLogger(__name__)


# ============================================================================
# Compression and chunking utilities
# ============================================================================


def _compress(data: str) -> bytes:
    """Compress a string using zlib (level 6 for speed/ratio balance)."""
    return zlib.compress(data.encode("utf-8"), level=6)


def _decompress(data: bytes) -> str:
    """Decompress zlib-compressed bytes back to a string."""
    return zlib.decompress(data).decode("utf-8")


def _split_into_chunks(data: bytes, chunk_size: int) -> list[bytes]:
    """Split bytes into ordered chunks of at most chunk_size bytes."""
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def _reassemble_chunks(chunks: list[bytes]) -> bytes:
    """Reassemble ordered chunks into the original bytes."""
    return b"".join(chunks)


def _exceeds_bson_threshold(data: dict, threshold: int) -> bool:
    """Check if a dict exceeds the size threshold for a single BSON document."""
    encoded = BSON.encode(data)
    return len(encoded) > threshold


# ============================================================================
# Storage metadata keys
# ============================================================================


class StorageKey(StrEnum):
    """Keys used in storage metadata documents."""

    CHUNKED = auto()  # List of keys for individually chunked values
    INLINE = auto()  # Dict of inline values (small enough to embed)
    ALL_CHUNKED = auto()  # Boolean flag indicating entire dict was chunked


# ============================================================================
# Public API
# ============================================================================


def build_chunk_key(**parts: str) -> str:
    """Build base chunk key from named parts."""
    return ":".join(str(v) for v in parts.values() if v)


# ============================================================================
# Internal chunk storage helpers
# ============================================================================


def _chunk_key(base_key: str, item: str = "") -> str:
    """Build chunk_key for MongoDB storage."""
    return f"{base_key}:{item}" if item else base_key


def _store_chunks(
    coll: "RetryableCollection", key: str, data: str, chunk_size: int
) -> int:
    """Compress, split, and store data as MongoDB chunk documents."""
    coll.delete_many({"chunk_key": key})  # Remove old chunks
    compressed = _compress(data)
    chunks = _split_into_chunks(compressed, chunk_size)
    if chunks:
        coll.insert_many(
            [{"chunk_key": key, "seq": i, "data": c} for i, c in enumerate(chunks)],
            ordered=True,
        )
    logger.debug("Stored %d bytes as %d chunks for %s", len(data), len(chunks), key)
    return len(chunks)


def _retrieve_chunks(coll: "RetryableCollection", key: str) -> str:
    """Retrieve, reassemble, and decompress chunked data from MongoDB."""
    cursor = (
        coll.find({"chunk_key": key}, {"data": 1, "_id": 0})
        .sort("seq", ASCENDING)
        .hint([("chunk_key", ASCENDING), ("seq", ASCENDING)])
    )
    chunks = [doc["data"] for doc in cursor]
    if not chunks:
        raise KeyError(f"No chunks for {key}")
    return _decompress(_reassemble_chunks(chunks))


# ============================================================================
# Main storage API
# ============================================================================


def prepare_chunk_storage(
    coll: "RetryableCollection",
    base_key: str,
    data: dict[str, str],
    threshold: int,
) -> dict[str, Any]:
    """Prepare data for storage, chunking if it exceeds threshold."""
    chunk_size = threshold  # Use threshold as chunk size

    # Fast path: all data fits inline
    if not _exceeds_bson_threshold(data, threshold):
        return {StorageKey.INLINE.value: data}

    # Separate oversized items and chunk them individually
    chunked_keys = []
    inline = {}
    for k, v in data.items():
        if _exceeds_bson_threshold({k: v}, threshold):
            _store_chunks(coll, _chunk_key(base_key, k), v, chunk_size)
            chunked_keys.append(k)
        else:
            inline[k] = v

    storage = {StorageKey.INLINE.value: inline, StorageKey.CHUNKED.value: chunked_keys}

    # If metadata itself is too large, chunk entire dict
    if _exceeds_bson_threshold(storage, threshold):
        _store_chunks(coll, base_key, json.dumps(data), chunk_size)
        logger.warning("Chunked entire dict for %s (%d items)", base_key, len(data))
        return {StorageKey.ALL_CHUNKED.value: True}

    logger.debug(
        "Chunked %s: %d inline, %d separate", base_key, len(inline), len(chunked_keys)
    )
    return storage


def retrieve_chunk_storage(
    coll: "RetryableCollection",
    base_key: str,
    storage_doc: dict[str, Any],
) -> dict[str, str]:
    """Retrieve data from storage, decompressing chunks if needed."""
    # Handle entire dict was chunked
    if storage_doc.get(StorageKey.ALL_CHUNKED.value):
        json_str = _retrieve_chunks(coll, base_key)
        return json.loads(json_str)

    # Combine inline items with individually chunked items
    result = dict(storage_doc.get(StorageKey.INLINE.value, {}))
    for k in storage_doc.get(StorageKey.CHUNKED.value, []):
        result[k] = _retrieve_chunks(coll, _chunk_key(base_key, k))

    return result
