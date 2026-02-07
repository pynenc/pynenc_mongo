"""
MongoDB utilities for storing and retrieving large documents using chunked collections.

When a document exceeds MongoDB's 16MB BSON limit, this module compresses the data
with zlib and splits it across multiple documents in a dedicated chunks collection.
Each chunk is a simple document with a key, sequence number, and binary data payload.

Key components:
- store_chunked: compress and store large data across multiple documents
- retrieve_chunked: retrieve and reassemble chunked data
- delete_chunked: remove all chunks for a given key
- purge_chunked: clear all data in a chunks collection
"""

import logging
from typing import TYPE_CHECKING

from pymongo import ASCENDING

from pynenc_mongo.util.chunked_data import (
    compress,
    decompress,
    reassemble_chunks,
    split_into_chunks,
)

if TYPE_CHECKING:
    from pynenc_mongo.util.mongo_client import RetryableCollection

logger = logging.getLogger(__name__)


def store_chunked(
    collection: "RetryableCollection",
    key: str,
    data: str,
) -> int:
    """
    Compress and store a large string as chunked documents.

    Each chunk document has:
      - chunk_key: the lookup key
      - seq: 0-based sequence number
      - data: binary chunk payload

    Existing chunks for the same key are replaced.

    :param collection: The RetryableCollection to store chunks in
    :param key: Unique identifier for the data
    :param data: The string to store
    :return: Number of chunks stored
    """
    # Remove any previous chunks for this key
    delete_chunked(collection, key)

    compressed = compress(data)
    chunks = split_into_chunks(compressed)

    # Bulk insert all chunks at once for better performance
    if chunks:
        chunk_docs = [
            {
                "chunk_key": key,
                "seq": seq,
                "data": chunk_bytes,
            }
            for seq, chunk_bytes in enumerate(chunks)
        ]
        collection.insert_many(chunk_docs, ordered=True)

    logger.debug(
        f"Stored {len(data)} bytes as {len(chunks)} chunks "
        f"({len(compressed)} bytes compressed) for key: {key}"
    )
    return len(chunks)


def retrieve_chunked(
    collection: "RetryableCollection",
    key: str,
) -> str:
    """
    Retrieve and reassemble chunked data.

    :param collection: The RetryableCollection containing chunks
    :param key: The lookup key
    :return: The original string
    :raises KeyError: If no chunks are found for the key
    """
    # Use projection to only fetch 'data' field (skip _id and chunk_key)
    # Use hint to ensure we use the compound index for optimal query performance
    cursor = (
        collection.find({"chunk_key": key}, {"data": 1, "_id": 0})
        .sort("seq", ASCENDING)
        .hint([("chunk_key", ASCENDING), ("seq", ASCENDING)])
    )
    chunks = [doc["data"] for doc in cursor]

    if not chunks:
        raise KeyError(f"No chunks found for key: {key}")

    compressed = reassemble_chunks(chunks)
    return decompress(compressed)


def delete_chunked(
    collection: "RetryableCollection",
    key: str,
) -> None:
    """
    Delete all chunks for a given key.

    :param collection: The RetryableCollection containing chunks
    :param key: The lookup key to delete
    """
    collection.delete_many({"chunk_key": key})


def purge_chunked(collection: "RetryableCollection") -> None:
    """
    Delete all documents in the chunks collection.

    :param collection: The RetryableCollection to purge
    """
    collection.delete_many({})
