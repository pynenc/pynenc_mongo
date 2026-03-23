"""
Utilities for compressing and splitting large strings into chunks.

Provides functions to compress data with zlib and split it into chunks that
fit within MongoDB's BSON document size limit. Chunks are simple byte sequences
with an index for ordered reassembly.

Key components:
- compress / decompress: zlib-based string compression
- split_into_chunks / reassemble_chunks: size-based splitting and reassembly
- exceeds_bson_threshold: check if data needs chunking
"""

import logging
import zlib
from typing import Any

from bson import BSON

logger = logging.getLogger(__name__)


def compress(data: str) -> bytes:
    """
    Compress a string using zlib.

    Uses compression level 6 for balanced speed/ratio tradeoff.
    Level 6 is significantly faster than default (9) with minimal size difference.

    :param data: UTF-8 string to compress
    :return: Compressed bytes
    """
    return zlib.compress(data.encode("utf-8"), level=6)


def decompress(data: bytes) -> str:
    """
    Decompress zlib-compressed bytes back to a string.

    :param data: Compressed bytes
    :return: Decompressed UTF-8 string
    """
    return zlib.decompress(data).decode("utf-8")


def split_into_chunks(data: bytes, chunk_size: int) -> list[bytes]:
    """
    Split bytes into ordered chunks of at most chunk_size bytes.

    :param data: The bytes to split
    :param chunk_size: Maximum size per chunk in bytes
    :return: List of byte chunks in order
    """
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def reassemble_chunks(chunks: list[bytes]) -> bytes:
    """
    Reassemble ordered chunks into the original bytes.

    :param chunks: List of byte chunks in order
    :return: Reassembled bytes
    """
    return b"".join(chunks)


def exceeds_bson_threshold(data: dict[str, str] | str, threshold: int) -> bool:
    """
    Check if a string or dict of strings exceeds the size threshold for a single BSON document.

    :param data: The string or dict of strings to check
    :param threshold: Size threshold in bytes
    :return: True if the encoded data exceeds the threshold
    """
    doc: dict[str, Any] = {"payload": data} if isinstance(data, str) else data
    encoded = BSON.encode(doc)
    return len(encoded) > threshold
