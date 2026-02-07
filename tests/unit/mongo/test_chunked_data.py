"""Unit tests for chunked data compression and splitting utilities.

These are pure Python tests with no MongoDB dependency.
"""

from pynenc_mongo.util.chunked_data import (
    compress,
    decompress,
    exceeds_bson_threshold,
    reassemble_chunks,
    split_into_chunks,
)


def test_compress_decompress_roundtrip() -> None:
    """Verify compress/decompress roundtrip preserves original data."""
    original = "hello world" * 100
    compressed = compress(original)
    assert decompress(compressed) == original


def test_compress_reduces_size() -> None:
    """Verify compression actually reduces repetitive data."""
    original = "a" * 10_000
    compressed = compress(original)
    assert len(compressed) < len(original)


def test_compress_handles_unicode() -> None:
    """Verify compression handles multi-byte unicode characters."""
    original = "こんにちは世界🚀" * 50
    compressed = compress(original)
    assert decompress(compressed) == original


def test_compress_empty_string() -> None:
    """Verify compression handles empty strings."""
    compressed = compress("")
    assert decompress(compressed) == ""


def test_split_into_chunks_single() -> None:
    """Small data should produce a single chunk."""
    data = b"small payload"
    chunks = split_into_chunks(data, chunk_size=1024)
    assert len(chunks) == 1
    assert chunks[0] == data


def test_split_into_chunks_multiple() -> None:
    """Data larger than chunk_size should split into multiple chunks."""
    data = b"x" * 100
    chunks = split_into_chunks(data, chunk_size=30)
    assert len(chunks) == 4  # 30 + 30 + 30 + 10
    assert chunks[0] == b"x" * 30
    assert chunks[-1] == b"x" * 10


def test_split_exact_boundary() -> None:
    """Data exactly divisible by chunk_size should not produce an empty trailing chunk."""
    data = b"y" * 60
    chunks = split_into_chunks(data, chunk_size=30)
    assert len(chunks) == 2
    assert all(len(c) == 30 for c in chunks)


def test_reassemble_chunks_roundtrip() -> None:
    """Split then reassemble should produce the original bytes."""
    data = b"some binary data" * 50
    chunks = split_into_chunks(data, chunk_size=100)
    assert reassemble_chunks(chunks) == data


def test_reassemble_empty_list() -> None:
    """Reassembling an empty list should return empty bytes."""
    assert reassemble_chunks([]) == b""


def test_exceeds_bson_threshold_below() -> None:
    """Data below the threshold should not exceed it."""
    assert not exceeds_bson_threshold("small", threshold=1000)


def test_exceeds_bson_threshold_above() -> None:
    """Data above the threshold should exceed it."""
    large = "x" * 2000
    assert exceeds_bson_threshold(large, threshold=1000)


def test_exceeds_bson_threshold_exact() -> None:
    """Data at exactly the threshold should exceed it (>=)."""
    exact = "x" * 1000
    assert exceeds_bson_threshold(exact, threshold=1000)


def test_exceeds_bson_threshold_one_below() -> None:
    """Data one byte below threshold should not exceed it."""
    below = "x" * 999
    assert not exceeds_bson_threshold(below, threshold=1000)


def test_exceeds_bson_threshold_multibyte() -> None:
    """Threshold check should use byte length, not character count."""
    # Each CJK char is 3 bytes in UTF-8
    cjk_string = "你" * 334  # 334 * 3 = 1002 bytes
    assert exceeds_bson_threshold(cjk_string, threshold=1000)

    shorter = "你" * 333  # 333 * 3 = 999 bytes
    assert not exceeds_bson_threshold(shorter, threshold=1000)


def test_full_pipeline_compress_split_reassemble_decompress() -> None:
    """End-to-end test: compress, split, reassemble, decompress."""
    original = "The quick brown fox jumps over the lazy dog. " * 500
    compressed = compress(original)
    chunks = split_into_chunks(compressed, chunk_size=256)
    reassembled = reassemble_chunks(chunks)
    result = decompress(reassembled)
    assert result == original


def test_chunk_threshold_from_config() -> None:
    """Verify the config field controls the threshold."""
    from pynenc_mongo.conf.config_mongo import ConfigMongo

    # Default threshold
    conf_default = ConfigMongo()
    assert conf_default.chunk_threshold == 15 * 1024 * 1024  # 15MB

    # Custom threshold
    conf_custom = ConfigMongo({"chunk_threshold": 5000})
    assert conf_custom.chunk_threshold == 5000
