"""Unit tests for GridFS storage configuration and threshold logic.

Note: GridFS operations require real MongoDB and don't work with mongomock.
Actual storage/retrieval tests are in integration tests.
"""

from typing import TYPE_CHECKING

import pytest

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_client import GridFSStorage

if TYPE_CHECKING:
    pass


@pytest.fixture
def mongo_conf() -> ConfigMongo:
    """Fixture for a ConfigMongo instance with a lower GridFS threshold."""
    return ConfigMongo(
        {
            "mongo_url": "mongodb://localhost:27017/test",
            "gridfs_threshold": 1000,  # 1KB for testing
        }
    )


@pytest.fixture
def gridfs_storage(mongo_conf: ConfigMongo) -> GridFSStorage:
    """Fixture for GridFSStorage instance."""
    return GridFSStorage(mongo_conf, collection_prefix="test_gridfs")


def test_gridfs_storage_initialization(
    gridfs_storage: GridFSStorage, mongo_conf: ConfigMongo
) -> None:
    """Test GridFSStorage initializes correctly."""
    assert gridfs_storage.conf == mongo_conf
    assert gridfs_storage._collection_prefix == "test_gridfs"


def test_should_use_gridfs_below_threshold(gridfs_storage: GridFSStorage) -> None:
    """Test that small data doesn't trigger GridFS."""
    small_data = "x" * 500  # 500 bytes
    assert not gridfs_storage.should_use_gridfs(small_data)


def test_should_use_gridfs_above_threshold(gridfs_storage: GridFSStorage) -> None:
    """Test that large data triggers GridFS."""
    large_data = "x" * 2000  # 2KB
    assert gridfs_storage.should_use_gridfs(large_data)


def test_should_use_gridfs_bytes(gridfs_storage: GridFSStorage) -> None:
    """Test should_use_gridfs works with bytes."""
    large_bytes = b"x" * 2000
    assert gridfs_storage.should_use_gridfs(large_bytes)


def test_should_use_gridfs_at_exact_threshold(gridfs_storage: GridFSStorage) -> None:
    """Test behavior at exact threshold boundary."""
    # Exactly at threshold should use GridFS
    exact_data = "x" * 1000
    assert gridfs_storage.should_use_gridfs(exact_data)

    # One byte below should not
    below_data = "x" * 999
    assert not gridfs_storage.should_use_gridfs(below_data)


def test_is_gridfs_key_detection() -> None:
    """Test GridFS key detection."""
    gridfs_key = f"{GridFSStorage.GRIDFS_PREFIX}some_key"
    plain_key = "some_key"

    assert GridFSStorage.is_gridfs_key(gridfs_key)
    assert not GridFSStorage.is_gridfs_key(plain_key)


def test_gridfs_prefix_constant() -> None:
    """Test that GridFS prefix is properly defined."""
    assert GridFSStorage.GRIDFS_PREFIX == "__gridfs__:"
    assert GridFSStorage.GRIDFS_PREFIX.endswith(":")


def test_different_collection_prefixes() -> None:
    """Test that different GridFSStorage instances can have different prefixes."""
    conf = ConfigMongo({"mongo_url": "mongodb://localhost:27017/test"})
    storage1 = GridFSStorage(conf, collection_prefix="prefix1")
    storage2 = GridFSStorage(conf, collection_prefix="prefix2")

    assert storage1._collection_prefix == "prefix1"
    assert storage2._collection_prefix == "prefix2"
    assert storage1.conf is storage2.conf  # Same config


def test_default_threshold_from_config() -> None:
    """Test that GridFS threshold comes from config."""
    # Default threshold
    conf_default = ConfigMongo()
    storage_default = GridFSStorage(conf_default)
    assert storage_default.conf.gridfs_threshold == 15 * 1024 * 1024  # 15MB

    # Custom threshold
    conf_custom = ConfigMongo({"gridfs_threshold": 5000})
    storage_custom = GridFSStorage(conf_custom)
    assert storage_custom.conf.gridfs_threshold == 5000
