from typing import TYPE_CHECKING
from unittest.mock import patch

import mongomock
import pytest
from pynenc import PynencBuilder

from pynenc_mongo.util.mongo_client import PynencMongoClient

if TYPE_CHECKING:
    from collections.abc import Generator

    from pynenc.app import Pynenc


@pytest.fixture
def patch_mongo_client() -> "Generator[None, None, None]":
    """
    Patch PyMongoClient in pynenc_mongo.util.mongo_client to use mongomock.MongoClient.

    Use this fixture to ensure all MongoDB operations are mocked for unit tests.
    """
    # Reset the singleton so patching works for each test
    PynencMongoClient._instances.clear()
    with patch("pynenc_mongo.util.mongo_client.PyMongoClient", mongomock.MongoClient):
        yield
    PynencMongoClient._instances.clear()


@pytest.fixture(scope="function")
def app_instance(patch_mongo_client: None) -> "Generator['Pynenc', None, None]":
    """
    Provides a Pynenc app instance with a mongomock MongoDB backend for unit tests.

    Relies on patch_mongo_client fixture to ensure mongomock is used for all MongoDB operations.

    :return: Generator yielding a Pynenc app instance with mongomock backend
    """
    app = PynencBuilder().mongo(url="mongodb://localhost:27017/test").build()
    yield app
    app.purge()  # Clean up after test
