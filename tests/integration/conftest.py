from typing import TYPE_CHECKING

import pytest
from docker.errors import DockerException
from pynenc import PynencBuilder
from testcontainers.mongodb import MongoDbContainer

if TYPE_CHECKING:
    from collections.abc import Generator

    from pynenc import Pynenc


class PatchedMongoDbContainer(MongoDbContainer):
    """
    MongoDbContainer subclass that waits for the correct log output.
    """

    def _connect(self) -> None:
        # Wait for the actual log line emitted by the MongoDB image
        from testcontainers.core.waiting_utils import wait_for_logs

        wait_for_logs(self, "waiting for connections")


@pytest.fixture(scope="function")
def app_instance_builder() -> "Generator['PynencBuilder', None, None]":
    """
    Fixture that provides a Pynenc app instance with a real Mongo backend,
    using testcontainers to start a Mongo container for integration tests.

    :return: Generator yielding a Pynenc app instance with Mongo backend
    :raises RuntimeError: If Docker is not running or accessible
    """
    try:
        container = PatchedMongoDbContainer("mongo:3.6")
        with container as mongo_container:
            mongo_url = (
                f"mongodb://test:test@{mongo_container.get_container_host_ip()}:"
                f"{mongo_container.get_exposed_port(27017)}/test?authSource=admin"
            )
            yield PynencBuilder().mongo(url=mongo_url)
    except DockerException as e:
        raise RuntimeError(
            "Docker is not running or not accessible. Please start Docker to run integration tests."
        ) from e


@pytest.fixture(scope="function")
def app_instance(
    app_instance_builder: "PynencBuilder",
) -> "Generator['Pynenc', None, None]":
    """
    Fixture that provides a Pynenc app instance built from the app_instance_builder.

    :param app_instance_builder: Fixture providing a PynencBuilder instance
    :return: Generator yielding a built Pynenc app instance
    """
    app = app_instance_builder.build()
    yield app
