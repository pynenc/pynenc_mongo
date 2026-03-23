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


@pytest.fixture(scope="session")
def mongo_container() -> "Generator[PatchedMongoDbContainer, None, None]":
    """
    Session-scoped MongoDB container - created once for all tests.

    :return: Generator yielding a running MongoDB container
    :raises RuntimeError: If Docker is not running or accessible
    """
    try:
        container = PatchedMongoDbContainer("mongo:3.6")
        with container as mongo_container:
            yield mongo_container
    except DockerException as e:
        raise RuntimeError(
            "Docker is not running or not accessible. Please start Docker to run integration tests."
        ) from e


@pytest.fixture(scope="session")
def mongo_url(mongo_container: PatchedMongoDbContainer) -> str:
    """
    Session-scoped MongoDB URL derived from the container.

    Computed once and reused by all tests, avoiding repeated port lookups.

    :param mongo_container: Session-scoped MongoDB container
    :return: MongoDB connection URL string
    """
    return (
        f"mongodb://test:test@{mongo_container.get_container_host_ip()}:"
        f"{mongo_container.get_exposed_port(27017)}/pynenc?authSource=admin"
    )


@pytest.fixture(scope="function")
def app_instance_builder(
    mongo_url: str,
) -> "PynencBuilder":
    """
    Fixture that provides a fresh Pynenc app instance builder with Mongo backend.

    The MongoDB URL is session-scoped (computed once), while the builder is
    function-scoped so each test gets a clean builder to configure.

    :param mongo_url: Session-scoped MongoDB connection URL
    :return: PynencBuilder instance configured for Mongo
    """
    return PynencBuilder().mongo(url=mongo_url)


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
    app.purge()
