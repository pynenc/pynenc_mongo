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


@pytest.fixture(scope="function")
def app_instance_builder(
    mongo_container: PatchedMongoDbContainer,
) -> "Generator['PynencBuilder', None, None]":
    """
    Fixture that provides a Pynenc app instance builder with a real Mongo backend.

    Uses a session-scoped container for performance.

    :param mongo_container: Session-scoped MongoDB container
    :return: Generator yielding a PynencBuilder instance with Mongo backend
    """
    mongo_url = (
        f"mongodb://test:test@{mongo_container.get_container_host_ip()}:"
        f"{mongo_container.get_exposed_port(27017)}/test?authSource=admin"
    )
    yield PynencBuilder().mongo(url=mongo_url)


@pytest.fixture(scope="function")
def app_instance(
    app_instance_builder: "PynencBuilder",
    mongo_container: PatchedMongoDbContainer,
) -> "Generator['Pynenc', None, None]":
    """
    Fixture that provides a Pynenc app instance built from the app_instance_builder.

    :param app_instance_builder: Fixture providing a PynencBuilder instance
    :param mongo_container: MongoDB container for cleanup
    :return: Generator yielding a built Pynenc app instance
    """
    app = app_instance_builder.build()
    # app.purge()
    yield app
    app.purge()  # Clean up app data after each test

    # # Clean up: drop all non-system databases after app is done
    # mongo_url = (
    #     f"mongodb://test:test@{mongo_container.get_container_host_ip()}:"
    #     f"{mongo_container.get_exposed_port(27017)}/test?authSource=admin"
    # )
    # client = MongoClient(mongo_url)
    # try:
    #     for db_name in client.list_database_names():
    #         if db_name not in ("admin", "local", "config"):
    #             client.drop_database(db_name)
    # finally:
    #     client.close()
