import pytest
from pynenc import PynencBuilder


@pytest.mark.usefixtures("patch_mongo_client")
def test_mongo_builder_sets_url_and_precedence() -> None:
    app = (
        PynencBuilder()
        .mongo(url="mongodb://user:pass@host:1234/db?authSource=admin")
        .build()
    )
    conf = app.orchestrator.conf
    assert conf.mongo_url == "mongodb://user:pass@host:1234/db?authSource=admin"
    # All other connection params should be default
    assert conf.mongo_db == "pynenc"
    assert conf.mongo_host == "localhost"
    assert conf.mongo_port == 27017
    assert conf.mongo_username == ""
    assert conf.mongo_password == ""
    assert conf.mongo_auth_source == ""
    app.purge()


@pytest.mark.usefixtures("patch_mongo_client")
def test_mongo_builder_sets_individual_params() -> None:
    app = (
        PynencBuilder()
        .mongo(
            db="mydb",
            host="myhost",
            port=12345,
            username="alice",
            password="secret",
            auth_source="admin",
        )
        .build()
    )
    conf = app.orchestrator.conf
    assert conf.mongo_url == ""
    assert conf.mongo_db == "mydb"
    assert conf.mongo_host == "myhost"
    assert conf.mongo_port == 12345
    assert conf.mongo_username == "alice"
    assert conf.mongo_password == "secret"
    assert conf.mongo_auth_source == "admin"
    app.purge()


@pytest.mark.usefixtures("patch_mongo_client")
@pytest.mark.parametrize(
    "kwargs",
    [
        {"url": "mongodb://host", "db": "should_fail"},
        {"url": "mongodb://host", "host": "should_fail"},
        {"url": "mongodb://host", "port": 1234},
        {"url": "mongodb://host", "username": "user"},
        {"url": "mongodb://host", "password": "pw"},
        {"url": "mongodb://host", "auth_source": "admin"},
    ],
)
def test_mongo_builder_raises_on_url_and_other_params(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        PynencBuilder().mongo(**kwargs)


@pytest.mark.usefixtures("patch_mongo_client")
def test_mongo_builder_partial_params() -> None:
    app = PynencBuilder().mongo(host="hostonly").build()
    conf = app.orchestrator.conf
    assert conf.mongo_host == "hostonly"
    assert conf.mongo_url == ""
    app.purge()
