from cistell import ConfigField
from pynenc.conf.config_base import ConfigPynencBase


class ConfigMongo(ConfigPynencBase):
    """
    Specific Configuration for any Mongo client.

    This class provides configuration settings specific to Mongo clients, allowing
    for customization of the Mongo connection used in the system.

    :cvar ConfigField[str] mongo_username:
        The username to use when connecting to the Mongo server. Defaults to an empty
        string, indicating that no username is provided.

    :cvar ConfigField[str] mongo_password:
        The password to use when connecting to the Mongo server. Defaults to an empty
        string, indicating that no password is provided.

    :cvar ConfigField[str] mongo_host:
        The hostname of the Mongo server. Defaults to 'localhost', specifying that
        the Mongo server is expected to be running on the same machine as the client.

    :cvar ConfigField[int] mongo_port:
        The port number on which the Mongo server is listening. Defaults to 27017,
        which is the default port for Mongo.

    :cvar ConfigField[str] mongo_db:
        The database name to connect to on the Mongo server. Defaults to 'pynenc',

    :cvar ConfigField[str] mongo_url:
        The URL of the Mongo server. This field is intended to be used when the Mongo
        server is accessed via a URL rather than a hostname and port. Defaults to an
        empty string, indicating that no URL is provided.
        If specified will override all other connection parameters.

    :cvar ConfigField[str] mongo_auth_source:
        The authentication source database for MongoDB. Defaults to an empty string,
        which means the default authentication database will be used. Set this if your
        MongoDB user is stored in a database other than the one you are connecting to.

    :cvar ConfigField[int] mongo_pool_max_connections:
        The maximum number of connections allowed in the MongoDB connection pool.
        Defaults to 100.

    :cvar ConfigField[int] socket_timeout:
        The socket timeout in seconds for MongoDB operations. Defaults to 10.

    :cvar ConfigField[int] socket_connect_timeout:
        The connection timeout in seconds for establishing a MongoDB connection.
        Defaults to 10.

    :cvar ConfigField[int] max_retries:
        The maximum number of retry attempts for MongoDB operations. Defaults to 3.

    :cvar ConfigField[float] retry_base_delay:
        The base delay in seconds between retry attempts for MongoDB operations.
        Defaults to 0.1.

    Example usage of the `ConfigMongo` class involves initializing it with specific
    values for host, port, database, or authentication source, or relying on the defaults
    for a standard Mongo setup.
    """

    mongo_username = ConfigField("")
    mongo_password = ConfigField("")
    mongo_host = ConfigField("localhost")
    mongo_port = ConfigField(27017)
    mongo_db = ConfigField("pynenc")
    mongo_url = ConfigField("")
    mongo_auth_source = ConfigField("")

    # Mongo connection pool settings
    mongo_pool_max_connections = ConfigField(100)
    socket_timeout = ConfigField(10)
    socket_connect_timeout = ConfigField(10)

    # Connection management settings
    max_retries = ConfigField(3)
    retry_base_delay = ConfigField(0.1)
