from pynenc.conf.config_state_backend import ConfigStateBackend

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigStateBackendMongo(ConfigStateBackend, ConfigMongo):
    """Specific Configuration for the MongoDB State Backend."""
