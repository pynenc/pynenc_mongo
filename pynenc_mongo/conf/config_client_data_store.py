from pynenc.conf.config_client_data_store import ConfigClientDataStore

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigClientDataStoreMongo(ConfigClientDataStore, ConfigMongo):
    """Specific Configuration for the MongoDB Client Data Store"""
