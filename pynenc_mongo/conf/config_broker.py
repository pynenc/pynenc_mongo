from pynenc.conf.config_broker import ConfigBroker

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigBrokerMongo(ConfigBroker, ConfigMongo):
    """Specific Configuration for the MongoDB Broker"""
