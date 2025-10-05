from pynenc.conf.config_trigger import ConfigTrigger

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigTriggerMongo(ConfigTrigger, ConfigMongo):
    """Specific Configuration for the MongoDB Trigger"""
