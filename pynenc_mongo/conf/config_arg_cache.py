from pynenc.conf.config_arg_cache import ConfigArgCache

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigArgCacheMongo(ConfigArgCache, ConfigMongo):
    """Specific Configuration for the MongoDB Argument Cache"""
