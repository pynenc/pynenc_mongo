from pynenc.conf.config_orchestrator import ConfigOrchestrator

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigOrchestratorMongo(ConfigOrchestrator, ConfigMongo):
    """Specific Configuration for the Mongo Orchestrator"""

    auto_final_invocation_purge_hours: int = 24
    max_pending_seconds: int = 3600
