from cistell import ConfigField
from pynenc.conf.config_orchestrator import ConfigOrchestrator

from pynenc_mongo.conf.config_mongo import ConfigMongo


class ConfigOrchestratorMongo(ConfigOrchestrator, ConfigMongo):
    """
    MongoDB-specific configuration for the orchestrator.

    Inherits all settings from ConfigOrchestrator.

    :cvar ConfigField[float] stale_lock_threshold_seconds:
        Maximum age in seconds for a transition lock claim before it is
        considered stale. When a worker crashes while holding a lock, its
        claim will exceed this threshold and be automatically cleared by
        the next worker that attempts the same transition. The lock only
        protects a very fast read-validate-write cycle, so this value
        should be kept low. Defaults to 5.0.
    """

    stale_lock_threshold_seconds = ConfigField(5.0)
