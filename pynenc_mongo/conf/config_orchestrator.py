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
    :cvar ConfigField[int] lock_max_retries:
        Maximum number of retry attempts to acquire the transition lock
        before raising InvocationStatusRaceConditionError. Defaults to 3.
    :cvar ConfigField[float] lock_retry_base_delay:
        Base delay in seconds between lock retry attempts, multiplied by
        the attempt number for linear backoff. Defaults to 0.05.
    """

    stale_lock_threshold_seconds = ConfigField(5.0)
    lock_max_retries = ConfigField(3)
    lock_retry_base_delay = ConfigField(0.05)
