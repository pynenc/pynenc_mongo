"""
Mongo plugin builder extensions for Pynenc.

This module contains the Mongo-specific builder methods that will be moved to the
pynenc-mongo plugin package.

Key components:
- MongoBuilderPlugin: Plugin class that registers Mongo methods
- mongo(): Main method for full Mongo stack configuration
- mongo_arg_cache(): Mongo-specific argument caching method
- mongo_trigger(): Mongo-specific trigger system method
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pynenc.builder import PynencBuilder


class MongoBuilderPlugin:
    """Mongo plugin that provides builder methods for Mongo backend configuration."""

    @staticmethod
    def register_builder_methods(builder_class: type["PynencBuilder"]) -> None:
        """
        Register Mongo builder methods with PynencBuilder.

        This method is called automatically when the plugin is discovered via entry points.

        :param type["PynencBuilder"] builder_class: The PynencBuilder class to extend
        """
        # Register main Mongo method
        builder_class.register_plugin_method("mongo", mongo)

        # Register component-specific methods
        builder_class.register_plugin_method("mongo_arg_cache", mongo_arg_cache)
        builder_class.register_plugin_method("mongo_trigger", mongo_trigger)

        # Register configuration validator
        builder_class.register_plugin_validator(validate_mongo_config)


def mongo(
    builder: "PynencBuilder", url: str | None = None, db: int | None = None
) -> "PynencBuilder":
    """
    Configure Mongo components for the Pynenc application.

    This sets up all Mongo-related components (orchestrator, broker, state backend,
    and argument cache) to use Mongo as their backend.

    :param PynencBuilder builder: The PynencBuilder instance
    :param str | None url: The Mongo URL to connect to. If specified, overrides all other connection
        parameters including host, port, and db
    :param int | None db: The Mongo database number to use. Only valid when url is not provided.
        If url is provided, the database should be specified in the URL itself
    :return: The builder instance for method chaining
    :raises ValueError: If both url and db are provided, since url takes precedence
    """
    if url and db is not None:
        raise ValueError(
            "Cannot specify both 'url' and 'db' parameters. "
            "When using 'url', specify the database in the URL (e.g., 'mongo://host:port/db'). "
            "The 'url' parameter overrides all other connection settings."
        )

    if url:
        builder._config["mongo_url"] = url
    elif db is not None:
        builder._config["mongo_db"] = db

    builder._config.update(
        {
            "orchestrator_cls": "MongoOrchestrator",
            "broker_cls": "MongoBroker",
            "state_backend_cls": "MongoStateBackend",
            "arg_cache_cls": "MongoArgCache",
            "trigger_cls": "MongoTrigger",
        }
    )
    builder._plugin_components.add("mongo")
    builder._using_memory_components = False
    return builder


def mongo_arg_cache(
    builder: "PynencBuilder",
    min_size_to_cache: int = 1024,
    local_cache_size: int = 1024,
) -> "PynencBuilder":
    """
    Configure Mongo-based argument caching.

    This method configures the Mongo argument cache with the specified parameters.
    It requires that Mongo components have been configured either through mongo()
    or through configuration files.

    :param PynencBuilder builder: The PynencBuilder instance
    :param int min_size_to_cache: Minimum string length (in characters) required to cache an argument.
        Arguments smaller than this size will be passed directly. Default is 1024 characters (roughly 1KB)
    :param int local_cache_size: Maximum number of items to cache locally. Default is 1024
    :return: The builder instance for method chaining
    :raises ValueError: If Mongo configuration is not present
    """
    if "mongo" not in builder._plugin_components and "mongo_url" not in builder._config:
        raise ValueError(
            "Mongo arg cache requires mongo configuration. Call mongo() first."
        )

    builder._config.update(
        {
            "arg_cache_cls": "MongoArgCache",
            "min_size_to_cache": min_size_to_cache,
            "local_cache_size": local_cache_size,
        }
    )
    builder._plugin_components.add("mongo")
    return builder


def mongo_trigger(
    builder: "PynencBuilder",
    scheduler_interval_seconds: int = 60,
    enable_scheduler: bool = True,
) -> "PynencBuilder":
    """
    Configure Mongo-based trigger system.

    This method configures the Mongo trigger system with the specified parameters.
    It requires that Mongo components have been configured either through mongo()
    or through configuration files.

    :param PynencBuilder builder: The PynencBuilder instance
    :param int scheduler_interval_seconds: Interval in seconds for the scheduler to check for time-based triggers.
        Default is 60 seconds (1 minute)
    :param bool enable_scheduler: Whether to enable the scheduler for time-based triggers.
        Default is True
    :return: The builder instance for method chaining
    :raises ValueError: If Mongo configuration is not present
    """
    if "mongo" not in builder._plugin_components and "mongo_url" not in builder._config:
        raise ValueError(
            "Mongo trigger requires mongo configuration. Call mongo() first."
        )

    builder._config.update(
        {
            "trigger_cls": "MongoTrigger",
            "scheduler_interval_seconds": scheduler_interval_seconds,
            "enable_scheduler": enable_scheduler,
        }
    )
    builder._plugin_components.add("mongo")
    return builder


def validate_mongo_config(config: dict[str, Any]) -> None:
    """
    Validate Mongo plugin configuration.

    This function validates that Mongo configuration is present when Mongo components
    are being used. It's called automatically during the build process.

    :param dict[str, Any] config: The builder configuration dictionary
    :raises ValueError: If Mongo configuration is invalid
    """
    # Check if any Mongo components are being used
    uses_mongo = any(
        config.get(key, "").startswith("Mongo")
        for key in [
            "orchestrator_cls",
            "broker_cls",
            "state_backend_cls",
            "arg_cache_cls",
            "trigger_cls",
        ]
    )

    if uses_mongo:
        # Ensure Mongo connection configuration is present
        has_mongo_config = any(
            [
                config.get("mongo_url"),
                config.get("mongo_host"),
                config.get("mongo_db") is not None,
            ]
        )

        if not has_mongo_config:
            raise ValueError(
                "Mongo components require connection configuration. "
                "Set mongo_url, mongo_host, or call mongo() with connection parameters."
            )
