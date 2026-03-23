# mongo_collection_specs.py
"""
Collection specifications for all MongoDB-based Pynenc components.
Centralized location for all collection definitions and indexes.
"""

import hashlib
import re
from dataclasses import dataclass, field

from pymongo import IndexModel

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_client import PynencMongoClient, RetryableCollection


def sanitize_collection_prefix(app_id: str, max_prefix_len: int = 40) -> str:
    """
    Sanitize an app_id for safe use as a MongoDB collection name prefix.

    Replaces any character that is not alphanumeric or underscore with an
    underscore, prepends an underscore if the result starts with a digit,
    then truncates to fit within ``max_prefix_len`` (including an 8-character
    deterministic hash suffix). The hash is always computed from the full
    original app_id, so different app_ids that truncate to the same string
    still produce distinct prefixes.

    :param str app_id: The application identifier to sanitize
    :param int max_prefix_len: Maximum length of the returned prefix (default 40)
    :return: A string safe for use in MongoDB collection names
    """
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", app_id)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    sanitized = sanitized or "_default"
    hash_suffix = hashlib.sha256(app_id.encode()).hexdigest()[:8]
    # Reserve space for underscore + hash_suffix
    max_name_len = max_prefix_len - len(hash_suffix) - 1
    sanitized = sanitized[:max_name_len]
    return f"{sanitized}_{hash_suffix}"


@dataclass
class CollectionSpec:
    """Specification for a collection with its indexes"""

    name: str
    indexes: list[IndexModel] = field(default_factory=list)


class MongoCollections:
    """Abstract base class for MongoDB collections with app_id-based prefix enforcement."""

    def __init__(self, conf: ConfigMongo, prefix: str, app_id: str):
        self.conf = conf
        self.prefix = prefix.rstrip("_") + "_"  # Normalize component prefix
        self._app_prefix = sanitize_collection_prefix(app_id)

    def _prefixed_name(self, base_name: str) -> str:
        """Return the collection name prefixed with the sanitized app_id."""
        return f"{self._app_prefix}_{base_name}"

    def instantiate_retriable_coll(
        self, spec: "CollectionSpec"
    ) -> "RetryableCollection":
        """
        Instantiate a RetryableCollection for the given CollectionSpec.

        Prepends the sanitized app_id prefix to the collection name so that
        different apps sharing the same database are fully isolated.

        :param spec: Specification for the collection
        :return: RetryableCollection instance
        """
        prefixed_spec = CollectionSpec(
            name=self._prefixed_name(spec.name),
            indexes=spec.indexes,
        )
        client = PynencMongoClient.get_instance(self.conf)
        return client.get_collection(prefixed_spec)

    def purge_all(self) -> None:
        """Purge all collections belonging to this app."""
        for attr_name in dir(self):
            if attr_name.startswith("_"):
                continue
            col = getattr(self, attr_name)
            if isinstance(col, RetryableCollection):
                col.delete_many({})
