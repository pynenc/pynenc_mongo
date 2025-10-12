import ast
import inspect

from pynenc_mongo.conf.config_mongo import ConfigMongo
from pynenc_mongo.util.mongo_collections import CollectionSpec, MongoCollections


def get_subclasses(cls: type) -> list[type]:
    """Recursively get all subclasses of a class."""
    subclasses = cls.__subclasses__()
    return subclasses + [sub for c in subclasses for sub in get_subclasses(c)]


def extract_collection_specs(cls: type) -> list[CollectionSpec]:
    """Extract CollectionSpec objects from @cached_property methods."""
    specs = []
    source = inspect.getsource(cls)
    tree = ast.parse(source)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and any(
            isinstance(dec, ast.Name) and dec.id == "cached_property"
            for dec in node.decorator_list
        ):
            for stmt in node.body:
                for subnode in ast.walk(stmt):
                    if (
                        isinstance(subnode, ast.Call)
                        and isinstance(subnode.func, ast.Name)
                        and subnode.func.id == "CollectionSpec"
                    ):
                        name = None
                        indexes = []
                        for kw in subnode.keywords:
                            if (
                                kw.arg == "name"
                                and isinstance(kw.value, ast.Constant)
                                and isinstance(kw.value.value, str)
                            ):
                                name = kw.value.value
                            elif kw.arg == "indexes" and isinstance(kw.value, ast.List):
                                indexes = [None] * len(kw.value.elts)
                        if name:
                            specs.append(CollectionSpec(name=name, indexes=indexes))
    return specs


def test_collection_specs() -> None:
    """Validate all MongoCollections subclasses for prefix and indexes."""
    for cls in get_subclasses(MongoCollections):
        # Get the prefix from the __init__ signature
        # prefix_arg = cls.__init__.__code__.co_varnames[2]  # Assuming prefix is 3rd arg after self, conf
        dummy_conf = ConfigMongo({}, None)
        instance = cls.__new__(cls)  # type: ignore
        cls.__init__(instance, dummy_conf)  # type: ignore
        prefix_value = instance.prefix

        specs = extract_collection_specs(cls)
        for spec in specs:
            assert spec.name.startswith(prefix_value), (
                f"Collection '{spec.name}' in {cls.__name__} does not start with prefix '{prefix_value}'"
            )
            assert len(spec.indexes) > 0, (
                f"Collection '{spec.name}' in {cls.__name__} has no indexes defined"
            )
