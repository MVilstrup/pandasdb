import importlib
import os
import pkgutil
from collections import defaultdict

__TAGS__ = defaultdict(list)

from pandasdb.transformer.transforms.transformer import Transformer
from pandasdb.transformer.utils import invert_tranformer_name


def tag(*tag_names):
    def wrapper(transformer):
        for tag in tag_names:
            __TAGS__[tag].append(transformer)

        return transformer

    return wrapper


def load_all_tags(folder=None, recursive=True):
    """ Import all submodules of a module, recursively, including subpackages
    :param recursive: bool
    :param package: package (name or actual module)
    :type package: str | module
    :rtype: dict[str, types.ModuleType]
    """
    if folder is None:
        load_all_tags(list(os.walk(os.getcwd())))
    elif isinstance(folder, list):
        for sub_folder in folder:
            load_all_tags(sub_folder)
    else:
        if isinstance(folder, str):
            package = importlib.import_module(folder)
        else:
            package = folder

        results = {}
        for loader, name, is_pkg in pkgutil.walk_packages(package.__path__):
            full_name = package.__name__ + '.' + name
            results[full_name] = importlib.import_module(full_name)
            if recursive and is_pkg:
                results.update(load_all_tags(full_name))

        return results


def generate_tags(from_folders=None):
    load_all_tags(from_folders)

    for tag, transformers in __TAGS__.items():
        transformer_pairs = {}
        for transformer in transformers:
            if isinstance(transformers, Transformer):
                transformer_pairs[invert_tranformer_name(transformer.__name__)] = transformer

        TagType = type(tag, (type,), transformer_pairs)
        yield tag, TagType
