from collections import defaultdict
from functools import wraps
from pprint import pprint

from pandasdb.src.transformer import TransformationCore


def metaclass_creator(meta):
    @wraps(meta)
    def metaclass_wrapper(cls):
        __name = str(cls.__name__)
        __bases = tuple(cls.__bases__)
        __dict = dict(cls.__dict__)

        for each_slot in __dict.get("__slots__", tuple()):
            __dict.pop(each_slot, None)

        __dict["__metaclass__"] = meta

        __dict["__wrapped__"] = cls

        return meta(__name, __bases, __dict)

    return metaclass_wrapper


def assemble_transformer(transformer_cls, core_template, cls_attributes):
    class TransformerCoreAttributes(type):
        def __new__(meta, name, bases, dct):
            dct.update({
                "_core_": core_template,
                **cls_attributes,
            })
            return super(TransformerCoreAttributes, meta).__new__(meta, name, bases, dct)

    with_core = metaclass_creator(TransformerCoreAttributes)
    return with_core(transformer_cls)


def merge_templates(child, parent):
    locations = [
        "_columns_", "_aggregations_", "_pre_conditions_", "_post_conditions_",
        "_split_conditions_", "_groups_", "_splits_", "_indexes_", "_parameters_"
    ]
    if not parent:
        return child
    elif not child:
        return parent
    else:
        merged = parent
        for location in locations:
            for name, value in child[location].items():
                merged[location][name] = value

        return merged


def recursive_merge(parent):
    if issubclass(parent, TransformationCore) and hasattr(parent, "_core_"):
        parent_core = parent._core_

        if issubclass(parent, TransformationCore):
            for grand_parent in parent.__bases__:
                parent_core = merge_templates(parent_core, recursive_merge(grand_parent))

        return parent_core


def transformer(cls):
    from pandasdb.src.transformer import TransformationCache
    from pandasdb.src.transformer import Transformer
    from pandasdb.src.transformer import to_transformer_name

    transformer_name = to_transformer_name(cls.__name__)

    inherited_template = {}
    for parent in cls.__bases__:
        inherited_template = merge_templates(inherited_template, recursive_merge(parent))

    new_template = TransformationCache.get_template(to_transformer_name(cls.__name__))

    transformer_template = merge_templates(inherited_template, new_template)

    # Copy all functionality of the original class
    cls_attributes = {}
    for attr_name, attr in vars(cls).items():
        if not attr_name.startswith("_") and not attr_name.endswith("_"):
            cls_attributes[attr_name] = attr

    CustomTransformer = type(transformer_name, (Transformer,), {})

    transformer_cls = assemble_transformer(CustomTransformer, transformer_template, cls_attributes)

    return transformer_cls
