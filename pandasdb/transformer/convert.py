from functools import wraps
from threading import Lock

initialization_lock = Lock()


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


def transformer(cls):
    from pandasdb.transformer.cache import TransformationCache
    from pandasdb.transformer.transforms.transformer import Transformer
    from pandasdb.transformer.utils import to_transformer_name

    transformer_name = to_transformer_name(cls.__name__)

    transformer_template = TransformationCache.get_template(to_transformer_name(cls.__name__))

    # Copy all functionality of the original class
    cls_attributes = {}
    for attr_name, attr in vars(cls).items():
        if not attr_name.startswith("_") and not attr_name.endswith("_"):
            cls_attributes[attr_name] = attr

    CustomTransformer = type(transformer_name, (Transformer,), {})

    transformer_cls = assemble_transformer(CustomTransformer, transformer_template, cls_attributes)

    if transformer_template["_parameters_"]:
        return transformer_cls
    else:
        return transformer_cls()
