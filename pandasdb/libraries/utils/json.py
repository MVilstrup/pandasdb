from datetime import date, datetime
import ujson
import numpy as np
import dateutil.parser
import pandas as pd

_all_iterators = [list, tuple, set, np.ndarray, pd.Series]
_all_values = [int, float, bool]


def _value_serializer(obj):
    """ Serialize object which are not naturally serilizable by json """

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    return obj


def _value_deserializer(obj):
    """ Deserialize objects """

    if isinstance(obj, str):
        try:
            return dateutil.parser.parse(obj)
        except:
            return obj

    if any([isinstance(obj, _type) for _type in _all_iterators]):
        return np.array(obj)

    return obj


def _recurse_over(obj, _value_func):
    if any([isinstance(obj, _type) for _type in _all_iterators]):
        return [_recurse_over(val, _value_func) for val in obj]
    elif isinstance(obj, dict):
        serialized = {}
        for name, _obj in obj.items():
            serialized[name] = _recurse_over(_obj, _value_func)

        return serialized
    else:
        return _value_func(obj)


def _serialize(obj):
    return _recurse_over(obj, _value_serializer)


def _deserialize(obj):
    return _recurse_over(obj, _value_deserializer)


def dumps(obj):
    return ujson.dumps(_serialize(obj))


def dump(obj, stream):
    ujson.dump(_serialize(obj), stream)


def loads(object):
    if isinstance(object, str) and object and object[0] in ["{", "["]:
        return _deserialize(ujson.loads(object))
    return object


def load(stream):
    return _deserialize(ujson.load(stream))
