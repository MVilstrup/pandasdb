import re
import inspect


def curr_func():
    return inspect.stack()[0][3]


def iterable(element):
    if isinstance(element, str):
        return False
    try:
        iterator = iter(element)
    except TypeError:
        return False
    else:
        return True


def numpy_type_mapper(nptype):
    return nptype


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def string_to_python_attr(name):
    return camel_to_snake(name).replace(" ", "_").lower().replace('"', "")


def type_check(instance, TYPE):
    return isinstance(instance, TYPE) or issubclass(type(instance), TYPE) or instance == TYPE
