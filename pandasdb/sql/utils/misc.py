import inspect
import uuid
from copy import deepcopy
import re


def standardize_key(name):
    name = name.strip()
    name = "".join([char if char.isalpha() else "_" for char in name])
    name = camel_to_snake(name)
    name = re.sub('_+', '_', name)
    name = re.sub('_+$', '', name)
    name = re.sub('^_+', '', name)

    return name


def snake_to_camel(word):
    return ''.join(x.capitalize() or '_' for x in word.split('_'))


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
    name = name.split(".")[-1]
    return camel_to_snake(name).replace(" ", "_").lower().replace('"', "")


def type_check(instance, TYPE):
    return isinstance(instance, TYPE) or issubclass(type(instance), TYPE) or instance == TYPE


def string_if(element, formatter=""):
    if not element:
        return ""

    if isinstance(element, list) or isinstance(element, tuple):
        return formatter.join(map(str, element))

    return f"{element}"


def maybe_copy(element):
    try:
        return deepcopy(element) if not hasattr(element, "copy") else element.copy()
    except:
        return element


def ID():
    return uuid.uuid4().hex[:6].upper()


def is_lambda(v):
    LAMBDA = lambda: 0
    return isinstance(v, type(LAMBDA)) and v.__name__ == LAMBDA.__name__


def find_missing_values(database, missing_values):
    from pandasdb import Async

    jobs = []
    columns = []
    for table_name, table in database._tables.items():
        for column in table.columns:
            jobs.append(column.isin(missing_values).sum().execute)
            columns.append(column)

    def controlled_execution(job):
        def wrapper():
            try:
                return job()
            except:
                pass

        return wrapper

    for i, result in enumerate(Async.handle(*map(controlled_execution, jobs))):
        if result and result > 0:
            yield columns[i], result
