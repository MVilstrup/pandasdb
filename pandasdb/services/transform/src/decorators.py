import inspect
from functools import partial, lru_cache

from pandasdb.libraries.utils import to_transformer_name
from pandasdb.services.transform.src.cache import TransformationCache
from pandasdb.services.transform.src.data_containers import (ColumnContainer,
                                                             AggregationContainer,
                                                             ConditionContainer,
                                                             ParameterContainer,
                                                             IndexContainer,
                                                             GroupContainer,
                                                             SortByContainer,
                                                             SplitContainer)


def get_info(function):
    arguments = inspect.getfullargspec(function)[0]
    class_name, function_name = function.__qualname__.split(".")[-2:]

    if "self" in arguments:
        raise ValueError("all transformations should be static functions")

    return to_transformer_name(class_name), function_name, arguments


def include_column(transformer, column_name, input_columns=None, temporary=False, function=None, is_copy=False,
                   if_not_exists=False):
    if input_columns is None:
        input_columns = [column_name]

    TransformationCache.add_column(transformer, column_name,
                                   ColumnContainer(name=column_name,
                                                   input_columns=input_columns,
                                                   is_temporary=temporary,
                                                   transform=function,
                                                   is_copy=is_copy),
                                   if_not_exists)


def include_all_columns(transformer, columns, function=None, is_copy=True, if_not_exists=False):
    for column in columns:
        include_column(transformer=transformer, column_name=column, function=function, is_copy=is_copy,
                       if_not_exists=if_not_exists)


def column(function=None, temporary=False, cache=False):
    if function is None:
        return partial(column, temporary=temporary, cache=cache)

    transformer, column_name, input_columns = get_info(function)
    function = function if not cache else lru_cache(function)
    include_column(transformer, column_name, input_columns, temporary, function)

    return function


def column_generator(function):
    transformer, column_name, input_columns = get_info(function)

    if input_columns is None:
        input_columns = [column_name]

    TransformationCache.add_column(transformer, column_name,
                                   ColumnContainer(name=column_name,
                                                   input_columns=input_columns,
                                                   is_temporary=False,
                                                   transform=function,
                                                   is_copy=False,
                                                   generates_columns=True))


def aggregate(function=None, cache=False):
    if function is None:
        return partial(aggregate, cache=cache)

    transformer, column_name, input_columns = get_info(function)
    function = function if not cache else lru_cache(function)

    TransformationCache.add_aggregation(transformer, column_name,
                                        AggregationContainer(name=column_name,
                                                             input_columns=input_columns,
                                                             transform=function))
    return function


def pre_condition(function=None, cache=False):
    if function is None:
        return partial(pre_condition, cache=cache)

    transformer, column_name, input_columns = get_info(function)
    function = function if not cache else lru_cache(function)

    TransformationCache.add_pre_condition(transformer, column_name,
                                          ConditionContainer(name=column_name,
                                                             input_columns=input_columns,
                                                             transform=function))
    return function


def post_condition(function=None, cache=False):
    if function is None:
        return partial(post_condition, cache=cache)

    transformer, column_name, input_columns = get_info(function)
    function = function if not cache else lru_cache(function)

    TransformationCache.add_post_condition(transformer, column_name,
                                           ConditionContainer(name=column_name,
                                                              input_columns=input_columns,
                                                              transform=function))
    return function


def split_condition(function):
    transformer, column_name, input_columns = get_info(function)

    TransformationCache.add_split_condition(transformer, column_name,
                                            ConditionContainer(name=column_name,
                                                               input_columns=input_columns,
                                                               transform=function))
    return function


def Split(columns, sort_by):
    if not isinstance(columns, (list, tuple)):
        columns = [columns]
    if not isinstance(sort_by, (list, tuple)):
        sort_by = [sort_by]

    class_name = inspect.stack()[1][0].f_locals["__qualname__"]
    transformer_name = to_transformer_name(class_name)
    TransformationCache.add_split(transformer_name, "SPLIT",
                                  SplitContainer(GroupContainer(columns), SortByContainer(sort_by)))
    include_all_columns(transformer_name, list(columns) + list(sort_by), is_copy=True, if_not_exists=True)

    if not TransformationCache.has_index(transformer_name):
        TransformationCache.add_index(transformer_name, "INDEX", IndexContainer(columns))


def Index(*columns):
    class_name = inspect.stack()[1][0].f_locals["__qualname__"]
    transformer_name = to_transformer_name(class_name)
    TransformationCache.add_index(transformer_name, "INDEX", IndexContainer(columns))
    include_all_columns(transformer_name, columns, is_copy=True, if_not_exists=True)


def Group(*columns):
    class_name = inspect.stack()[1][0].f_locals["__qualname__"]
    transformer_name = to_transformer_name(class_name)
    TransformationCache.add_group(transformer_name, "GROUP", GroupContainer(columns))
    include_all_columns(transformer_name, columns, is_copy=True, if_not_exists=True)

    if not TransformationCache.has_index(transformer_name):
        TransformationCache.add_index(transformer_name, "INDEX", IndexContainer(columns))


def Copy(*columns, with_transformation=None, **renamed_columns):
    class_name = inspect.stack()[1][0].f_locals["__qualname__"]
    transformer_name = to_transformer_name(class_name)

    for column in columns:
        renamed_columns[column] = column

    for column_name, input_column in renamed_columns.items():
        assert not callable(input_column), "Column transformation should be expressed in 'with_transformation'"
        include_column(transformer=transformer_name,
                       column_name=column_name,
                       input_columns=[input_column],
                       function=with_transformation,
                       is_copy=True,
                       if_not_exists=True)


def Parameter(name, identifier=None, default_value=None, transform=None, helper=None, dtype=None):
    class_name = inspect.stack()[1][0].f_locals.get("__qualname__", None)
    if not class_name:
        return

    transformer_name = to_transformer_name(class_name)

    if identifier is None:
        identifier = name

    TransformationCache.add_parameter(transformer_name, name, ParameterContainer(name=name,
                                                                                 identifier=identifier,
                                                                                 default_value=default_value,
                                                                                 transform=transform,
                                                                                 helper=helper,
                                                                                 dtype=dtype))
