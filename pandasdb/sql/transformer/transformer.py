from collections import defaultdict
import inspect
import pandas as pd
from functools import partial, lru_cache
from pandasdb.sql.transformer.DAG import TransformDAG
from pandasdb.sql.transformer.containers import Transformation, Parameter
from pandasdb.sql.transformer.versioning import Identifier


class Transformer:
    __versions__ = defaultdict(Identifier)

    @classmethod
    def version(cls):
        return cls.__versions__[cls.__name__]

    def __new__(cls, *args, **kwargs):
        if cls.version().has_parameters:
            cls._handle_parameters(kwargs)
            return cls
        elif not args:
            return cls

        df = args[0]

        try:
            df = df.df()
        except AttributeError:
            pass

        with cls.version():
            cls._replace_star_(df.columns)

            # Set all parameters
            for parameter in cls.version().parameters.values():
                setattr(cls, parameter.identifier, parameter.value)

            result = cls.transform(df)

        return result

    @classmethod
    def _replace_star_(cls, columns):
        star_transform = cls.version().columns.pop("*", None)
        if star_transform:
            for column_name in columns:
                if not cls.version().is_included(column_name):
                    column = Transformation(name=column_name,
                                            inputs=[column_name],
                                            function=star_transform.function,
                                            kwargs=dict(is_copy=True))
                    Transformer.__versions__[cls.__name__].update_columns(column)

    @classmethod
    def _handle_parameters(cls, kwargs):
        for parameter_name, parameter in cls.version().parameters.items():
            if parameter.filled:
                continue

            if parameter_name not in kwargs:
                raise ValueError(f"{parameter_name} not provided. Hint: '{parameter.helper}'")

            parameter.update(kwargs[parameter_name])

    @classmethod
    def parameters(cls):
        all_params = cls.version().parameters.values()
        return type("Params", (object,), {parameter.identifier: parameter.value for parameter in all_params})

    @staticmethod
    def _notify_(function=None, transformer=None, column=None):
        if function is not None:
            if "self" in inspect.getfullargspec(function)[0]:
                raise ValueError("columns can only be static functions")

            transformer, column = function.__qualname__.split(".")[-2:]

        return transformer, column

    @staticmethod
    def _column(function=None, transformer_name=None, column_name=None, cache=None, arguments=None, **kwargs):
        if transformer_name is not None and column_name is not None:
            transformer_name, column_name = Transformer._notify_(transformer=transformer_name, column=column_name)
        else:
            transformer_name, column_name = Transformer._notify_(function)

        function = function if not cache else lru_cache(function)

        column = Transformation(column_name, arguments, function, kwargs)
        Transformer.__versions__[transformer_name].update_columns(column)
        return function

    @staticmethod
    def _aggregate(function, arguments, cache):
        transformer_name, column_name = Transformer._notify_(function)
        function = function if not cache else lru_cache(function)

        Transformer.__versions__[transformer_name].update_aggregations(Transformation(column_name, arguments, function))
        return function

    @staticmethod
    def _group(transformer_name, columns, sort_by, is_split, transforms=None):
        notify = lambda column: Transformer._notify_(transformer=transformer_name, column=column)[1]
        columns = list(map(notify, columns))

        transform = Transformation("SPLIT" if is_split else "GROUP",
                                   columns,
                                   None,
                                   dict(sort_by=sort_by, transforms=transforms))

        if is_split:
            Transformer.__versions__[transformer_name].update_splits(transform)
        else:
            Transformer.__versions__[transformer_name].update_groups(transform)

        for column in columns:
            Transformer._column(transformer_name=transformer_name, column_name=column, arguments=[column])

        Transformer._index(transformer_name=transformer_name, columns=columns)

    @staticmethod
    def _index(transformer_name, columns):
        Transformer.__versions__[transformer_name].update_index(Transformation("INDEX", columns, None))

        for column in columns:
            Transformer._column(transformer_name=transformer_name, column_name=column, arguments=[column])

    @staticmethod
    def _condition(function, is_before, cache):
        transformer_name, condition_name = Transformer._notify_(function)
        arguments = inspect.getfullargspec(function)[0]
        function = function if not cache else lru_cache(function)

        condition = Transformation(condition_name, arguments, function)

        if is_before:
            Transformer.__versions__[transformer_name].update_pre_conditions(condition)
        else:
            Transformer.__versions__[transformer_name].update_post_conditions(condition)

        return function

    @staticmethod
    def parameter(name, identifier=None, default_value=None, transform=None, helper=None):
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]

        if identifier is None:
            identifier = name

        Transformer.__versions__[transformer_name].update_parameter(Parameter(name=name,
                                                                              identifier=identifier,
                                                                              default_value=default_value,
                                                                              transform=transform,
                                                                              helper=helper))

    @staticmethod
    def column(function=None, temporary=False, cache=False):
        if function is None:
            return partial(Transformer.column, temporary=temporary, cache=cache)

        arguments = inspect.getfullargspec(function)[0]

        return Transformer._column(function=function,
                                   cache=cache,
                                   arguments=arguments,
                                   temporary=temporary)

    @staticmethod
    def aggregate(function=None, inputs=None, cache=False):
        if function is None:
            return partial(Transformer.column, inputs=inputs, cache=cache)

        return Transformer.aggregate(function,
                                     inspect.getfullargspec(function)[0],
                                     cache)

    @staticmethod
    def copy(*columns, with_function=None, **renamed_columns):
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]

        for column in columns:
            Transformer._column(function=with_function, transformer_name=transformer_name, column_name=column,
                                arguments=[column], is_copy=True)

        for column_name, input_column in renamed_columns.items():
            Transformer._column(function=with_function, transformer_name=transformer_name, column_name=column_name,
                                arguments=[input_column], is_copy=True)

    @staticmethod
    def group(*columns, **transforms):
        modified_transforms = {}
        for transform_name, function in transforms.items():
            assert callable(function), "Only columns and transformations can be grouped on"
            modified_transforms[transform_name] = Transformation(transform_name,
                                                                 inspect.getfullargspec(function)[0],
                                                                 function)

        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]
        Transformer._group(transformer_name, list(columns), sort_by=None, is_split=False, transforms=transforms)

    @staticmethod
    def input(data):
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]
        Transformer.__versions__[transformer_name].update_input(data)

    @staticmethod
    def split(columns, sort_by):
        if not isinstance(columns, (list, tuple)):
            columns = [columns]
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]

        Transformer._group(transformer_name, list(columns), sort_by=sort_by, is_split=True)

    @staticmethod
    def index(*columns):
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]
        Transformer._index(transformer_name, list(columns))

    @staticmethod
    def pre_condition(function=None, cache=False):
        if function is None:
            return partial(Transformer.pre_condition, cache=cache)

        return Transformer._condition(function, is_before=True, cache=cache)

    @staticmethod
    def post_condition(function=None, cache=False):
        if function is None:
            return partial(Transformer.post_condition, cache=cache)

        return Transformer._condition(function, is_before=False, cache=cache)

    @classmethod
    def __prepare_data__(cls, columns, input_df, transformed_df, aggregates, views):
        sub_df = pd.concat([
            input_df[list(set(columns) & set(input_df.columns))],
            transformed_df[list(set(columns) & set(transformed_df.columns) - set(input_df.columns))]
        ], axis=1)

        for column in set(columns) & set(aggregates.keys()):
            sub_df[column] = aggregates[column]

        for column in set(columns) & set(views.keys()):
            sub_df[column] = views[column]

        return sub_df

    @classmethod
    def _dag_(cls, input_columns):
        """
        Calculate all transformations in the correct order
        """
        transformations = TransformDAG(input_columns)

        # Add all aggregates
        for column_name, input_columns in cls._aggregates_():
            transformations.add("aggregates", column_name, input_columns)

        # Add all columns
        for column_name, input_columns in cls._columns_():
            transformations.add("columns", column_name, input_columns)

        return transformations

    @classmethod
    def __apply__(cls, df, new_df, split):
        aggregates = {}
        views = {}

        for column, input_columns, col_type in cls._dag_(df.columns):
            if col_type == "aggregates":
                sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates, views)
                aggregate = cls.version().aggregations[column]
                aggregates[column] = aggregate.function(**sub_df.to_dict(orient="series"))
            elif col_type == "columns":

                sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates, views)
                column_transform = cls.version().columns[column]

                if column_transform.kwargs.get("is_copy") or column_transform.function is None:
                    if column_transform.function is None:
                        # just copy the column and move on
                        new_df[column] = sub_df[input_columns[0]]
                    else:
                        input_vals = sub_df[input_columns[0]]
                        if split:
                            new_df[column] = column_transform.function(input_vals)
                        else:
                            new_df[column] = input_vals.apply(column_transform.function)

                    continue

                container = views if column_transform.kwargs.get("temporary") else new_df

                if split:
                    container[column] = column_transform.function(**sub_df.to_dict(orient="series"))
                else:
                    container[column] = sub_df.apply(lambda row: column_transform.function(**row), axis=1)

        if split:
            return new_df
        else:
            return new_df, aggregates, views

    @classmethod
    def _pre_conditions_(cls):
        for (condition_name, condition) in cls.version().pre_conditions.items():
            yield condition.name, condition.inputs, condition.function

    @classmethod
    def _post_conditions_(cls):
        for (condition_name, condition) in cls.version().post_conditions.items():
            yield condition.name, condition.inputs, condition.function

    @classmethod
    def _groups_(cls):
        groups = cls.version().groups
        if groups:
            return groups.inputs

    @classmethod
    def _splits_(cls):
        splits = cls.version().splits
        if splits:
            return splits.inputs, splits.kwargs["sort_by"]

        return None, None

    @classmethod
    def _indexes_(cls):
        return cls.version().index.inputs

    @classmethod
    def _aggregates_(cls):
        for aggregate_name, aggregate in cls.version().aggregations.items():
            yield aggregate.name, aggregate.inputs

    @classmethod
    def _columns_(cls):
        for column_name, column in cls.version().columns.items():
            yield column.name, column.inputs

    @classmethod
    def transform(cls, df):

        if not cls.version().has_columns:
            for column in df.columns:
                cls._column(transformer_name=cls.__name__, column_name=column, arguments=[column])

        new_df = pd.DataFrame({})

        """
        Apply all pre conditions
        """
        for condition_name, input_columns, function in cls._pre_conditions_():
            df = df[df[input_columns].apply(lambda row: function(**row), axis=1)]

        """
        Group the data if needed
        """
        if cls.version().has_groups:
            group_colums = cls._groups_()
            df = df.groupby(group_colums).agg(list).reset_index()

            for column in group_colums:
                new_df[column] = df[column]

        """
        Alternatively split up the data in groups
        """

        if cls.version().has_splits:
            split_columns, sort_columns = cls._splits_()

            for _, gdf in df.sort_values(sort_columns).groupby(split_columns):
                curr_df = pd.DataFrame({column: gdf[column] for column in split_columns})
                curr_df = cls.__apply__(gdf, curr_df, split=True)
                new_df = pd.concat([new_df, curr_df], axis=0)
            aggregates = {}
            views = {}
        else:
            new_df, aggregates, views = cls.__apply__(df, new_df, split=False)

        """
        Apply all post conditions
        """
        for condition_name, input_columns, function in cls._post_conditions_():
            sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates, views)
            new_df = new_df[sub_df.apply(lambda row: function(**row), axis=1)]

        """
        Finally Index the dataframe
        """
        if cls.version().has_index:
            index_columns = cls._indexes_()
            new_df = new_df.set_index(index_columns).sort_index()

        return new_df
