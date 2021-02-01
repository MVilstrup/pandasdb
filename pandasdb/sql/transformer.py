from collections import defaultdict
import inspect
import pandas as pd
from functools import partial, lru_cache

import networkx as nx

def convert(**kwargs):
    return kwargs

class TransformDAG:
    __root__ = "<ROOT>"

    def __init__(self, inputs):
        self.DAG = nx.DiGraph()
        self.DAG.add_node(self.__root__, type="ROOT")

        for input_column in inputs:
            self.DAG.add_node(input_column, type="INPUT", dependencies=None)
            self.DAG.add_edge(self.__root__, input_column)

    def add(self, type, name, dependencies):
        if name not in self.DAG.nodes:
            self.DAG.add_node(name, type=type, dependencies=dependencies)
        else:
            self.DAG.nodes[name]["type"] = type
            self.DAG.nodes[name]["dependencies"] = dependencies

        if not dependencies:
            self.DAG.add_edge(self.__root__, name)
        else:
            for dependency in dependencies:
                self.DAG.add_edge(dependency, name)

    def __iter__(self):
        required_columns = set(self.DAG.nodes)

        state = set()
        for name in self.DAG.successors(self.__root__):
            if name in required_columns and name != self.__root__:
                state.add(name)

                yield name, self.DAG.nodes[name]["dependencies"], self.DAG.nodes[name]["type"]

        missing_streams = [name for name in required_columns if name not in state and name != self.__root__]
        while missing_streams:
            for name in missing_streams:
                dependencies = [dependency for dependency in self.DAG.predecessors(name) if dependency != self.__root__]
                if all([dependency in state for dependency in dependencies]):
                    state.add(name)
                    yield name, self.DAG.nodes[name]["dependencies"], self.DAG.nodes[name].get("type")

            missing_streams = [node for node in self.DAG.nodes if node not in state and node != self.__root__]



class Transformer:
    __transforms__ = defaultdict(lambda: defaultdict(dict))
    __aggregates__ = defaultdict(dict)
    __calls__ = defaultdict(lambda: defaultdict(int))
    __groups__ = {}

    def __new__(cls, df):
        return cls.transform(df)

    def _notify_(function=None, transformer=None, column=None):
        if function is not None:
            if "self" in inspect.getfullargspec(function)[0]:
                raise ValueError("columns can only be static functions")

            transformer, column = function.__qualname__.split(".")

        Transformer.__calls__[transformer][column] += 1
        return transformer, column

    @staticmethod
    def column(function=None, inputs=None, cache=False, post_process=None):
        if function is None:
            return partial(Transformer.column, inputs=inputs, cache=cache, post_process=post_process)

        transformer_name, column_name = Transformer._notify_(function)

        arguments = inspect.getfullargspec(function)[0]
        if "self" in arguments:
            raise ValueError("columns can only be static functions")

        function = function if not cache else lru_cache(function)

        Transformer.__transforms__[transformer_name]["columns"][column_name] = (arguments, function, post_process)
        return function

    @staticmethod
    def aggregate(function=None, inputs=None, cache=False):
        if function is None:
            return partial(Transformer.column, inputs=inputs, cache=cache)

        transformer_name, column_name = Transformer._notify_(function)
        arguments = inspect.getfullargspec(function)[0]

        function = function if not cache else lru_cache(function)

        Transformer.__aggregates__[transformer_name][column_name] = (arguments, function)

        return function

    @staticmethod
    def copy(*columns, **renamed_columns):
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]

        for column in columns:
            transformer_name, column = Transformer._notify_(transformer=transformer_name, column=column)
            Transformer.__transforms__[transformer_name]["columns"][column] = ([column], None, None)

        for column_name, input_column in renamed_columns.items():
            transformer_name, column_name = Transformer._notify_(transformer=transformer_name, column=column_name)
            Transformer.__transforms__[transformer_name]["columns"][column_name] = ([input_column], None, None)

    @staticmethod
    def group(*columns):
        transformer_name = inspect.stack()[1][0].f_locals["__qualname__"]
        Transformer.__groups__[transformer_name] = list(columns)

    @staticmethod
    def pre_condition(function=None, cache=False):
        if function is None:
            return partial(Transformer.pre_condition, cache=cache)

        transformer_name, condition_name = Transformer._notify_(function)
        arguments = inspect.getfullargspec(function)[0]
        function = function if not cache else lru_cache(function)

        Transformer.__transforms__[transformer_name]["pre_conditions"][condition_name] = (arguments, function)
        return function

    @staticmethod
    def post_condition(function=None, cache=False):
        if function is None:
            return partial(Transformer.post_condition, cache=cache)

        transformer_name, condition_name = Transformer._notify_(function)
        arguments = inspect.getfullargspec(function)[0]
        function = function if not cache else lru_cache(function)

        Transformer.__transforms__[transformer_name]["post_conditions"][condition_name] = (arguments, function)
        return function

    @classmethod
    def __included__(cls, column):
        latest_version = max(Transformer.__calls__[cls.__name__].values())
        version = Transformer.__calls__[cls.__name__][column]

        if version == latest_version:
            return True
        elif version == 1:
            Transformer.__calls__[cls.__name__][column] = latest_version
            return True
        else:
            Transformer.__calls__[cls.__name__].pop(column)
            return False

    @classmethod
    def __prepare_data__(cls, columns, input_df, transformed_df, aggregates):
        sub_df = pd.concat([
            input_df[list(set(columns) & set(input_df.columns))],
            transformed_df[list(set(columns) & set(transformed_df.columns) - set(input_df.columns))]
        ], axis=1)

        for column in set(columns) & set(aggregates.keys()):
            sub_df[column] = aggregates[column]

        return sub_df

    @classmethod
    def transform(cls, df):
        if not isinstance(df, pd.DataFrame):
            df = df.df()

        new_df = pd.DataFrame({})

        """
        Start out grouping the data if needed
        """
        if cls.__groups__.get(cls.__name__):
            columns = list(cls.__groups__.get(cls.__name__))
            df = df.groupby(columns).agg(list).reset_index()

            for column in columns:
                new_df[column] = df[column]

        """
        Apply all pre conditions
        """
        condition_events = list(cls.__transforms__[cls.__name__]["pre_conditions"].items())
        for (condition_name, (input_columns, function)) in condition_events:
            df = df[df[input_columns].apply(lambda row: function(**row), axis=1)]

        """
        Calculate all transformations in the correct order
        """
        transformations = TransformDAG(df.columns)

        # Add all aggregates
        for column_name, (input_columns, _) in cls.__aggregates__[cls.__name__].items():
            transformations.add("AGGREGATE", column_name, input_columns)

        # Add all columns
        for column_name, (input_columns, _, _) in Transformer.__transforms__[cls.__name__]["columns"].items():
            transformations.add("COLUMN", column_name, input_columns)

        aggregates = {}

        for column, input_columns, type in transformations:
            if type == "AGGREGATE":
                sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates)
                function = cls.__aggregates__[cls.__name__][column][1]
                aggregates[column] = function(**sub_df.to_dict(orient="series"))
            elif type == "COLUMN":
                sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates)
                _, function, post_process = cls.__transforms__[cls.__name__]["columns"][column]

                if function is not None:
                    new_df[column] = sub_df.apply(lambda row: function(**convert(**row)), axis=1)
                    if callable(post_process):
                        new_df[column] = new_df[column].apply(lambda val: post_process(val, new_df[column]))
                else:
                    new_df[column] = sub_df[input_columns[0]]


        """
        Apply all post conditions
        """
        condition_events = list(cls.__transforms__[cls.__name__]["post_conditions"].items())
        for condition_name, (input_columns, function) in condition_events:
            sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates)
            new_df = new_df[sub_df.apply(lambda row: function(**convert(**row)), axis=1)]

        return new_df


class join:
    def __init__(self, left=None, right=None, on=None, how=None, names=None):
        self._left = left
        self._right = right
        self._on = on
        self._how = how

        if names is None:
            self._suffixes = ["_left", "_right"]
        else:
            self._suffixes = names

    def left(self, left):
        self._left = left
        return self

    def right(self, right):
        self._right = right
        return self

    def on(self, on):
        self._on = on
        return self

    def how(self, how):
        self._how = how
        return self

    def names(self, names):
        self._suffixes = names
        return self

    def __getitem__(self, item):
        return self()[item]

    def __call__(self):
        return pd.merge(self._left, self._right, on=self._on, how=self._how, suffixes=self._suffixes)


class left_join(join):
    def __init__(self, left=None, right=None, on=None, names=None):
        join.__init__(self, left, right, on, how="left", names=names)


class right_join(join):
    def __init__(self, left=None, right=None, on=None, names=None):
        join.__init__(self, left, right, on, how="right", names=names)


class inner_join(join):
    def __init__(self, left=None, right=None, on=None, names=None):
        join.__init__(self, left, right, on, how="inner", names=names)


column = Transformer.column
post_condition = Transformer.post_condition
pre_condition = Transformer.pre_condition
copy = Transformer.copy
group = Transformer.group
aggregate = Transformer.aggregate
