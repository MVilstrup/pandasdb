from typing import Optional, Callable
from pandasdb.sql.utils import camel_to_snake
import json
import numpy as np


class Record:
    def __init__(self, **kwargs):
        self._kwargs = {}

        for key, value in kwargs.items():
            self.update(camel_to_snake(key), value)

    def update(self, column: str, value):
        if callable(value):
            value = value(self.get(column))
        setattr(self, column, value)
        self._kwargs[column] = value
        return self

    def update_where(self, value, column_cond: Optional[Callable] = None, value_cond: Optional[Callable] = None):
        for column, val in self._kwargs.items():
            if callable(column_cond):
                if column_cond(column):
                    self.update(column, value)
            if callable(value_cond):
                if value_cond(val):
                    self.update(column, value)

        return self

    def expand(self, column, depth=1):
        def inner_keys(kwargs, depth, parent_key=None):
            all_keys = {}
            for key, value in list(kwargs.items()):
                if isinstance(value, dict) and depth > 1:
                    kwargs.pop(key)

                    for inner_key, inner_value in inner_keys(value, depth - 1, key).items():
                        name = f"{parent_key}_{inner_key}" if parent_key else inner_key
                        all_keys[name] = inner_value
                else:
                    if key != parent_key:
                        name = f"{parent_key}_{key}" if parent_key else key
                        all_keys[name] = value
            return all_keys

        if isinstance(self._kwargs[column], str):
            new_kwargs = json.loads(self._kwargs[column])
        elif isinstance(self._kwargs[column], dict):
            new_kwargs = self._kwargs[column]
        else:
            raise ValueError(f"{column} contains no data to be expanded")

        new_kwargs = inner_keys(new_kwargs, depth)

        self._kwargs.pop(column)
        new_kwargs.update(self._kwargs)
        return Record(**new_kwargs)

    def get(self, column):
        if column not in self._kwargs:
            self._kwargs[column] = np.NaN

        return self._kwargs[column]

    def remove(self, *columns):
        for column in columns:
            self._kwargs.pop(column)
        return Record(**self._kwargs)

    def __getitem__(self, item: str):
        return self.get(item)

    def __getattr__(self, name):
        if name not in ["keys", "_kwargs"] and not (name.startswith("_") or name in self._kwargs):
            return self.get(name)

        return self.__getattribute__(name)

    def __setitem__(self, item: str, value):
        return self.update(item, value)

    def __add__(self, other):
        if isinstance(other, dict):
            for key, value in other.items():
                self.update(key, value)
        elif isinstance(other, Record):
            for key, value in other:
                self.update(key, value)
        else:
            raise ValueError("Record can only be added to dict or another record")

        return self

    def __radd__(self, other):
        return self + other

    def __iter__(self):
        return iter(self._kwargs.items())

    def __str__(self):
        return json.dumps(self._kwargs, sort_keys=True, indent=4)

    def __repr__(self):
        return json.dumps(self._kwargs, sort_keys=True, indent=4)

    def __contains__(self, item):
        return item in self._kwargs
