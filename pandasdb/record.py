from pandasdb.utils import json


class Record:
    def __init__(self, **kwargs):
        self._kwargs = {}

        for key, value in kwargs.items():
            self.update(key, value)

    def update(self, column, value):
        setattr(self, column, value)
        self._kwargs[column] = value
        return self

    def expand(self, column):
        if isinstance(self._kwargs[column], str):
            new_kwargs = json.loads(self._kwargs[column])
        elif isinstance(self._kwargs[column], dict):
            new_kwargs = self._kwargs[column]
        else:
            raise ValueError(f"{column} contains no data to be expanded")

        self._kwargs.pop(column)
        new_kwargs.update(self._kwargs)
        return Record(**new_kwargs)

    def get(self, column):
        return self._kwargs[column]

    def remove(self, *columns):
        for column in columns:
            self._kwargs.pop(column)
        return Record(**self._kwargs)

    def __getitem__(self, item):
        return self._kwargs[item]

    def __setitem__(self, item, value):
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