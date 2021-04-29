from dataclasses import dataclass, field
from typing import List, Any


@dataclass
class ColumnContainer:
    name: str
    input_columns: List[str]
    is_temporary: bool
    transform: callable
    is_copy: bool = field(default=False)
    generates_columns: bool = field(default=False)
    vectorized: bool = field(default=False)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name and self.input_columns == other.input_columns


@dataclass
class AggregationContainer:
    name: str
    input_columns: List[str]
    transform: callable

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name and self.input_columns == other.input_columns


@dataclass
class ConditionContainer:
    name: str
    input_columns: List[str]
    transform: callable
    vectorized: bool

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        return self.name == other.name and self.input_columns == other.input_columns


@dataclass
class GroupContainer:
    columns: List[str]


@dataclass
class IndexContainer:
    columns: List[str]


@dataclass
class SortByContainer:
    columns: List[str]


@dataclass
class SplitContainer:
    group: GroupContainer
    sort_by: SortByContainer


@dataclass
class ParameterContainer:
    name: str
    identifier: str
    default_value: Any = field(default=None)
    value: Any = field(default=None)
    transform: callable = field(default=None)
    helper: str = field(default=None)
    dtype: type = field(default=None)
    filled = False

    def update(self, value):
        if self.transform is not None:
            value = self.transform(value)

        self.value = value
        self.filled = True
