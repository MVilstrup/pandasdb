from dataclasses import dataclass, field
from typing import List, Any, Union


@dataclass
class ColumnContainer:
    name: str
    input_columns: List[str]
    is_temporary: bool
    transform: callable
    is_copy: bool = field(default=False)


@dataclass
class AggregationContainer:
    name: str
    input_columns: List[str]
    transform: callable


@dataclass
class ConditionContainer:
    name: str
    input_columns: List[str]
    transform: callable


@dataclass
class GroupContainer:
    columns: List[str]


@dataclass
class IndexContainer:
    columns: List[str]


@dataclass
class SortByContainer:
    columns: List[str]
    transform: callable


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
    filled = False

    def update(self, value):
        if self.transform is not None:
            value = self.transform(value)

        self.value = value
        self.filled = True


@dataclass
class ManyToManyContainer:
    left: str
    right: str
    in_common: Union[List[str], str]
    unique: List[str]

