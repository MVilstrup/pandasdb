from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from typing import List, Any


class State:
    def __init__(self):
        self.transforms = defaultdict(dict)
        self.groups = dict()
        self.parameters = dict()
        self.index = None
        self.input = None

    @property
    def empty(self):
        return not any([
            bool(self.transforms),
            bool(self.groups),
            bool(self.index),
        ])

    def __eq__(self, other):
        return all([
            self.transforms == other.transforms,
            self.groups == other.groups,
            self.index == other.index,
        ])

    def __ne__(self, other):
        return not (self == other)

    def copy(self):
        return deepcopy(self)


@dataclass
class Transformation:
    name: str
    inputs: List[str]
    function: callable
    kwargs: dict = field(default_factory=dict)

@dataclass
class Parameter:
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

