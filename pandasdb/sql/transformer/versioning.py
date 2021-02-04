from collections import defaultdict
from copy import deepcopy

class State:
    def __init__(self):
        self.transforms = defaultdict(dict)
        self.groups = dict()
        self.index = []
        self.input = None

    @property
    def empty(self):
        return not any([
            bool(self.transforms),
            bool(self.groups),
            bool(self.index),
            bool(self.input)
        ])

    def __eq__(self, other):
        return all([
            self.transforms == other.transforms,
            self.groups == other.groups,
            self.index == other.index,
            self.input == other.input
        ])

    def __ne__(self, other):
        return not (self == other)

    def copy(self):
        return deepcopy(self)


class Identifier:
    def __init__(self):
        self.prev_state = State()
        self.current_state = State()

    def __enter__(self):
        if self.current_state.empty or self.current_state == self.prev_state:
            self.current_state = self.prev_state.copy()


        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.current_state != self.prev_state:
            self.prev_state = self.current_state

        self.current_state = State()

    @property
    def columns(self):
        return self.current_state.transforms.get("columns", {})

    @property
    def aggregations(self):
        return self.current_state.transforms.get("aggregations", {})

    @property
    def splits(self):
        return self.current_state.groups.get("splits")

    @property
    def groups(self):
        return self.current_state.groups.get("groups")

    @property
    def index(self):
        return list(dict.fromkeys(self.current_state.index))

    @property
    def input(self):
        return self.current_state.input

    @property
    def pre_conditions(self):
        return self.current_state.transforms.get("pre_conditions", {})

    @property
    def post_conditions(self):
        return self.current_state.transforms.get("post_conditions", {})

    @property
    def has_columns(self):
        return bool(self.columns)

    @property
    def has_aggregations(self):
        return bool(self.aggregations)

    @property
    def has_splits(self):
        return bool(self.splits)

    @property
    def has_groups(self):
        return bool(self.groups)

    @property
    def has_index(self):
        return bool(self.index)

    @property
    def has_input(self):
        return bool(self.input)

    @property
    def has_pre_conditions(self):
        return bool(self.pre_conditions)

    @property
    def has_post_conditions(self):
        return bool(self.post_conditions)

    def update_post_conditions(self, name, conditions):
        self.current_state.transforms["post_conditions"][name] = conditions

    def update_pre_conditions(self, name, conditions):
        self.current_state.transforms["pre_conditions"][name] = conditions

    def update_columns(self, column, args):
        self.current_state.transforms["columns"][column] = args

    def update_aggregations(self, column, args):
        self.current_state.transforms["aggregations"][column] = args

    def update_splits(self, splits):
        self.current_state.groups["splits"] = splits

    def update_groups(self, groups):
        self.current_state.groups["groups"] = groups

    def update_index(self, index):
        self.current_state.index += index

    def update_input(self, input):
        self.current_state.input = input
