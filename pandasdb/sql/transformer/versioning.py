from pandasdb.sql.transformer.containers import State


class Identifier:
    def __init__(self):
        self.prev_state = State()
        self.current_state = State()
        self.copied = False

    def __enter__(self):
        if self.current_state.empty or self.current_state == self.prev_state:
            self.current_state = self.prev_state.copy()
            self.copied = True

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.copied:
            self.prev_state = self.current_state
            self.copied = False

        self.current_state = State()

    def is_included(self, column):
        for column_name, transform in self.columns.items():
            if column == column_name:
                return True
            if column in transform.inputs and transform.kwargs.get("is_copy", False):
                return True

        return False

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
        return self.current_state.index

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
    def parameters(self):
        return self.current_state.parameters

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

    @property
    def has_parameters(self):
        return any([not param.filled for param in self.parameters.values()])

    def update_parameter(self, parameter):
        self.current_state.parameters[parameter.name] = parameter

    def update_post_conditions(self, condition):
        self.current_state.transforms["post_conditions"][condition.name] = condition

    def update_pre_conditions(self, condition):
        self.current_state.transforms["pre_conditions"][condition.name] = condition

    def update_columns(self, column):
        self.current_state.transforms["columns"][column.name] = column

    def update_aggregations(self, aggregation):
        self.current_state.transforms["aggregations"][aggregation.name] = aggregation

    def update_splits(self, splits):
        self.current_state.groups["splits"] = splits

    def update_groups(self, groups):
        self.current_state.groups["groups"] = groups

    def update_index(self, index):
        if self.has_index:
            self.current_state.index.inputs = list(dict.fromkeys(self.current_state.index.inputs + index.inputs))
        else:
            self.current_state.index = index

    def update_input(self, input):
        self.current_state.input = input
