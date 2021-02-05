from pandasdb.transformer.data_containers import *


class TransformationCore:

    @property
    def _columns(self) -> List[ColumnContainer]:
        return self._core_["_columns_"]

    @property
    def _aggregations(self) -> List[AggregationContainer]:
        return self._core_["_aggregations_"]

    @property
    def _pre_conditions(self) -> List[ConditionContainer]:
        return self._core_["_pre_conditions_"]

    @property
    def _post_conditions(self) -> List[ConditionContainer]:
        return self._core_["_post_conditions_"]

    @property
    def _groups(self) -> GroupContainer:
        group_columns = []
        for group in self._core_["_groups_"]:
            group_columns += group.columns

        if group_columns:
            return GroupContainer(columns=group_columns)

    @property
    def _splits(self) -> SplitContainer:
        split_columns, sort_columns = [], []
        for split in self._core_["_splits_"]:
            split_columns += split.group.columns
            sort_columns += split.sort_by.columns

        if split_columns:
            return SplitContainer(GroupContainer(split_columns), SortByContainer(sort_columns))

    @property
    def _indexes(self) -> IndexContainer:
        indexes = []
        for index in self._core_["_indexes_"]:
            indexes += list(index.columns)

        return IndexContainer(list(dict.fromkeys(indexes)))

    @property
    def _parameters(self) -> List[ParameterContainer]:
        return self._core_["_parameters_"]

    @property
    def _many_to_many(self) -> ManyToManyContainer:
        return self._core_["_many_to_many_"]
