from typing import List

from pandasdb.services.transform.src.data_containers import (ColumnContainer,
                                                             AggregationContainer,
                                                             ConditionContainer,
                                                             GroupContainer,
                                                             SplitContainer,
                                                             SortByContainer,
                                                             IndexContainer,
                                                             ParameterContainer)


class TransformationCore:
    """
    TransformationCore is, as the name implies, the core of the transformation. It gets are "_core_" injected dynamically
    which is why the 'self._core_' cannot be seen anywhere in this file
    """

    @property
    def _columns(self) -> List[ColumnContainer]:
        return list(self._core_["_columns_"].values())

    @property
    def _aggregations(self) -> List[AggregationContainer]:
        return list(self._core_["_aggregations_"].values())

    @property
    def _pre_conditions(self) -> List[ConditionContainer]:
        return list(self._core_["_pre_conditions_"].values())

    @property
    def _post_conditions(self) -> List[ConditionContainer]:
        return list(self._core_["_post_conditions_"].values())

    @property
    def _split_conditions(self) -> List[ConditionContainer]:
        return list(self._core_["_split_conditions_"].values())

    @property
    def _groups(self) -> GroupContainer:
        group_columns = []
        for group in self._core_["_groups_"].values():
            group_columns += group.columns

        if group_columns:
            return GroupContainer(columns=group_columns)

    @property
    def _splits(self) -> SplitContainer:
        split_columns, sort_columns = [], []
        for split in self._core_["_splits_"].values():
            split_columns += split.group.columns
            sort_columns += split.sort_by.columns

        if split_columns:
            return SplitContainer(GroupContainer(split_columns), SortByContainer(sort_columns))

    @property
    def _indexes(self) -> IndexContainer:
        indexes = []
        for index in self._core_["_indexes_"].values():
            indexes += list(index.columns)

        return IndexContainer(list(dict.fromkeys(indexes)))

    @property
    def _parameters(self) -> List[ParameterContainer]:
        return list(self._core_["_parameters_"].values())
