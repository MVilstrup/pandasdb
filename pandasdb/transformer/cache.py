from collections import defaultdict


class TransformationCache:
    _columns_ = defaultdict(dict)
    _aggregations_ = defaultdict(dict)
    _pre_conditions_ = defaultdict(dict)
    _post_conditions_ = defaultdict(dict)
    _groups_ = defaultdict(dict)
    _splits_ = defaultdict(dict)
    _indexes_ = defaultdict(dict)
    _parameters_ = defaultdict(dict)
    _many_to_many_ = dict()

    @staticmethod
    def get_template(transformer_name):
        return {
            "_columns_": list(TransformationCache._columns_.pop(transformer_name, {}).values()),
            "_aggregations_": list(TransformationCache._aggregations_.pop(transformer_name, {}).values()),
            "_pre_conditions_": list(TransformationCache._pre_conditions_.pop(transformer_name, {}).values()),
            "_post_conditions_": list(TransformationCache._post_conditions_.pop(transformer_name, {}).values()),
            "_groups_": list(TransformationCache._groups_.pop(transformer_name, {}).values()),
            "_splits_": list(TransformationCache._splits_.pop(transformer_name, {}).values()),
            "_indexes_": list(TransformationCache._indexes_.pop(transformer_name, {}).values()),
            "_parameters_": list(TransformationCache._parameters_.pop(transformer_name, {}).values()),
            "_many_to_many_": TransformationCache._many_to_many_.pop(transformer_name, None)
        }

    @staticmethod
    def add_column(transformer_name, column_name, column, if_not_exists=False):
        if if_not_exists and column_name in TransformationCache._columns_[transformer_name]:
            return

        TransformationCache._columns_[transformer_name][column_name] = column

    @staticmethod
    def add_aggregation(transformer_name, aggregation_name, aggregation):
        TransformationCache._aggregations_[transformer_name][aggregation_name] = aggregation

    @staticmethod
    def add_pre_condition(transformer_name, pre_condition_name, pre_condition):
        TransformationCache._pre_conditions_[transformer_name][pre_condition_name] = pre_condition

    @staticmethod
    def add_post_condition(transformer_name, post_condition_name, post_condition):
        TransformationCache._post_conditions_[transformer_name][post_condition_name] = post_condition

    @staticmethod
    def add_group(transformer_name, group_name, group):
        TransformationCache._groups_[transformer_name][group_name] = group

    @staticmethod
    def add_split(transformer_name, split_name, split):
        TransformationCache._splits_[transformer_name][split_name] = split

    @staticmethod
    def add_index(transformer_name, index_name, index):
        TransformationCache._indexes_[transformer_name][index_name] = index

    @staticmethod
    def add_parameter(transformer_name, parameter_name, parameter):
        TransformationCache._parameters_[transformer_name][parameter_name] = parameter

    @staticmethod
    def add_many_to_many(transformer_name, many_to_many):
        TransformationCache._many_to_many_[transformer_name] = many_to_many
