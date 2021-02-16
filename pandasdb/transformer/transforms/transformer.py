from collections import defaultdict

from pandasdb.transformer.DAG import TransformDAG
from pandasdb.transformer.data_containers import AggregationContainer, ColumnContainer
from pandasdb.transformer.transforms.core import TransformationCore, Union
import pandas as pd
from pandasdb.transformer.utils import to_df


class Transformer(TransformationCore):

    def __init__(self, **given_params):
        self._param_values = {}
        for parameter in self._parameters:
            name, identifier, dtype = parameter.name, parameter.identifier, parameter.dtype
            if name not in given_params:
                if parameter.default_value is None:
                    raise ValueError(f"{name} is needed to compute. Reason: {parameter.helper}")
                else:
                    parameter.update(parameter.default_value)
                    self._param_values[identifier] = parameter.value
            else:
                if dtype is not None:
                    if not isinstance(given_params[name], dtype):
                        raise ValueError(f"{name} has type {type(given_params[name])} but {dtype} was expected")

                parameter.update(given_params[name])
                self._param_values[identifier] = parameter.value

    def __new__(cls, *args, **kwargs) -> Union[TransformationCore, pd.DataFrame]:
        assert not (kwargs and args)

        obj_ref = super(cls.__bases__[-1], cls).__new__(cls)
        obj_ref.__init__(**kwargs)
        if not args:
            return obj_ref
        else:
            return obj_ref(*args, **kwargs)

    def _apply_pre_conditions(self, df: pd.DataFrame):
        for condition in self._pre_conditions:
            input_cols, param_cols = self.divide(condition.input_columns, [df.columns, self._param_values.keys()])
            parmeters = {param: self._param_values[param] for param in param_cols}

            condition_result = df[input_cols].apply(lambda row: condition.transform(**row, **parmeters), axis=1)

            df = df[condition_result]

        return df

    def _apply_post_conditions(self, transformed_df, df, aggregates, views):
        for condition in self._post_conditions:
            target = condition.input_columns
            input_data, extra_params = self._multi_source_extract(target, transformed_df, df, aggregates, views)
            condition_result = input_data.apply(lambda row: condition.transform(**row, **extra_params), axis=1)
            transformed_df = transformed_df[condition_result]

        return transformed_df

    def _apply_split_conditions(self, split_df):
        for condition in self._split_conditions:
            condition_result = condition.transform(**split_df[condition.input_columns].to_dict(orient="series"))
            split_df = split_df[condition_result]

        return split_df

    @staticmethod
    def _align_tables(left, right, in_common, unique, names):
        # Handle column naming
        name_left, name_right = names
        left_index, right_index = in_common

        column_overlap = (set(left.columns) & set(right.columns)) - set(in_common)

        left = left.rename(columns={column: f"{name_left}_{column}" for column in column_overlap})
        right = right.rename(columns={column: f"{name_right}_{column}" for column in column_overlap})

        left_unique, right_unique = unique
        left_unique = left_unique if left_unique not in column_overlap else f"{name_left}_{left_unique}"
        right_unique = right_unique if right_unique not in column_overlap else f"{name_right}_{right_unique}"

        left.set_index(left_index, inplace=True)
        right.set_index(right_index, inplace=True)

        right_on_left = left.join(right, how="left")
        left_on_right = right.join(left, how="left")

        all_rows = pd.concat([right_on_left, left_on_right], axis=0).dropna(subset=[left_unique, right_unique])
        unique_rows = all_rows.drop_duplicates(subset=[left_unique, right_unique])

        return unique_rows.reset_index()

    @staticmethod
    def _apply_func(df, transformation, extra_params, is_split):
        def prepare(kwargs):
            if isinstance(kwargs, dict):
                return {key: prepare(value) for key, value in kwargs.items()}
            if isinstance(kwargs, list):
                return pd.Series(kwargs)
            if isinstance(kwargs, pd.Series):
                return kwargs
            if pd.isna(kwargs):
                return None
            return kwargs

        # @no:format
        if len(transformation.input_columns) == 1 and not extra_params:
            stream = df[transformation.input_columns[0]]
            if transformation.transform is None:
                return prepare(stream)
            elif is_split:
                return transformation.transform(stream)
            else:
                return stream.apply(transformation.transform)
        else:
            if is_split:
                return transformation.transform(**df.to_dict(orient="series"), **extra_params)
            else:
                return df.apply(lambda row: transformation.transform(**row, **extra_params), axis=1)
        # @do:format

    def _generate_columns(self, transformed_df, result, is_split):
        if is_split:
            for column_name, data in result.items():
                transformed_df[column_name] = data
            return transformed_df
        else:
            columns = {}
            for col_info in result:
                for key in col_info.keys():
                    if key not in columns:
                        columns[key] = []

            for row in result:
                for column in columns.keys():
                    columns[column].append(row.get(column, None))

            for column, values in columns.items():
                transformed_df[column] = values

            return transformed_df

    def _apply_transform(self, transformed_df, input_df, is_split):
        aggregates, views = {}, {}

        # @no:format
        for transformation in self._execution_order(input_df.columns):
            if isinstance(transformation, AggregationContainer):
                reduced_df, extra_params = self._multi_source_extract(transformation.input_columns, transformed_df, input_df, aggregates, views)
                aggregates[transformation.name] = transformation.transform(**reduced_df.to_dict(orient="series"), **extra_params)

            elif isinstance(transformation, ColumnContainer):
                reduced_df, extra_params = self._multi_source_extract(transformation.input_columns, transformed_df, input_df, aggregates, views)

                if transformation.generates_columns:
                    result = self._apply_func(reduced_df, transformation, extra_params, is_split)
                    transformed_df = self._generate_columns(transformed_df, result, is_split)

                elif transformation.is_temporary:
                    views[transformation.name] = self._apply_func(reduced_df, transformation, extra_params, is_split)
                else:
                    transformed_df[transformation.name] = self._apply_func(reduced_df, transformation, extra_params, is_split)
        # @do:format

        return transformed_df, aggregates, views

    def divide(self, target_columns, available_columns):
        target_columns = set(target_columns)
        selected_columns = set()

        column_buckets = []
        for columns in available_columns:
            matching_column = (target_columns - selected_columns) & set(columns)
            selected_columns = selected_columns | matching_column
            column_buckets.append(matching_column)

        left_over = target_columns - selected_columns
        assert len(left_over) == 0, f"{left_over} could not be matched with the input data"

        return column_buckets

    def _multi_source_extract(self, target_columns, transformed_df, input_df, aggregates, views):
        available_columns = [transformed_df.columns, input_df.columns, aggregates.keys(),
                             views.keys(), self._param_values.keys()]

        trans_cols, input_cols, agg_cols, view_cols, param_cols = self.divide(target_columns, available_columns)

        reduced_df = pd.concat([transformed_df[trans_cols], input_df[input_cols]], axis=1)

        for column in view_cols:
            reduced_df[column] = views[column]

        extra = {}

        for column in agg_cols:
            extra[column] = aggregates[column]

        for column in param_cols:
            extra[column] = self._param_values[column]

        return reduced_df, extra

    def _execution_order(self, input_columns):
        """
        Calculate all transformations in the correct order
        """
        transformations = TransformDAG(input_columns)

        all_columns = [column for column in self._columns if not column.name == "*"]
        if any([column.name == "*" for column in self._columns]) or not all_columns:
            extra_columns = []
            for input_column in input_columns:
                already_included = False

                for column in all_columns:
                    if input_column == column.name:
                        already_included = True
                        break
                    elif input_column in column.input_columns and column.is_copy:
                        already_included = True
                        break

                if not already_included:
                    new_column = ColumnContainer(name=input_column, input_columns=[input_column], is_temporary=False,
                                                 transform=None, is_copy=True)
                    extra_columns.append(new_column)

            all_columns += extra_columns

        # Add all columns
        for column in all_columns:
            transformations.add(column)

        # Add all aggregates
        for aggregate in self._aggregations:
            transformations.add(aggregate)

        return transformations

    def transform(self, df) -> pd.DataFrame:

        """ Apply all pre conditions one at a time """
        df = self._apply_pre_conditions(df)

        """ Group the data if needed """
        assert not (self._groups and self._splits), "Groups and Splits cannot be combined"
        if self._groups:
            df = df.groupby(self._groups.columns).agg(list).reset_index()

        transformed_df = pd.DataFrame({})

        """ Split up the data into groups if needed """
        if self._splits:
            aggregates, views = defaultdict(list), defaultdict(list)

            for _, gdf in df.sort_values(self._splits.sort_by.columns).groupby(self._splits.group.columns):
                gdf = self._apply_split_conditions(gdf)

                curr_df = pd.DataFrame({column: gdf[column] for column in self._splits.sort_by.columns})
                curr_df, curr_aggregates, curr_views = self._apply_transform(curr_df, gdf, is_split=True)

                transformed_df = pd.concat([transformed_df, curr_df], axis=0)
                for key, values in curr_aggregates.items():
                    aggregates[key] += list(values)
                for key, values in curr_views.items():
                    aggregates[key] += list(values)

        else:
            transformed_df, aggregates, views = self._apply_transform(transformed_df, df, is_split=False)

        """ Apply all post conditions """
        transformed_df = self._apply_post_conditions(transformed_df, df, aggregates, views)

        """ Finally Index the Dataframe """
        if self._indexes.columns:
            if len(self._indexes.columns) == 1:
                index = self._indexes.columns[0]
            else:
                index = list(self._indexes.columns)

            transformed_df = transformed_df.set_index(index).sort_index()

        return transformed_df

    def __call__(self, *args, **kwargs) -> pd.DataFrame:
        if hasattr(self, "initialize"):
            df = to_df(type(self).initialize(*args, **kwargs))
        else:
            assert args and not kwargs, "Transformer only accepts a sigle DataFrame"
            df = to_df(args[0])

        transformed = self.transform(df)

        if hasattr(self, "finalize"):
            return type(self).finalize(transformed)
        else:
            return transformed
