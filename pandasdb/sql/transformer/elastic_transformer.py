from pandasdb.sql.transformer import Transformer
import pandas as pd
from collections import defaultdict

from pandasdb.sql.utils import iterable


class ElasticTransformer(Transformer):

    @classmethod
    def __apply__func__(cls, function, input_df, output_df, target_column, split, id_column, **kwargs):
        if not id_column in output_df:
            output_df[id_column] = input_df[id_column]

        print("apply")

        if not split:
            rows = []
            for input_row in input_df.to_dict(orient="records"):
                ID = input_row.pop(id_column)
                for output_row in output_df[output_df[id_column] == ID].to_dict(orient="records"):
                    for result in function(**input_row):
                        copy = output_row.copy()
                        copy[target_column] = result
                        rows.append(copy)

            return pd.DataFrame(rows)
        else:
            raise NotImplementedError("Cannot expand splittet dataframes at the moment")

    @classmethod
    def __apply__(cls, df, new_df, split):
        aggregates = {}
        views = {}

        expanded = defaultdict(lambda: defaultdict(list))

        for column, input_columns, col_type in cls._dag_(df.columns):
            if col_type == "aggregates":
                sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates, views)
                function = cls.__transforms__[cls.__name__][col_type][column][1]
                aggregates[column] = function(**sub_df.to_dict(orient="series"))
            elif col_type == "columns":
                _, function, kwargs = cls.__transforms__[cls.__name__]["columns"][column]
                sub_df = cls.__prepare_data__(input_columns, df, new_df, aggregates, views)

                if function is None:
                    # just copy the function and move on
                    for id, value in zip(df[cls.ID].values, sub_df[input_columns[0]].values):
                        expanded[id][column].append(value)

                elif split:
                    id = df[cls.ID].iloc[0]
                    row = sub_df.to_dict(orient="series")
                    result = function(**row)
                    if iterable(result):
                        for value in result:
                            expanded[id][column].append(value)
                    else:
                        expanded[id][column].append(result)
                else:
                    for id, row in zip(df[cls.ID].values, sub_df.to_dict(orient="records")):
                        for result in function(**row):
                            if iterable(result):
                                for value in result:
                                    expanded[id][column].append(value)
                            else:
                                expanded[id][column].append(result)

        data = defaultdict(list)
        for ID, records in expanded.items():
            max_length = max([len(values) for values in records.values()])
            for column, values in records.items():
                data[column] += values + [values[-1] for _ in range(max_length - len(values))]

            data[cls.ID] += [ID for _ in range(max_length)]

        new_df = pd.DataFrame(data)

        if split:
            return new_df
        else:
            return new_df, aggregates, views
