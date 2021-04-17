import pandas as pd


class FlexTable:

    @staticmethod
    def elongate(df=None, type_column=None, value_column=None, columns=None, renamed_columns=None):
        assert type_column is not None
        assert value_column is not None
        assert not (columns is None and renamed_columns is None)

        if columns is None:
            columns = []
        if renamed_columns is None:
            renamed_columns = {}

        if df is None:
            return lambda df: FlexTable.elongate(df, type_column, value_column, columns, renamed_columns)

        if len(columns) == 1 and isinstance(columns[0], dict):
            renamed_columns.update(columns[0])
            columns = []

        for column in columns:
            renamed_columns[column] = column

        df = df.rename(columns=renamed_columns)
        target_columns = list(renamed_columns.values())

        index = df.index
        idx_df = df[[col for col in df.columns if col not in target_columns]]

        dfs = []
        for column in target_columns:
            curr_df = pd.DataFrame({value_column: df[column]}, index=index)
            curr_df = pd.concat([idx_df, curr_df], axis=1)
            curr_df[type_column] = column
            dfs.append(curr_df)

        return pd.concat(dfs, axis=0)

    @staticmethod
    def check(param, value):
        if isinstance(value, str):
            value = [value, value]

        conditions = all([isinstance(value, (tuple, list)), len(value) == 2])
        assert conditions, f"{param} be of format: (id_left, id_right) or common_id"
        return value

    @staticmethod
    def row_generator(df, column_name, function, column_inputs):
        rows = []
        for row in df.to_dict(orient="records"):
            for result in function(**{column: row[column] for column in column_inputs}):
                row_copy = row.copy()
                row_copy[column_name] = result
                rows.append(row_copy)

        return pd.DataFrame(rows)

    @staticmethod
    def expand_column(df, target_column, **include_columns):
        def not_collection(column):
            column = column[column.notna()]
            if len(column) == 0:
                return True

            return not isinstance(column.iloc[0], (list, set, dict))

        constants = [const_column for const_column in df.columns if not_collection(df[const_column])]
        if not include_columns:
            for constant_column in constants:
                include_columns[constant_column] = constant_column

        constant_df = df.rename(columns=include_columns)[list(include_columns.values())]

        rows = []
        for row, target_variables in zip(constant_df.to_dict(orient="records"), df[target_column].values):
            for variable in target_variables:
                row_copy = row.copy()
                row_copy[target_column] = variable
                rows.append(row_copy)

        return pd.DataFrame(rows)

    @staticmethod
    def multi_join(left, right, in_common, unique, names):
        left, right = clean_df(left, right)

        in_common = FlexTable.check("in_common", in_common)
        unique = FlexTable.check("unique", unique)

        # Handle column naming
        name_left, name_right = names
        left_index, right_index = in_common

        column_overlap = (set(left.columns) & set(right.columns)) - set(in_common)

        left = left.rename(columns={column: f"{name_left}_{column}" for column in column_overlap})
        right = right.rename(columns={column: f"{name_right}_{column}" for column in column_overlap})

        left_unique, right_unique = unique
        left_unique = left_unique if left_unique not in column_overlap else f"{name_left}_{left_unique}"
        right_unique = right_unique if right_unique not in column_overlap else f"{name_right}_{right_unique}"

        left.dropna(subset=[left_index], inplace=True)
        left.set_index(left_index, inplace=True)

        right.dropna(subset=[right_index], inplace=True)
        right.set_index(right_index, inplace=True)

        right_on_left = left.join(right, how="left")
        left_on_right = right.join(left, how="left")

        all_rows = pd.concat([right_on_left, left_on_right], axis=0).dropna(subset=[left_unique, right_unique])
        unique_rows = all_rows.drop_duplicates(subset=[left_unique, right_unique])

        return unique_rows.reset_index()


def clean_df(*dfs):
    if len(dfs) > 1:
        return list(map(clean_df, dfs))

    df = dfs[0]

    if not isinstance(df, pd.DataFrame):
        df = df.df()

    if type(df.index) != pd.RangeIndex:
        df = df.reset_index()

    return df
