from pandas.api.types import is_string_dtype


def convert_types(column, dtype):
    if is_string_dtype(column):
        return column.astype(str)

    return column
