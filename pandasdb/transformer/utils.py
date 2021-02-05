import pandas as pd


def to_transformer_name(name):
    return f"{name}Transformer"


def to_df(df):
    if not isinstance(df, pd.DataFrame):
        df = df.df()

    if type(df.index) != pd.RangeIndex:
        df = df.reset_index()

    return df
