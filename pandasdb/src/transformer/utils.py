import pandas as pd


def to_transformer_name(name):
    return f"{name.split('.')[-1]}Transformer"

def invert_tranformer_name(name):
    return name[:-11]


def to_df(df):
    if not isinstance(df, pd.DataFrame):
        df = df.df()

    if not isinstance(df.index, (pd.Int64Index, pd.RangeIndex)):
        df = df.reset_index()

    return df
