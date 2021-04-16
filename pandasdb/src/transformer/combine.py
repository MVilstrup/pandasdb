from pandasdb.libraries.utils import iterable
import pandas as pd


class Combine:
    def __init__(self, *transformers):
        if len(transformers) == 1 and iterable(transformers[0]):
            transformers = transformers[0]

        self.transformers = list(transformers)

    def then(self, transform):
        new = Combine(*self.transformers)
        new.transformers.append(transform)
        return new

    def on(self, df):
        return self(df)

    def __call__(self, df):
        result = df
        for transformer in self.transformers:
            if isinstance(transformer, pd.DataFrame):
                result = transformer

            result = transformer(result)
        return result
