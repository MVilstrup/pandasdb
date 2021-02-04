from pandasdb.sql.transformer.join import left_join, right_join, inner_join
from pandasdb.sql.utils import iterable


class Combine:
    def __init__(self, *transformers):
        if len(transformers) == 1 and iterable(transformers[0]):
            transformers = transformers[0]

        self.transformers = list(transformers)

    def left_join(self, right, on, names=None):
        new = Combine(*self.transformers)
        new.transformers.append(left_join(right=right, on=on, names=names))
        return new

    def right_join(self, right, on, names=None):
        new = Combine(*self.transformers)
        new.transformers.append(right_join(right=right, on=on, names=names))
        return new

    def inner_join(self, right, on, names=None):
        new = Combine(*self.transformers)
        new.transformers.append(inner_join(right=right, on=on, names=names))
        return new

    def then(self, transform):
        new = Combine(*self.transformers)
        new.transformers.append(transform)
        return new

    def on(self, df):
        return self(df)

    def __call__(self, df):
        result = df
        for transformer in self.transformers:
            result = transformer(result)
        return result
