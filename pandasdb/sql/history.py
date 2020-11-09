import pandas as pd
import numpy as np
from datetime import datetime, timedelta


class DataSlice:
    def __init__(self, start, end, df, as_table=False):
        from pandasdb.sql.table import Table
        self.START = start
        self.END = end
        if as_table:
            self.DF = Table.from_df(df)
        else:
            self.DF = df

    def __getattr__(self, item):
        try:
            getattr(self.DF, item)
        except:
            return self.__getattribute__(item.upper())

    def __getitem__(self, item):
        return self.DF[item]

    def __len__(self):
        return len(self.DF)

    def __bool__(self):
        return len(self.DF) > 0


class SpanIndex:

    def __init__(self, freq, length, start, end):
        self.idx = np.empty(length)
        self.idx[:] = np.NaN
        self.freq = freq
        self.start = start
        self.end = end

        freq = freq.lower()

        tmp_bin = pd.date_range(self.start, self.end, freq=freq)
        end_offset = (tmp_bin[-1] - tmp_bin[-2])

        bin_at = pd.date_range(self.start, self.end + end_offset, freq=freq)
        self.bin_endpoints = list(zip(bin_at, bin_at[1:]))

    def insert(self, start, index):
        new_idx = np.where(index, start.timestamp(), np.NaN)

        not_seen_before = (np.isnan(self.idx) & ~np.isnan(new_idx))

        self.idx[not_seen_before] = start.timestamp()

    def get(self, start, end):
        return np.where((self.idx >= start.timestamp()) & (self.idx < end.timestamp()), True, False)

    def bins(self):
        for start, end in self.bin_endpoints:
            yield (start, end), self.get(start, end)


class History:
    def __init__(self, df: pd.DataFrame, on_column: str, freq: str, as_table=False):
        self._on_column = on_column
        self._start = df[on_column].min()
        self._end = df[on_column].max()
        self._df = df
        self._iter = None
        self._aggregate = None
        self._with_before = False
        self._with_after = False
        self._as_table = as_table

        self._index = SpanIndex(freq, len(df), self._start, self._end)

        for curr_start, curr_end in self._index.bin_endpoints:
            indexes = ((df[on_column] >= curr_start) & (df[on_column] < curr_end)).values
            self._index.insert(curr_start, indexes)

    def _all_windows(self):
        for (start, end), indexes in self._index.bins():
            df = self._df[indexes]
            yield DataSlice(start=start, end=end, df=df, as_table=self._as_table)

    def _before_window(self, window_start):
        date_start = self._index.start
        df = self._df[self._index.get(date_start, window_start)]
        return DataSlice(start=date_start, end=window_start, df=df, as_table=self._as_table)

    def _after_window(self, window_end):
        data_end = self._index.end
        df = self._df[self._index.get(window_end, data_end)]
        return DataSlice(start=window_end, end=data_end, df=df, as_table=self._as_table)

    def include_before(self):
        self._with_before = True
        return self

    def include_after(self):
        self._with_after = True
        return self

    def include(self, before=None, after=None):
        if before is not None:
            self._with_before = before
        if after is not None:
            self._with_after = after

    def aggregate(self, aggregate):
        if isinstance(aggregate, dict):
            self._aggregate = lambda *args, **kwargs: {key: func(*args, **kwargs) for key, func in aggregate.items()}
        elif callable(aggregate):
            self._aggregate = aggregate
        else:
            raise ValueError("aggregate should either be a of type function or dict[str, function]")

        return self

    def __iter__(self):
        return self

    def __next__(self):
        if self._iter is None:
            self._iter = iter(self._all_windows())

        window = next(self._iter)
        if self._with_before and self._with_after:
            arguments = [self._before_window(window.start), window, self._after_window(window.end)]
        elif self._with_before:
            arguments = [self._before_window(window.start), window]
        elif self._with_after:
            arguments = [window, self._after_window(window.end)]
        else:
            arguments = [window]

        if self._aggregate is not None:
            return self._aggregate(*arguments)
        else:
            return arguments if len(arguments) > 1 else arguments[0]

    def df(self):
        assert self._aggregate is not None, "df can only be used in combination with an aggregate function"
        data = list(iter(self))
        return pd.DataFrame(data, index=list(map(lambda x: x[0], self._index.bin_endpoints)))
