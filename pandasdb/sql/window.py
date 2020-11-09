from collections import defaultdict
import pandas as pd
import numpy as np


class TimeIndex:

    def __init__(self, df, on_column):
        self.idx = np.empty(len(df))
        self.idx[:] = np.NaN
        self.on_column = on_column

        self._df = df
        self._time_index = df[self.on_column].apply(lambda dt: pd.to_datetime(dt, utc=True))

        self.start, self.end = self._time_index.min(), self._time_index.max()

    def get(self, start, end):
        start, end = pd.to_datetime(start, utc=True), pd.to_datetime(end, utc=True)
        return self._df[(self._time_index >= start) & (self._time_index < end)]

    def __getitem__(self, date_tuple):
        start, end = date_tuple
        return self.get(start, end)


class Window:

    def __init__(self, freq):
        self.freq = freq
        self._indexes = defaultdict(dict)
        self._format = self._to_tuples

    @property
    def start(self):
        all_start_dates = []
        for name, index_columns in self._indexes.items():
            for column, time_index in index_columns.items():
                all_start_dates.append(time_index.start)

        return min(all_start_dates)

    @property
    def end(self):
        all_end_dates = []
        for name, index_columns in self._indexes.items():
            for column, time_index in index_columns.items():
                all_end_dates.append(time_index.end)

        return min(all_end_dates)

    @property
    def date_bins(self):
        tmp_range = pd.date_range(self.start, self.end, freq=self.freq)
        end_offset = (tmp_range[-1] - tmp_range[-2])

        # Ensuring the end of the range includes the final datapoint, by adding an offset
        ranges = pd.date_range(self.start, self.end + end_offset, freq=self.freq)
        return list(zip(ranges, ranges[1:]))

    def _get(self, start, end, include_empty=False):
        data = defaultdict(dict)

        for name, index_columns in self._indexes.items():
            for column, time_index in index_columns.items():
                df = time_index[start, end]

                if len(df) > 0 or include_empty:
                    data[name][column] = df

        return data

    def _to_tuples(self):
        for start, end in self.date_bins:
            tuples = []

            for name, index_columns in self._get(start, end, include_empty=True).items():
                if len(index_columns) == 1:
                    tuples.append(list(index_columns.values())[0])
                else:
                    tuples.append(tuple(index_columns.values()))

            yield tuples

    def _to_dict(self):
        for start, end in self.date_bins:
            yield self._get(start, end)

    def as_tuples(self):
        self._format = self._to_tuples
        return self

    def as_dict(self):
        self._format = self._to_dict
        return self

    def add(self, data, name, on, fill_na="False"):
        if not isinstance(data, pd.DataFrame):
            try:
                df = data.df()
            except:
                raise ValueError("data should be a DataFrame or have a .df() function")
        else:
            df = data

        if not (isinstance(on, list) or isinstance(on, tuple)):
            on = [on]

        for on_column in on:
            if fill_na is not "False":
                df[on_column] = df[on_column].fillna(fill_na)
            self._indexes[name][on_column] = TimeIndex(df, on_column)

        return self

    def __len__(self):
        return len(self.date_bins)

    def __iter__(self):
        return iter(self._format())
