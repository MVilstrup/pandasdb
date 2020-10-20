import pandas as pd


class Stream:

    def __init__(self, conn, query, length):
        self.length = length
        self.conn = conn
        self.query = query
        self._window_size = 100
        self._stream = None
        self._transformations = []
        self._filters = []
        self.aligners = []
        self._batch_size = max(2000, self.length // 10)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            record = next(self._stream)
            for _type, transform in self._transformations:
                if _type == "APPLY":
                    record = transform(record)
                else:
                    if not transform(record):
                        return next(self)
            return record
        except TypeError:
            self._stream = iter(self.conn.stream(str(self.query), self._batch_size))
            return next(self)

    def align_records(self):
        align = ForwardAligner()
        self.aligners.append(align)
        self._transformations.append(("APPLY", align.align))
        return self

    def df(self):
        self._batch_size = self.length
        return pd.DataFrame([dict(record) for record in self])

    def apply(self, func):
        self._transformations.append(("APPLY", func))
        return self

    def filter(self, func):
        self._transformations.append(("FILTER", func))
        return self


class ForwardAligner:
    def __init__(self):
        self._seen_keys = {}

    def align(self, record):
        for key, value in record:
            if key not in self._seen_keys:
                self._seen_keys[key] = None

        for key in self._seen_keys.keys():
            if key not in record:
                record.update(key, None)

        return record
