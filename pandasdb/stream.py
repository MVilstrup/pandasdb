from typing import Optional, List, Tuple, Callable, Iterator

import pandas as pd
from datetime import datetime

from pandasdb.record import Record


class Stream:

    def __init__(self, conn, query, length: int):
        self.table_length: int = length
        self.conn = conn
        self.query = query
        self._stream: Optional[Iterator] = None
        self._transformations: List[Tuple[str, Callable]] = []
        self.aligners: List[ForwardAligner] = []
        self._batch_size: int = max(2000, self.table_length // 10)
        self._last_iteration: Optional[datetime] = None

    def __iter__(self):
        return self

    def __next__(self) -> Record:
        # We might have consumed a bit from the stream object in a Notebook
        # We thus restart the iterator when is has not been used for more than 2 seconds
        if self._last_iteration is not None:
            if (datetime.now() - self._last_iteration).seconds > 1:
                self._stream = None

        # Indicate we just used the stream
        self._last_iteration = datetime.now()

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

    def df(self) -> pd.DataFrame:
        self._batch_size = self.table_length
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

    def align(self, record: Record) -> Record:
        for key, value in record:
            if key not in self._seen_keys:
                self._seen_keys[key] = None

        for key in self._seen_keys.keys():
            if key not in record:
                record.update(key, None)

        return record
