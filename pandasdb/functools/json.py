from pandasdb.utils.aync import Async
from pandasdb.utils import string_to_python_attr
from random import sample
import json
from pandasdb.column import Column

class Json:
    def __init__(self, column: Column, num_samples=3):
        self._column = column
        self._keys = []

        # Generate a list of indices which can be used to extract rows from various parts of the table
        batches = sample(range(0, column.length, 10), num_samples)
        print(batches)

        jobs = [self._column.take(amount=10, offset=idx).df for idx in batches]
        for result in Async.handle(*jobs):
            for row in map(json.loads, result):
                for key, value in row.items():
                    if not hasattr(self, key) and value is not None:
                        json_col = self._column._ops.Column(name=f"{self._column.name}{self.symbol}'{key}'",
                                                            dtype=type(value),
                                                            supported_ops=self._column._ops)

                        json_col.table = self._column.table
                        setattr(self, string_to_python_attr(key), json_col)
                        self._keys.append(json_col)

    def explode(self, degree=1):
        return self._keys
