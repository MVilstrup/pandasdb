from copy import deepcopy

from pandasdb.utils import maybe_copy


class Query:
    def __init__(self, ops, action, table, columns, where=None, limit=None,
                 offset=None, order_by=None, joins=[], group_by=[]):
        self.ops = ops
        self._action = action
        self._table = table
        self._columns = columns
        self._where = where
        self._joins = joins
        self._groups = group_by
        self._order_by = order_by
        self._limit = limit
        self._offset = offset

    def select(self, *columns):
        raise NotImplementedError("Children should implement select")

    def where(self, condition):
        raise NotImplementedError("Children should implement where")

    def join(self, statement):
        raise NotImplementedError("Children should implement join")

    def group_by(self, statement):
        raise NotImplementedError("Children should implement group_by")

    def order_by(self, *columns, ascending=True):
        raise NotImplementedError("Children should implement order_by")

    def limit(self, amount: int):
        raise NotImplementedError("Children should implement limit")

    def offset(self, statement):
        raise NotImplementedError("Children should implement offset")

    def __str__(self):
        raise NotImplementedError("Children should implement __str__")

    def copy(self):
        # Query can have many subclasses, so when copying the class we should ensure
        # to get specific
        _class = type(self)
        return _class(ops=self.ops,
                      table=self._table,
                      action=maybe_copy(self._action),
                      columns=maybe_copy(self._columns),
                      where=maybe_copy(self._where),
                      joins=maybe_copy(self._joins),
                      group_by=maybe_copy(self._groups),
                      order_by=maybe_copy(self._order_by),
                      limit=maybe_copy(self._limit),
                      offset=maybe_copy(self._offset))
