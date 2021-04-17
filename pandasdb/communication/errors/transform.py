from pandasdb.communication.errors.base import PandasDBException


class PandasTransformError(PandasDBException):
    pass

class ColumnGenerationError(PandasTransformError):
    pass

class EmptyDataFrameError(PandasTransformError):
    pass

class EmptyColumnError(PandasTransformError):
    pass