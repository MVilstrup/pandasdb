class PandasTransformError(Exception):
    pass

class ColumnGenerationError(ValueError):
    pass

class EmptyDataFrameError(PandasTransformError):
    pass

class EmptyColumnError(PandasTransformError):
    pass