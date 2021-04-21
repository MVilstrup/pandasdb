from pandasdb.communication.errors.base import PandasDBException


class PandasDBIOError(PandasDBException):
    pass

class PandasDBConfigurationError(PandasDBIOError):
    pass