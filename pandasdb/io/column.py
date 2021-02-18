import sqlalchemy.types as sat
import sqlalchemy.schema as sas


class ColumnSchema(sas.Column):
    def __init__(self, name, type, *args, **kwargs):
        sas.Column.__init__(self, name, type, *args, **kwargs)


class IntegerColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.Integer, *args, **kwargs)


class StringColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.String, *args, **kwargs)


class TextColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.String(length=65535), *args, **kwargs)


class FloatingColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.Float, *args, **kwargs)


class BooleanColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.Boolean, *args, **kwargs)


class DateColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.Date, *args, **kwargs)


class DateTimeColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.DateTime, *args, **kwargs)


class JSONColumn(ColumnSchema):
    def __init__(self, name, *args, **kwargs):
        ColumnSchema.__init__(self, name, sat.JSON, *args, **kwargs)
