import sqlalchemy.types as sat
import sqlalchemy.schema as sas


class TableBuilder:
    def __init__(self, schema, name):
        self.schema = schema
        self.name = name

        self.alt_schemas = []
        self.columns = []

    def dublicate_endpoint(self, schema):
        self.alt_schemas.append(schema)
        return self

    def String(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.String, *args, **kwargs))
        return self

    def Text(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.String(length=65535), *args, **kwargs))
        return self

    def Integer(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.Integer, *args, **kwargs))
        return self

    def JSON(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.JSON, *args, **kwargs))
        return self

    def DateTime(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.DateTime, *args, **kwargs))
        return self

    def Date(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.Date, *args, **kwargs))
        return self

    def Boolean(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.Boolean, *args, **kwargs))
        return self

    def Float(self, name, *args, **kwargs):
        self.columns.append(sas.Column(name, sat.Float, *args, **kwargs))
        return self

    def build(self):
        from pandasdb.services.databases.src.table import Table
        table = Table(self.name, schema=self.schema, is_view=False)
        table._columns = self.columns
        table._dublication_endpoints = self.alt_schemas
        return table

    def __getattr__(self, item):
        try:
            return self.__getattribute__(item)
        except AttributeError:
            raise AttributeError(f"'TableBuilder' object has no attribute '{item}'. You probably forgot to call '.build()'")
