import ibis

from pandasdb.sql.connection.connection import Connection


class PandasConnection(Connection):

    def __init__(self, name="", host="", schema="public", username="", password="", port=-1, database="", tunnel=None,
                 ssh_username=None, ssh_key=None, type="", tables=None):
        Connection.__init__(self, ibis.pandas.connect, name=name, host=host, schema=schema, username=username,
                            password=password,
                            port=port, database=database, tunnel=tunnel, ssh_username=ssh_username, ssh_key=ssh_key,
                            type=type, tables=tables)
