import ibis
from dbflow.configuration import Configuration

from pandasdb.io.connection.connection import Connection


class MySQLConnection(Connection):

    def __init__(self, configuration: Configuration):
        Connection.__init__(self, ibis.mysql.connect, configuration)
