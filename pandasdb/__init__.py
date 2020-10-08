import sys
import os
import json

import pandasdb.operators as ops
from pandasdb.column import Column
from pandasdb.table import Table
from pandasdb.database import DataBase
from pandasdb.connections.connection import Connection as _Connection
from pandasdb.config.constants import CONNECTION_PATH
import pandasdb.config as config


# Add all the known databases directly to the module
config.database._refresh_dbs()
