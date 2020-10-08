import os
import sys
import json
from pandasdb.config.constants import CONNECTION_PATH, POSTGRES_TYPE, REDSHIFT_TYPE
from pandasdb.connections import PostgresConnection, RedshiftConnection


def add_db(type, name, username, password, host, database, port, ssh_key=None, tunnel=None, ssh_username=None):
    config = {
        "type": type,
        "username": username,
        "password": password,
        "tunnel": tunnel,
        "ssh_username": ssh_username,
        "host": host,
        "database": database,
        "port": port,
        "ssh_key": ssh_key,
    }

    if not os.path.exists(CONNECTION_PATH):
        folder_path = os.sep.join(CONNECTION_PATH.split(os.sep)[:-1])
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

        existing_databases = {}
    else:
        existing_databases = json.load(open(CONNECTION_PATH))

    existing_databases[name] = config
    json.dump(existing_databases, open(CONNECTION_PATH, "w"), indent=4, sort_keys=True)
    _refresh_dbs()


def add_postgress(name, username, password, host, database, port, ssh_key=None, tunnel=None, ssh_username=None):
    add_db("POSTGRESS", name, username, password, host, database, port, ssh_key, tunnel, ssh_username)


def add_redshift(name, username, password, host, database, port, ssh_key=None, tunnel=None, ssh_username=None):
    add_db("POSTGRESS", name, username, password, host, database, port, ssh_key, tunnel, ssh_username)


def _refresh_dbs():
    if os.path.exists(CONNECTION_PATH):
        def to_db(info):
            db_type = info.pop("type")
            if db_type == POSTGRES_TYPE:
                return PostgresConnection(**info)
            else:
                return RedshiftConnection(**info)

        dbs = type("DataBaseList", (), {name: to_db(info) for name, info in json.load(open(CONNECTION_PATH)).items()})
        setattr(sys.modules["pandasdb"], "DBs", dbs)

_refresh_dbs()