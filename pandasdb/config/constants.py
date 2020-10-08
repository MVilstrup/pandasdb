import os

CONNECTION_PATH = os.path.expanduser(f"~{os.sep}.pandas_db{os.sep}connections.json")

POSTGRES_TYPE = "POSTGRES"
REDSHIFT_TYPE = "REDSHIFT"