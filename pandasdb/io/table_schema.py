from functools import partial
from sys import getsizeof

import psycopg2
import pandas as pd
from sqlalchemy import Table, MetaData, UniqueConstraint
import numpy as np

from pandasdb.transformer import clean_df


class TableSchema:

    def __init__(self, database, name, *columns, unique_constraints={}):
        self.database = database
        self.name = name
        self._columns = {column.name: column for column in columns}
        self._dtypes = {column.name: column.type for column in columns}

        constraints = []
        for constrain_name, constraint_columns in unique_constraints.items():
            constraints.append(UniqueConstraint(*constraint_columns, name=f"{self.name}_{constrain_name}"))

        self._table = Table(name, MetaData(), *columns, *constraints)

    def replace_with(self, df: pd.DataFrame):
        df = clean_df(df)
        database = self.database()
        self._table.schema = database.configuration.schema
        self._table.drop(database.engine(), checkfirst=True)
        self._table.create(database.engine())

        # Change all NaN values to None in to store it properly in the database
        df = df.astype(object).where(pd.notnull(df), None)

        selected_columns = df[list(self._columns.keys())]

        self.insert(database, self.name, selected_columns)


    @staticmethod
    def insert(database, name, df):
        def generate_chunk(tuples, schema, name, columns, mogrify):
            # Comma-separated dataframe columns
            cols = ','.join(list(columns))

            # SQL query to execute
            value_shema = ', '.join(['%s' for _ in range(len(columns))])

            values = b','.join([b"(" + mogrify(value_shema, x) + b")" for x in tuples])
            return f"INSERT INTO {schema}.{name}({cols}) VALUES " + values.decode('utf-8')

        chunker = partial(generate_chunk,
                          schema=database.configuration.schema,
                          name=name,
                          columns=df.columns,
                          mogrify=database.engine().raw_connection().cursor().mogrify)

        value_tuples = [tuple(x) for x in df.to_numpy()]

        with database.connect() as connection:
            connection = connection.execution_options(schema_translate_map={None: database.configuration.schema})

            optimum_query_size = 8e+6
            orig_size = getsizeof(chunker(value_tuples))

            if orig_size > optimum_query_size:
                num_chunks = max(orig_size // optimum_query_size, 1)
            else:
                num_chunks = 1

            queries = list(map(chunker, np.array_split(value_tuples, num_chunks)))
            for query in queries:
                try:
                    connection.execute(query)
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error: %s" % error)
