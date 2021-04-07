from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pandasdb import Async
from pandasdb.io import TableSchema
from pandasdb.scheduling import subscribe_to
from datetime import datetime, timedelta


class DAGDefinition:
    start_date: datetime = days_ago(2)
    catchup: bool = False
    default_args: dict = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 5,
        'retry_delay': timedelta(minutes=5)
    }

    def __init__(self, **spec):
        self.spec = spec

        for required in ["dag_id", "description", "schedule_interval", "start_date", "catchup", "default_args", "tags"]:
            if required not in self.spec:
                try:
                    self.spec[required] = getattr(self, required)
                except:
                    raise ValueError(f"{required} not provided")

    def extraction(self):
        raise NotImplementedError()

    def transformation(self, *args, **kawrgs):
        raise NotImplementedError()

    def output_table(self) -> TableSchema:
        raise NotImplementedError()

    def local(self, write=False):
        # Extract
        tables = self.extraction()
        assert isinstance(tables, dict), "The input tables should be a dictionary"

        tables = {name: table.df if hasattr(table, "df") else table for name, table in tables.items()}

        jobs = {name: job for name, job in tables.items() if callable(job)}
        tables.update(Async.handle(**jobs))

        # Transform
        definition = self.transformation(**tables)

        # Load
        if write:
            self.output_table().replace_with(definition)
        else:
            return definition

    def create(self):
        dag = DAG(**self.spec)

        compute = PythonOperator(task_id='etl_tasks', python_callable=lambda: self.local(write=True), dag=dag)

        if hasattr(self, "dependencies"):
            subscribe_to(self.dependencies) >> compute

        globals()[self.spec["dag_id"]] = dag
        return globals()[self.spec["dag_id"]]
