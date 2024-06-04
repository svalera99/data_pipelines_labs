from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator


with DAG(dag_id="create_table_dag") as dag:
    b_create = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="airflow_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS full_measures
        (
        timestamp TIMESTAMP,
        temp FLOAT,
        clouds INTEGER,
        wind_speed FLOAT,
        humidity INTEGER,
        city STRING
        );"""
    )
