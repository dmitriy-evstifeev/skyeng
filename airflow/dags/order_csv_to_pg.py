from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators.csv_postgres import PostgresCsvtoTableOperator

from datetime import datetime


default_args = {'owner': 'airflow'}
directory = '/home/playwright/airflow/dumps/'
prev_source_table = 'temp.mysql_order_previous'
curr_source_table = 'temp.mysql_order_current'

sql = [
    f'truncate table {prev_source_table};',
    f'insert into {prev_source_table} select * from {curr_source_table};',
    f'truncate table {curr_source_table};'
]

with DAG(
    dag_id='dump_extracting_processing',
    start_date=datetime(2021, 6, 29),
    schedule_interval='*/5 * * * *',
    default_args=default_args,
) as dag:

    prepare_tables = PostgresOperator(
        task_id='order_tables_preparing',
        postgres_conn_id='postgres_ubuntu',
        sql=sql
    )

    load_dump = PostgresCsvtoTableOperator(
        task_id='csv_to_temp_pg',
        conn_id='postgres_ubuntu',
        schema='dwh',
        table = 'order',
        dumpdir = directory
    )

    update_order_dwh = PostgresOperator(
        task_id='raw_order_add_rows',
        postgres_conn_id='postgres_ubuntu',
        sql='/sql/dwh_order_incr_ins.sql'
    )

    prepare_tables >> load_dump >> update_order_dwh
