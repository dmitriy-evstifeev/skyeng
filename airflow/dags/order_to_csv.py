from airflow import DAG
from operators.mysql_csv import MySQLTabletoCsvOperator
from datetime import datetime

default_args = {'owner': 'airflow'}
query = 'select * from `order`'
directory = '/home/playwright/airflow/dumps/'
csv_head = ['id', 'student_id', 'teacher_id', 'stage', 'status', 'created_at', 'updated_at']

with DAG(
    dag_id='mysql_dump_table_to_csv',
    start_date=datetime(2021, 6, 29),
    schedule_interval='*/5 * * * *',
    default_args=default_args,
) as dag:

    dump = MySQLTabletoCsvOperator(
        task_id='order_dump',
        conn_id='mysql_ubuntu',
        schema='sourcedb',
        table = 'order',
        sql = query,
        savedir = directory,
        header = csv_head
        )
