from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

default_args = {'owner': 'airflow'}

with DAG(
    dag_id='source_order_duplicate_random_record',
    start_date=datetime(2021, 6, 29),
    schedule_interval='*/3 * * * *',
    default_args=default_args,
    catchup=False,
) as dag:

    sql_operator = MySqlOperator(
        task_id='duplicate_rand_rec',
        mysql_conn_id='mysql_ubuntu',
        sql='/sql/source_duplicate_random_record.sql'
    )
