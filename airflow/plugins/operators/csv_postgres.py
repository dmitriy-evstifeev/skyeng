from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook

import pandas as pd
import os


class PostgresCsvtoTableOperator(BaseOperator):
    def __init__(self, conn_id, schema, table, dumpdir, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.dumpdir = dumpdir
        self.prev_src_table = f'temp.mysql_{self.table}_previous'
        self.curr_src_table = f'temp.mysql_{self.table}_current'


    def get_last_dump(self, dir, table):
        files = os.listdir(dir)
        for file in files:
            if table in file:
                dump_path = dir + file
                dump = pd.read_csv(dump_path)
                data = [tuple(row) for row in dump.values]
                return data


    def load_dump_to_temp_pg(self, data):
        table = f'temp.mysql_{self.table}_current'
        hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.schema)
        hook.insert_rows(table=table, rows=data)


    def execute(self, context):
        last_dump = self.get_last_dump(self.dumpdir, self.table)
        self.load_dump_to_temp_pg(last_dump)