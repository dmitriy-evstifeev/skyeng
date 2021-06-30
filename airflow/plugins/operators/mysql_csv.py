from airflow.models.baseoperator import BaseOperator
from airflow.hooks.mysql_hook import MySqlHook
from pandas import DataFrame
from datetime import datetime
import os


class MySQLTabletoCsvOperator(BaseOperator):
    def __init__(self, conn_id, schema, table, sql, savedir, header, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.schema = schema
        self.table = table
        self.sql = sql
        self.savedir = savedir
        self.header = header


    def get_previous_dump(self, dir, table):
        files = os.listdir(dir)
        for file in files:
            if table in file:
                return file

    def dump_to_csv(self, data):
        dt_format = '%Y%m%d_%H%M%S'
        curr_dt = datetime.now()
        filename = f'{self.table}_{datetime.strftime(curr_dt, dt_format)}.csv'
        prev_dump = self.get_previous_dump(self.savedir, self.table)

        sql_df = DataFrame(data)
        sql_df.to_csv(self.savedir + filename, header=self.header, index=False)
        os.unlink(self.savedir + prev_dump)
        # print('everything is fine')


    def execute(self, context):
        hook = MySqlHook(mysql_conn_id=self.conn_id, schema=self.schema)
        records = hook.get_records(sql=self.sql)
        self.dump_to_csv(records)