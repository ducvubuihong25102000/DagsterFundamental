from typing import Any
from dagster._core.execution.context.input import InputContext
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine
import psycopg2

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        # TODO: your code here
        # insert new data from Pandas Dataframe to PostgreSQL table
        conn_string = 'postgresql://admin:admin123@localhost:5432/postgres'
        engine = create_engine(conn_string)
        conn = engine.connect()

        obj.to_sql('olist_orders_dataset', con=conn, if_exists='replace', index=False)

        conn.close()
    
    def load_input(self, context: InputContext):
        pass