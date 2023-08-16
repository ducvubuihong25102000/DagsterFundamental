import pandas as pd
import mysql.connector as sql_connector

class MySQLIOManager:
    print("mysql_io_manager")
    def __init__(self, config):
        # config for connecting to MySQL database
        self._config = config
    
    def extract_data(self, sql: str) -> pd.DataFrame:
        # YOUR CODE HERE
        # connect to MySQL database, use input sql statement to retrieve the data
        # e.g. SELECT * FROM table_A -> Pandas Dataframe
        db_connection = sql_connector.connect(host='localhost', database='mysql_db', user='admin', password='admin123')

        df = pd.read_sql('SELECT * FROM olist_orders_dataset', con=db_connection)

        return df