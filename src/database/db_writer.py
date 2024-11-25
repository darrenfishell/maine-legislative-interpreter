import duckdb
import pandas as pd
from pathlib import Path
from typing import List

class Database:
    def __init__(self, db_name):
        self.db_name = db_name + '.duckdb'
        self.data_root = Path(__file__).resolve().parents[2] / 'data'
        self.db_path = self.data_root / self.db_name

    def write_to_db(self, df, table_name, method='append'):
        with duckdb.connect(self.db_path) as conn:
            if method == 'append':
                duckdb.sql(f'INSERT INTO {table_name} SELECT * FROM df')
            else:
                duckdb.sql(f'CREATE TABLE {table_name} AS SELECT * FROM df')
            df.to_sql(name=table_name, con=conn, if_exists=method, index=False)

    def return_query_as_df(self, query):
        with duckdb.connect(self.db_path) as conn:
            return conn.sql(query).df()

    def truncate_tbl(self, table_name):
        with duckdb.connect(self.db_path) as conn:
            cursor = conn.cursor()
            delete_query = f'''TRUNCATE {table_name};'''
            print(f"Truncated: {table_name}")  # To show what is being executed (optional)
            try:
                cursor.execute(delete_query)
                conn.commit()
            except Exception as e:
                print('Table does not exist, will create anew.')

    def get_bill_text_sessions(self, session: int) -> pd.DataFrame:
        query = '''
                SELECT DISTINCT legislature 
                FROM BILL_TEXT
                '''
        with duckdb.connect(self.db_path) as conn:
            return pd.read_sql(query, con=conn)

    def get_testimony_metadata_inputs(self, session) -> pd.DataFrame:
        query = f'''
            SELECT DISTINCT paperNumber, legislature
            FROM BILL_TEXT
            WHERE legislature = {session}
        '''
        with duckdb.connect(self.db_path) as conn:
            return pd.read_sql(query, con=conn)

    def get_testimony_doc_ids(self, session) -> pd.DataFrame:
        query = f'''
            SELECT Id
            FROM TESTIMONY_HEADER
            WHERE legislature = {session}
        '''
        df = self.return_query_as_df(query)
        return df['Id'].to_list()

    def clean_orgs(self):
        '''
        Performs distance-based clustering on org
        names and writes to database table.
        '''
