import duckdb
import pandas as pd
import os
from pathlib import Path


class Database:
    def __init__(self, db_name, schema):
        self.db_name = db_name + '.duckdb'
        self.data_root = Path(__file__).resolve().parents[2] / 'data'
        self.db_path = self.data_root / self.db_name
        self.schema = schema
        os.makedirs(self.data_root, exist_ok=True)

    def latest_loaded_session(self) -> int:
        query = f'''
            SELECT MAX(legislature)
            FROM {self.schema}.bill_text
        '''
        with duckdb.connect(self.db_path) as conn:
            try:
                start_session = conn.sql(query).fetchone()[0]
            except:
                start_session = 122
            return start_session

    def get_testimony_metadata_inputs(self, session) -> pd.DataFrame:
        '''
        Gets all paper numbers from a given session.
        '''
        query = f'''
            SELECT DISTINCT paper_number
            FROM {self.schema}.bill_text
            WHERE legislature = {session}
        '''
        with duckdb.connect(self.db_path) as conn:
            return conn.sql(query).df()

    def get_testimony_doc_ids(self, session) -> pd.DataFrame:
        query = f'''
            SELECT id
            FROM {self.schema}.testimony_attributes
            WHERE legislature = {session}
        '''
        with duckdb.connect(self.db_path) as conn:
            return conn.sql(query).df()['id'].to_list()

    def clean_orgs(self):
        '''
        Performs distance-based clustering on org
        names and writes to database table.
        '''
