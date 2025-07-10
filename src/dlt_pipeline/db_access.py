import duckdb
import pandas as pd
import os
from pathlib import Path


class Database:
    def __init__(self, db_name, bronze_schema, silver_schema, gold_schema):
        self.db_name = db_name + '.duckdb'
        self.data_root = Path(__file__).resolve().parents[2] / 'data'
        self.db_path = self.data_root / self.db_name
        self.bronze_schema = bronze_schema
        self.silver_schema = silver_schema
        self.gold_schema = gold_schema
        os.makedirs(self.data_root, exist_ok=True)

    def latest_loaded_session(self) -> int:
        query = f'''
            SELECT MAX(legislature)
            FROM {self.bronze_schema}.bill_text
        '''
        with duckdb.connect(self.db_path) as conn:
            try:
                start_session = conn.sql(query).fetchone()[0]
            except:
                start_session = 122
            return start_session

    def get_query_as_df(self, query) -> pd.DataFrame:
        with duckdb.connect(self.db_path) as conn:
            return conn.execute(query).df()

    def table_exists(self, schema, table_name):
        with duckdb.connect(self.db_path) as conn:
            check = f'''
                SELECT COUNT(*) > 0
                FROM information_schema.tables
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name}'
            '''
            return conn.execute().fetchone()[0]

    def get_unprocessed_document_batch(self, bronze_schema="bronze", silver_schema="silver", batch_size=1000):

        # Check table existence once at the start
        with duckdb.connect(self.db_path) as conn:
            table_exists = conn.execute(f"""
                SELECT COUNT(*) > 0
                FROM information_schema.tables 
                WHERE table_schema = '{silver_schema}' 
                AND table_name = 'testimony_sentences'
            """).fetchone()[0]

        # Build query based on table existence
        if table_exists:
            base_query = f"""
                SELECT doc_id, doc_text
                FROM {bronze_schema}.testimony_full_text
                WHERE doc_id NOT IN (
                    SELECT DISTINCT doc_id 
                    FROM {silver_schema}.testimony_sentences
                )
                ORDER BY doc_id
            """
        else:
            base_query = f"""
                SELECT doc_id, doc_text
                FROM {bronze_schema}.testimony_full_text
                ORDER BY doc_id
            """

        offset = 0

        while True:
            query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"

            with duckdb.connect(self.db_path) as conn:
                docs_df = conn.execute(query).fetch_df()

            # Stop if no more results
            if docs_df.empty:
                break

            yield docs_df

            offset += batch_size


