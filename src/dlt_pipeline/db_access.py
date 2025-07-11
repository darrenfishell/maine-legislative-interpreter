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
            return conn.execute(check).fetchone()[0]

    def stream_query_results(self, query):
        """
        Stream query results one row at a time.
        
        Args:
            query (str): SQL query to execute
            column_names (list, optional): Column names for the result set.
                                         If None, will use default DuckDB column names.
        
        Yields:
            dict: Each row as a dictionary with column names as keys
        """
        with duckdb.connect(self.db_path) as conn:
            # Execute the query and get results
            result = conn.execute(query)
            
            
            column_names = [desc[0] for desc in result.description]
            
            # Stream results one row at a time
            for row in result.fetchall():
                yield dict(zip(column_names, row))

    def stream_unprocessed_documents(self, bronze_schema, silver_schema):
        """
        Stream unprocessed documents for text processing.
        
        Args:
            bronze_schema (str): Schema containing raw document data
            silver_schema (str): Schema containing processed sentence data
        
        Yields:
            dict: Each document with doc_id and doc_text
        """
        if self.table_exists(silver_schema, 'testimony_sentences'):
            query = f'''
                SELECT doc_id, doc_text
                FROM {bronze_schema}.testimony_full_text
                WHERE doc_id NOT IN (
                    SELECT DISTINCT doc_id 
                    FROM {silver_schema}.testimony_sentences
                )
                AND doc_text IS NOT NULL 
                AND LENGTH(doc_text) > 0
                ORDER BY doc_id
            '''
        else:
            query = f'''
                SELECT doc_id, doc_text
                FROM {bronze_schema}.testimony_full_text
                WHERE doc_text IS NOT NULL 
                AND LENGTH(doc_text) > 0
                ORDER BY doc_id
            '''
        
        yield from self.stream_query_results(query)


