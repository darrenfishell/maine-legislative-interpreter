import duckdb
import pandas as pd
import os
from pathlib import Path


class Database:
    def __init__(self, db_name, raw_schema, staging_schema):
        self.db_name = db_name + '.duckdb'
        self.data_root = Path(__file__).resolve().parents[2] / 'data'
        self.db_path = self.data_root / self.db_name
        self.raw_schema = raw_schema
        self.staging_schema = staging_schema
        os.makedirs(self.data_root, exist_ok=True)

    def latest_loaded_session(self) -> int:
        query = f'''
            SELECT MAX(legislature)
            FROM {self.raw_schema}.bill_text
        '''
        with duckdb.connect(self.db_path) as conn:
            try:
                start_session = conn.sql(query).fetchone()[0]
            except Exception:
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

    def get_uncleaned_documents(self, session):
        """Get raw documents that have not yet been cleaned into staging."""
        if self.table_exists(self.staging_schema, 'stg_testimony_cleaned_text'):
            query = f'''
                SELECT t.doc_id, t.doc_text
                FROM {self.raw_schema}.testimony_full_text t
                LEFT JOIN {self.staging_schema}.stg_testimony_cleaned_text s
                    ON t.doc_id = s.doc_id
                WHERE s.doc_id IS NULL
                AND t.doc_text IS NOT NULL
                AND t.doc_text != 'null'
                AND LENGTH(t.doc_text) > 10
                AND t.session = '{session}'
                ORDER BY t.doc_id
            '''
        else:
            query = f'''
                SELECT doc_id, doc_text
                FROM {self.raw_schema}.testimony_full_text
                WHERE doc_text IS NOT NULL
                AND doc_text != 'null'
                AND LENGTH(doc_text) > 10
                AND session = '{session}'
                ORDER BY doc_id
            '''

        with duckdb.connect(self.db_path) as conn:
            return conn.execute(query).df().to_dict('records')

    def get_unprocessed_documents(self, session):
        """Get staged (cleaned) documents that have not yet been embedded."""
        intermediate_schema = self.staging_schema.replace('staging', 'intermediate')

        if self.table_exists(intermediate_schema, 'int_sentence_embeddings'):
            query = f'''
                SELECT t.doc_id, t.cleaned_text
                FROM {self.staging_schema}.stg_testimony_cleaned_text t
                LEFT JOIN {intermediate_schema}.int_sentence_embeddings s
                    ON t.doc_id = s.doc_id
                WHERE s.doc_id IS NULL
                AND t.cleaned_text IS NOT NULL
                AND LENGTH(t.cleaned_text) > 10
                AND t.session = '{session}'
                ORDER BY t.doc_id
            '''
        else:
            query = f'''
                SELECT doc_id, cleaned_text
                FROM {self.staging_schema}.stg_testimony_cleaned_text
                WHERE cleaned_text IS NOT NULL
                AND LENGTH(cleaned_text) > 10
                AND session = '{session}'
                ORDER BY doc_id
            '''

        with duckdb.connect(self.db_path) as conn:
            return conn.execute(query).df().to_dict('records')
