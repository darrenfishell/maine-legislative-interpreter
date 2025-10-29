import asyncio
import pandas as pd
import json

from pypdf import PdfReader

from src.dlt_pipeline import LegislativeSession, get_current_session
from src.dlt_pipeline.db_access import Database
from src.dlt_pipeline import async_fetch as fetch
from urllib.parse import urlencode

from src import dlt_pipeline as ep

db = Database('maine_legislation_and_testimony')

async def load_bills(truncate_and_reload=False):
    start_session, end_session = 122, get_current_session()
    sessions = range(start_session, end_session + 1)
    table_name = 'BILL_TEXT'

    if truncate_and_reload:
        db.truncate_tbl(table_name)

    for session in sessions:
        session = LegislativeSession(session)
        await session.retrieve_bills()
        if session.bills is not None:
            db.write_to_db(session.bills, table_name, method='append')

async def load_testimonies(truncate_and_reload=False):
    start_session, end_session = 126, get_current_session()
    sessions = range(start_session, end_session + 1)
    table_name = 'TESTIMONY_HEADER'
    if truncate_and_reload:
        db.truncate_tbl(table_name)
    url = ep.testimony_metadata_config.get('base_url')
    base_params = ep.testimony_metadata_config.get('params')
    session_collector = {}
    for session in sessions:
        session_doc_df = db.get_testimony_metadata_inputs(session)
        queries = []
        for index, row in session_doc_df.iterrows():
            params = base_params.copy()
            params['$filter'] = params['$filter'].format(paper_number=row['paperNumber'],
                    legislature=row['legislature'])
            query_url = f'{url}?{urlencode(params)}'
            queries.append(query_url)
        results = await fetch.run_in_batches(queries[0:5])
        collector = []
        for result in results:
            if result is not None:
                try:
                    json_parsed = json.loads(result)
                except json.JSONDecodeError as e:
                    print(f'JSON parsing error {e}')
                for record in json_parsed:
                    collector.append(record)
        df = pd.DataFrame(collector)
        df['legislature'] = session
        # Convert nested columns to text
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, (dict, object))).any():
                df[col] = df[col].astype(str)
        try:
            df = df.drop(columns=['CommitteeTestimonyDocumentContents', '$type'])
        except KeyError:
            pass
        db.write_to_db(df, table_name, method='append')

async def parse_and_load_texts(truncate_and_reload=True):
    start_session, end_session = 126, get_current_session()
    sessions = range(start_session, end_session + 1)
    table_name = 'TESTIMONY_DETAIL'
    if truncate_and_reload:
        db.truncate_tbl(table_name)
    url = ep.testimony_text_config['base_url']

    for session in sessions:
        print(f'Loading testimony for session {session}')
        doc_ids = db.get_testimony_doc_ids(session)

        queries = []
        for doc_id in doc_ids:
            params = ep.testimony_text_config['params'].copy()
            params['documentId'] = doc_id
            query_url = f'{url}?{urlencode(params)}'
            queries.append(query_url)

        pdf_byte_list = await fetch.run_in_batches(queries)

        page_texts = []
        for doc in pdf_byte_list:
            if doc is not None:
                doc_text = ''
                doc_detail = {
                    'query_url': doc.get('query_url'),
                    'doc_text': None
                }
                try:
                    reader = PdfReader(doc.get('bytes'))
                    for page in reader.pages:
                        doc_text += page.extract_text()
                    doc_detail['doc_text'] = doc_text
                    page_texts.append(doc_detail)
                except Exception as e:
                    print(f'Error processing {doc.get("query_url")}: {e}')

        df = pd.DataFrame(page_texts)
        df['doc_id'] = df['query_url'].str.extract(r'documentId=(\d+)')
        db.write_to_db(df, table_name, method='append')

if __name__ == '__main__':
    asyncio.run(load_bills())
    asyncio.run(load_testimonies())
    asyncio.run(parse_and_load_texts())