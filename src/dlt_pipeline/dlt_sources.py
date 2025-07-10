import os
import re
import json
import spacy
import pandas as pd
import dlt
import subprocess
import sys

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
from pathlib import Path
from pypdf import PdfReader
from streamlit import table

import db_access as dba

DB_NAME = 'maine-legislative-testimony'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'
db = dba.Database(DB_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA)

def current_session():
    '''
    Returns: Latest legislative session
    '''
    base_url = 'https://legislature.maine.gov/backend/breeze/data/getCurrentLegislature'
    client = RESTClient(
        base_url=base_url
    )
    resp = client.get('/')
    session = dlt_json.loads(resp.text)
    return session[0]

@dlt.source
def session_data(session):
    pdf_repo = Path(__file__).parents[1] / 'testimony_pdfs' / str(session)
    os.makedirs(pdf_repo, exist_ok=True)

    @dlt.resource(
        primary_key=['ld_number', 'legislature', 'item_number']
    )
    def bill_text():
        client = RESTClient(
            base_url='https://legislature.maine.gov/mrs-search/api/billtext',
            paginator=OffsetPaginator(
                limit=200,
                limit_param='pageSize',
                total_path='hits.total.value'
            )
        )

        params = {
            'term': '',
            'title': '',
            'legislature': session,
            'requestItemType': 'false',
            'lmSponsorPrimary': 'false',
            'reqAmendExists': 'false',
            'reqAmendAdoptH': 'false',
            'reqAmendAdoptS': 'false',
            'reqChapterExists': 'false',
            'reqFNRequired': 'false',
            'reqEmergency': 'false',
            'reqGovernor': 'false',
            'reqBond': 'false',
            'reqMandate': 'false',
            'reqPublicLand': 'false',
            'showExtraParameters': 'true',
            'mustHave': '',
            'mustNotHave': '',
            'offset': 0,
            'pageSize': 100,
            'sortByScore': 'false',
            'showBillText': 'false',
            'sortAscending': 'false',
            'excludeOrders': 'false'
        }

        for bill_source in client.paginate(method="GET", params=params, data_selector='hits.hits[*]._source'):
            for bill in bill_source:
                yield bill

    @dlt.transformer(
        primary_key='Id',
        max_table_nesting=0,
        parallelized=True
    )
    def testimony_attributes(bill):

        base_params = {
            '$filter': (
                "(((Request/PaperNumber eq '{paper_number}') and "
                "(Request/Legislature eq {legislature})) and "
                "(Inactive ne true)) and "
                "(not (startswith(LastName, '@') eq true))"
            ),
            '$orderby': 'LastName,FirstName,Organization',
            '$expand': 'Request',
            '$select': 'Id,SourceDocument,RequestId,FileType,FileSize,'
                       'NamePrefix,FirstName,LastName,NameSuffix,'
                       'Organization,PresentedDate,PolicyArea,Topic,Created,CreatedBy,LastEdited,LastEditedBy,Private,'
                       'Inactive,TestimonySubmissionId,HearingDate,LDNumber,Request'
        }

        client = RESTClient(
            base_url='https://legislature.maine.gov/backend/breeze/data/CommitteeTestimony'
        )

        if session >= 126:  # Testimony data only available starting with 126th Legislature
            params = base_params.copy()
            paper_number = bill.get('paperNumber')
            params['$filter'] = params['$filter'].format(paper_number=paper_number, legislature=session)

            resp = client.get(path='/', params=params)

            try:
                content = dlt_json.loads(resp.text)
                for row in content:
                    row['legislature'] = session
                    yield row
            except Exception as e:
                print(f'Error decoding JSON for {paper_number}, Legislature {session}: {e}')
                print(f'Error JSON: {resp.text}')
                yield None
        else:
            yield None

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True
    )
    def testimony_pdf(testimony):

        client = RESTClient(
            base_url='https://legislature.maine.gov/backend/app/services/getDocument.aspx'
        )

        doc_id = testimony.get('Id')
        params = {'doctype': 'test', 'documentId': doc_id}
        resp = client.get(path='/', params=params)

        filepath = pdf_repo / f'{doc_id}.pdf'

        with open(filepath, 'wb') as f:
            f.write(resp.content)

        yield {
            'doc_id': doc_id,
            'session': session,
            'pdf_filepath': str(filepath)
        }

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True
    )
    def testimony_full_text(pdf_data):

        filepath = pdf_data.get('pdf_filepath')

        try:
            with open(filepath, 'rb') as f:
                pdf = PdfReader(f, strict=False)
                raw_text = '\n'.join([page.extract_text() for page in pdf.pages])
                json_text = json.dumps(raw_text)
            yield {
                'doc_id': pdf_data.get('doc_id'),
                'session': session,
                'doc_text': json_text
            }
        except Exception as e:
            print(f'Error processing {pdf_data.get("pdf_filepath")}: {e}')

    load_testimony = bill_text | testimony_attributes
    load_pdfs = load_testimony | testimony_pdf
    parse_pdf_text = load_pdfs | testimony_full_text

    return bill_text, load_testimony, load_pdfs, parse_pdf_text

@dlt.source
def text_vectorization():

    def load_spacy_model(model_name='en_core_web_sm'):

        try:
            nlp = spacy.load(model_name)
            return nlp
        except OSError:
            print(f'Model {model_name} not found. Downloading...')

            subprocess.check_call([
                sys.executable, '-m', 'spacy', 'download', model_name
            ])

            try:
                nlp = spacy.load(model_name)
                return nlp
            except Exception as e:
                print(f'Failed to load {model_name} after download: {e}')
                raise

    @dlt.resource
    def unprocessed_sentences(batch_size=1000):

        if db.table_exists(schema=SILVER_SCHEMA, table_name='testimony_sentences'):
            base_query = f'''
                SELECT doc_id, doc_text
                FROM {BRONZE_SCHEMA}.testimony_full_text
                WHERE doc_id NOT IN (
                    SELECT DISTINCT doc_id 
                    FROM {SILVER_SCHEMA}.testimony_sentences
                )
                ORDER BY doc_id
            '''
        else:
            base_query = f'''
                SELECT doc_id, doc_text
                FROM {BRONZE_SCHEMA}.testimony_full_text
                ORDER BY doc_id
            '''

        offset = 0

        while True:
            query = f"{base_query} LIMIT {batch_size} OFFSET {offset}"

            docs_df = db.get_query_as_df(query)

            if docs_df.empty:
                break

            yield docs_df

            offset += batch_size

    @dlt.transformer
    def document_sentence():
        nlp = load_spacy_model()

        if not nlp.has_pipe('sentencizer'):
            nlp.add_pipe('sentencizer')

        def clean_text(text):
            text = re.sub(r'\n+|\\n|\t+', ' ', text)
            text = re.sub(r'\s+', ' ', text)
            text = re.sub(r'[^a-zA-Z0-9.,()!?\- ]', '', text)
            text = text.strip()
            return text

        def tokenize_sentences(docs):
            nlp.max_length = max(docs['doc_text'].apply(len))
            all_sentences = []
            zip_pipe = zip(docs['doc_id'], nlp.pipe(docs['doc_text']))
            with nlp.select_pipes(enable=['sentencizer']):
                for doc_id, processed_doc in zip_pipe:
                    sentences = [{
                        'doc_id': doc_id,
                        'sentence': sent.text.strip()
                    } for sent in processed_doc.sents if len(sent) > 1]
                    all_sentences.extend(sentences)

            return pd.DataFrame(all_sentences)

        for documents in db.get_unprocessed_document_batch():
            documents['doc_text'] = documents['doc_text'].apply(clean_text)
            yield tokenize_sentences(documents)

    return unprocessed_sentences | document_sentence

def main(dev_mode=False):
    import logging
    logging.getLogger("pypdf").setLevel(logging.ERROR)

    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour='blue'),
        dataset_name=SCHEMA,
        dev_mode=dev_mode
    )

    last_session = db.latest_loaded_session()
    end_session = current_session()
    sessions = range(last_session, end_session + 1)

    # print(f'Bronze load — sessions {last_session}-{end_session}')
    #
    # for session in sessions:
    #     print(f'Processing session data for {session}')
    #
    #     bronze_load_info = pipeline.run(
    #         session_data(session),
    #         write_disposition='merge'
    #     )
    #     print(bronze_load_info)

    pipeline.dataset_name = 'silver'

    silver_load_info = pipeline.run(
        text_vectorization().add_limit(1),
        write_disposition='replace'
    )

    print(silver_load_info)

if __name__ == '__main__':
    main(dev_mode=False)