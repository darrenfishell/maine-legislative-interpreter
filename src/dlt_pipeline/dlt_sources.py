from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
import dlt
import os

from pathlib import Path
from pypdf import PdfReader
from io import BytesIO

import db_access as dba

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
            'pageSize': 20,
            'sortByScore': 'false',
            'showBillText': 'false',
            'sortAscending': 'false',
            'excludeOrders': 'false'
        }

        for page in client.paginate(method="GET", params=params, data_selector='hits.hits[*]._source'):
            yield page

    @dlt.resource(
        primary_key='Id',
        max_table_nesting=0
    )
    @dlt.transformer(parallelized=True)
    def testimony_attributes(page_of_bills):

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

        for bill in page_of_bills:
            params = base_params.copy()
            paper_number = bill.get('paperNumber')
            params['$filter'] = params['$filter'].format(paper_number=paper_number, legislature=session)

            resp = client.get(path='/', params=params)

            try:
                content = dlt_json.loads(resp.text)
                for row in content:
                    row['legislature'] = session
                yield content
            except Exception as e:
                print(f'Error decoding JSON for {paper_number}, Legislature {session}: {e}')
                print(f'Error JSON: {resp.text}')
                yield None

    @dlt.resource(primary_key='doc_id')
    @dlt.transformer(parallelized=True)
    def get_pdfs(testimonies):

        client = RESTClient(
            base_url='https://legislature.maine.gov/backend/app/services/getDocument.aspx'
        )

        @dlt.defer
        def fetch_pdf(doc_id):
            params = {
                'doctype': 'test',
                'documentId': doc_id
            }
            resp = client.get(path='/', params=params)
            return {
                'doc_id': doc_id,
                'session': session,
                'content': resp.content
            }

        for testimony in testimonies:
            yield fetch_pdf(testimony.get('Id'))

    @dlt.resource(primary_key='doc_id')
    @dlt.transformer(parallelized=True)
    def testimony_pdf(pdf_data):

        filepath = pdf_repo / f"{pdf_data.get('doc_id')}.pdf"

        with open(filepath, 'wb') as f:
            f.write(pdf_data.get('content'))

        yield {
            'doc_id': pdf_data.get('doc_id'),
            'session': session,
            'pdf_filepath': str(filepath)
        }

    @dlt.resource(primary_key='doc_id')
    @dlt.transformer(parallelized=True)
    def testimony_full_text(pdf_data):

        with open(pdf_data.get('pdf_filepath'), 'rb') as f:
            pdf_bytes = BytesIO(f.read())

            try:
                reader = PdfReader(pdf_bytes)
                full_text = ""
                for page in reader.pages:
                    full_text += page.extract_text()
            except Exception as e:
                print(f'Error processing {pdf_data.get("pdf_filepath")}: {e}')

        yield {
            'doc_id': pdf_data.get('doc_id'),
            'session': session,
            'full_text': full_text
        }

    load_testimony = bill_text | testimony_attributes
    load_pdfs = load_testimony | get_pdfs | testimony_pdf
    parse_pdf_text = load_pdfs | testimony_full_text

    return bill_text, load_testimony, load_pdfs, parse_pdf_text

DB_NAME = 'maine-legislative-testimony'
SCHEMA = 'bronze'
db = dba.Database(DB_NAME, SCHEMA)

def main(test=False, reset=False):

    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour="yellow"),
        dataset_name=SCHEMA
    )

    last_session = db.latest_loaded_session()
    end_session = current_session()
    sessions = range(last_session, end_session + 1)
    if test:
        sessions = range(end_session, end_session + 1)

    print(f"Processing sessions {last_session} through {end_session}")

    for session in sessions:
        print(f"Processing session data for {session}")
        load_info = pipeline.run(
            session_data(session),
            write_disposition='merge'
        )
        print(load_info)

if __name__ == '__main__':
    main(test=True)