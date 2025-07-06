from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
import dlt
import os
import json

from pathlib import Path
from pypdf import PdfReader

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

DB_NAME = 'maine-legislative-testimony'
SCHEMA = 'bronze'
db = dba.Database(DB_NAME, SCHEMA)

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

    print(f'Processing sessions {last_session} through {end_session}')

    for session in sessions:
        print(f'Processing session data for {session}')

        load_info = pipeline.run(
            session_data(session),
            write_disposition='merge'
        )
        print(load_info)

if __name__ == '__main__':
    main(dev_mode=False)