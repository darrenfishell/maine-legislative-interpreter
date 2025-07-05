from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
import dlt
import db_access as dba

db_name = 'maine-legislative-testimony'
db = dba.Database(db_name)

pipeline = dlt.pipeline(
    pipeline_name='maine_legislation_pipeline',
    destination=dlt.destinations.duckdb(db.db_path),
    dataset_name='legislation',
    progress=dlt.progress.tqdm(colour="yellow")
)

def current_session():
    base_url = 'https://legislature.maine.gov/backend/breeze/data/getCurrentLegislature'
    client = RESTClient(
        base_url=base_url
    )
    resp = client.get('/')
    session = dlt_json.loads(resp.text)
    return session[0]

@dlt.resource(
    primary_key=['ld_number', 'legislature', 'item_number']
)
def bill_text(session):
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
    parallelized=True
)
def testimony_attributes(session):

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

    # Get the bill data
    bill_df = db.get_testimony_metadata_inputs(session)

    # Create and yield deferred tasks for each bill
    for _, row in bill_df.iterrows():
        params = base_params.copy()
        paper_number = row['paper_number']
        params['$filter'] = params['$filter'].format(paper_number=paper_number, legislature=session)

        resp = client.get(path='/', params=params)

        try:
            content = dlt_json.loads(resp.text)
            yield content
        except Exception as e:
            print(f'Error decoding JSON for {paper_number}, Legislature {session}: {e}')
            print(f'Error JSON: {resp.text}')
            yield None

# @dlt.resource(
#     primary_key='Id',
#     parallelized=True
# )
# def testimony_texts(session):
#
#     base_params = {
#         'doctype': 'test',
#         'documentId': None
#     }
#
#     client = RESTClient(
#         base_url='https://legislature.maine.gov/backend/app/services/getDocument.aspx'
#     )

def main(test=False):

    last_session = db.latest_loaded_session()
    end_session = current_session()
    sessions = range(last_session, end_session + 1)
    if test:
        sessions = range(end_session, end_session + 1)

    print(f"Processing sessions {last_session} through {end_session}")

    for session in sessions:
        print(f"Processing bills for {session}")
        load_info = pipeline.run(
            bill_text(session),
            write_disposition='merge'
        )
        print(load_info)

        print(f"Processing testimonies for {session}")
        load_info = pipeline.run(
            testimony_attributes(session),
            write_disposition='replace'
        )
        print(load_info)

if __name__ == '__main__':
    main(test=True)