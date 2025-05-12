import json
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
import dlt
import endpoints as ep
import db_access as dba

db_name = 'maine-legislative-testimony'
db = dba.Database(db_name)

pipeline = dlt.pipeline(
    pipeline_name='maine_legislation_pipeline',
    destination=dlt.destinations.duckdb(db.db_path),
    dataset_name='legislation'
)

def current_session():
    base_url = ep.current_session['base_url']
    client = RESTClient(
        base_url=base_url
    )
    resp = client.get('/')
    session = json.loads(resp.text)
    return session[0]

@dlt.resource(primary_key=['ld_number', 'legislature', 'item_number'])
def bill_text():
    client = RESTClient(
        base_url='https://legislature.maine.gov/mrs-search/api/billtext',
        paginator=OffsetPaginator(
            limit=200,
            limit_param='pageSize',
            total_path='hits.total.value'
        )
    )

    # Get the last processed session from the database
    last_session = db.latest_loaded_session() or 122
    print(last_session)
    end_session = current_session()
    
    # Only process sessions from the last processed session + 1 to current
    sessions = range(last_session, end_session + 1)
    print(f"Processing sessions {last_session} through {end_session}")

    for session in sessions:
        params = dict(ep.bill_text_config['params'])
        params['legislature'] = session

        print(f"Fetching bills for session {session}")

        for page in client.paginate(method="GET", params=params, data_selector='hits.hits[*]._source'):
            yield page

@dlt.resource(primary_key='Id', parallelized=True,  max_table_nesting=1)
def testimony_attributes():
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
                   'Inactive,TestimonySubmissionId,HearingDate,LDNumber,Request,CommitteeTestimonyDocumentContents'
    }

    client = RESTClient(
        base_url='https://legislature.maine.gov/backend/breeze/data/CommitteeTestimony'
    )

    bill_df = db.get_testimony_metadata_inputs()

    for index, row in bill_df.iterrows():
        params = base_params.copy()
        paper_number = row['paper_number']
        legislature = row['legislature']
        params['$filter'] = params['$filter'].format(paper_number=paper_number,
                                                     legislature=legislature)

        resp = client.get(path='/', params=params)

        try:
            content = json.loads(resp.content)
            yield content
        except json.JSONDecodeError as e:
            print(f'Error decoding JSON for {paper_number}, Legislature {legislature}: {e}')

@dlt.source
def legislative_docs():
    yield bill_text()

@dlt.source
def testimony():
    yield testimony_attributes()

def main():
    print(f"Starting pipeline run...")
    # Run pipeline using the generator
    load_info = pipeline.run(
        testimony(),
        write_disposition='merge',
        refresh='drop_sources'
    )
    print(load_info)

if __name__ == '__main__':
    main()