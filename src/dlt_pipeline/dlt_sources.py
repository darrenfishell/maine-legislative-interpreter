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

@dlt.source
def legislative_docs():
    yield bill_text()

def main():
    print(f"Starting pipeline run...")
    # Run pipeline using the generator
    load_info = pipeline.run(
        legislative_docs(),
        write_disposition='merge'
    )
    print(load_info)

if __name__ == '__main__':
    main()