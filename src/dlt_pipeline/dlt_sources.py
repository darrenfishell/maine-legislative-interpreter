from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
import dlt
from src.pipeline import endpoints as ep
import os
import json


def current_session():
    base_url = ep.current_session['base_url']
    client = RESTClient(
        base_url=base_url
    )
    resp = client.get('/')
    session = json.loads(resp.text)
    return session[0]

@dlt.resource(primary_key='_id')
def bill_texts():

    client = RESTClient(
        base_url='https://legislature.maine.gov/mrs-search/api/billtext',
        paginator=OffsetPaginator(
            limit=100,
            limit_param='pageSize',
            total_path='hits.total.value'
        )
    )

    sessions = range(122, current_session() + 1)

    for session in sessions:
        params = dict(ep.bill_text_config['params'])
        params['legislature'] = session

        print(f"Fetching bills for session {session}")

        for page in client.paginate(method="GET", params=params):
            yield page

db_path = os.path.abspath('../../data/maine-legislative-testimony.duckdb')

pipeline = dlt.pipeline(
    pipeline_name='maine_legislation_pipeline',
    destination=dlt.destinations.duckdb(db_path),
    dataset_name='stage'
)

@dlt.source
def legislative_docs():
    yield bill_texts()

def main():
    # Run pipeline using the generator
    load_info = pipeline.run(
        legislative_docs(),
        write_disposition='merge',
        refresh='drop_sources'
    )
    print(load_info)

if __name__ == '__main__':
    main()