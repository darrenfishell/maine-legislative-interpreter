import logging
import dlt
import db_access as dba
from ..config import Config
from ..dlt_sources import session_data, text_vectorization, current_session


def get_bill_range(db: dba.Database):
    last_session = db.latest_loaded_session()
    end_session = current_session()
    return range(last_session, end_session + 1)


def main(dev_mode: bool = False):
    logging.getLogger('pypdf').setLevel(logging.ERROR)

    db = dba.Database(Config.DB_NAME, Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA)

    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour='blue'),
        dataset_name=Config.BRONZE_SCHEMA,
        dev_mode=dev_mode,
    )

    bill_range = get_bill_range(db)
    print(f'Bronze load — sessions {min(bill_range)}-{max(bill_range)}')

    for session in bill_range:
        bronze_load_info = pipeline.run(session_data(session), write_disposition='merge')
        print(bronze_load_info)

    pipeline.dataset_name = Config.SILVER_SCHEMA

    for session in range(126, max(bill_range)):
        silver_load_info = pipeline.run(text_vectorization(session), write_disposition='merge')
        print(silver_load_info)


if __name__ == '__main__':
    main(dev_mode=False)


