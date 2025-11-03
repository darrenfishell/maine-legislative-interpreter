import logging
import argparse
from typing import Optional
import dlt
from .. import db_access as dba
from ..config import Config
from ..dlt_sources import session_data, text_vectorization, current_session


def get_bill_range(db: dba.Database):
    last_session = db.latest_loaded_session()
    end_session = current_session()
    return range(last_session, end_session + 1)


def run(stage: str = 'both', session: Optional[int] = None, dev_mode: bool = False):
    logging.getLogger('pypdf').setLevel(logging.ERROR)

    db = dba.Database(Config.DB_NAME, Config.BRONZE_SCHEMA, Config.SILVER_SCHEMA)

    # Determine session(s)
    if session is None:
        bill_range = get_bill_range(db)
    else:
        bill_range = range(session, session + 1)

    if stage in ('bronze', 'both'):
        pipeline = dlt.pipeline(
            pipeline_name='me_legislation',
            destination=dlt.destinations.duckdb(db.db_path),
            progress=dlt.progress.tqdm(colour='blue'),
            dataset_name=Config.BRONZE_SCHEMA,
            dev_mode=dev_mode,
        )

        print(f'Bronze load — sessions {min(bill_range)}-{max(bill_range)}')
        for s in bill_range:
            bronze_load_info = pipeline.run(session_data(s), write_disposition='merge')
            print(bronze_load_info)

    if stage in ('silver', 'both'):
        pipeline = dlt.pipeline(
            pipeline_name='me_legislation',
            destination=dlt.destinations.duckdb(db.db_path),
            progress=dlt.progress.tqdm(colour='blue'),
            dataset_name=Config.SILVER_SCHEMA,
            dev_mode=dev_mode,
        )

        # Silver only available from 126 onward
        start_session = max(126, min(bill_range))
        end_session = max(bill_range)
        print(f'Silver load — sessions {start_session}-{end_session}')
        for s in range(start_session, end_session + 1):
            silver_load_info = pipeline.run(text_vectorization(s), write_disposition='merge')
            print(silver_load_info)


def main(dev_mode: bool = False):
    # Backward-compatible entrypoint (used by dlt_sources.main)
    run(stage='both', session=None, dev_mode=dev_mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run me_legislation pipeline')
    parser.add_argument('--stage', choices=['bronze', 'silver', 'both'], default='both', help='Which stage(s) to run')
    parser.add_argument('--session', type=int, default=None, help='Single session to run (default: full range)')
    parser.add_argument('--dev-mode', action='store_true', help='Enable dev_mode for dlt pipeline')
    args = parser.parse_args()
    run(stage=args.stage, session=args.session, dev_mode=args.dev_mode)


