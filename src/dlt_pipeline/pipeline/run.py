import logging
import argparse
import subprocess
from pathlib import Path
from typing import Optional
import dlt
from .. import db_access as dba
from ..config import Config
from ..dlt_sources import session_data, text_cleaning, text_vectorization, current_session


STAGES = ('raw', 'staging', 'intermediate', 'dbt', 'all')

DBT_PROJECT_DIR = Path(__file__).resolve().parents[3] / 'dbt'


def get_bill_range(db: dba.Database):
    last_session = db.latest_loaded_session()
    end_session = current_session()
    return range(last_session, end_session + 1)


def run_raw(db: dba.Database, bill_range: range, dev_mode: bool):
    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour='blue'),
        dataset_name=Config.RAW_SCHEMA,
        dev_mode=dev_mode,
    )
    print(f'Raw load -- sessions {min(bill_range)}-{max(bill_range)}')
    for s in bill_range:
        load_info = pipeline.run(session_data(s), write_disposition='merge')
        print(load_info)


def run_staging(db: dba.Database, bill_range: range, dev_mode: bool):
    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour='green'),
        dataset_name=Config.STAGING_SCHEMA,
        dev_mode=dev_mode,
    )
    end_session = max(bill_range)
    print(f'Staging (text cleaning) -- sessions 126-{end_session}')
    for s in range(126, end_session + 1):
        load_info = pipeline.run(text_cleaning(s), write_disposition='merge')
        print(load_info)


def run_intermediate(db: dba.Database, bill_range: range, dev_mode: bool):
    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour='yellow'),
        dataset_name=Config.INTERMEDIATE_SCHEMA,
        dev_mode=dev_mode,
    )
    end_session = max(bill_range)
    print(f'Intermediate (vectorization) -- sessions 126-{end_session}')
    for s in range(126, end_session + 1):
        load_info = pipeline.run(text_vectorization(s), write_disposition='merge')
        print(load_info)


def run_dbt():
    """Run dbt models for staging SQL, intermediate SQL, and mart layers."""
    if not DBT_PROJECT_DIR.exists():
        print(f'dbt project not found at {DBT_PROJECT_DIR}, skipping')
        return

    print(f'Running dbt models from {DBT_PROJECT_DIR}')
    result = subprocess.run(
        ['dbt', 'run'],
        cwd=str(DBT_PROJECT_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f'dbt run failed:\n{result.stderr}')


def run(stage: str = 'all', session: Optional[int] = None, dev_mode: bool = False):
    logging.getLogger('pymupdf').setLevel(logging.ERROR)

    db = dba.Database(Config.DB_NAME, Config.RAW_SCHEMA, Config.STAGING_SCHEMA)

    if session is None:
        bill_range = get_bill_range(db)
    else:
        bill_range = range(session, session + 1)

    if stage in ('raw', 'all'):
        run_raw(db, bill_range, dev_mode)

    if stage in ('staging', 'all'):
        run_staging(db, bill_range, dev_mode)

    if stage in ('intermediate', 'all'):
        run_intermediate(db, bill_range, dev_mode)

    if stage in ('dbt', 'all'):
        run_dbt()


def main(dev_mode: bool = False):
    run(stage='all', session=None, dev_mode=dev_mode)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run me_legislation pipeline')
    parser.add_argument('--stage', choices=STAGES, default='all', help='Which stage(s) to run')
    parser.add_argument('--session', type=int, default=None, help='Single session to run (default: full range)')
    parser.add_argument('--dev-mode', action='store_true', help='Enable dev_mode for dlt pipeline')
    args = parser.parse_args()
    run(stage=args.stage, session=args.session, dev_mode=args.dev_mode)
