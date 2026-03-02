import json
import dlt
from tqdm import tqdm

from ..config import Config
from .. import db_access as dba
from ..utils.text import clean_text


@dlt.source
def text_cleaning(session: int):
    """Read raw testimony text, apply cleaning, and write to staging schema."""

    tqdm.write(f'Cleaning testimony text for session {session}')

    @dlt.resource(
        primary_key='doc_id',
        parallelized=True,
    )
    def stg_testimony_cleaned_text():
        db = dba.Database(Config.DB_NAME, Config.RAW_SCHEMA, Config.STAGING_SCHEMA)
        uncleaned = db.get_uncleaned_documents(session)
        tqdm.write(f'Cleaning {len(uncleaned)} documents for session {session}')

        for doc in uncleaned:
            raw_text = doc['doc_text']

            # Raw text was stored via json.dumps; decode it
            try:
                raw_text = json.loads(raw_text)
            except (json.JSONDecodeError, TypeError):
                pass

            if not raw_text or raw_text == 'null':
                continue

            cleaned = clean_text(raw_text)
            if cleaned and len(cleaned) > 10:
                yield {
                    'doc_id': doc['doc_id'],
                    'session': session,
                    'cleaned_text': cleaned,
                }

    return stg_testimony_cleaned_text
