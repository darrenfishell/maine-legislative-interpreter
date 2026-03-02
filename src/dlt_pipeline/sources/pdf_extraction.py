import json
import os
import threading
import pymupdf
import dlt
from tqdm import tqdm

from ..config import Config
from .. import db_access as dba
from ..services.storage import get_pdf_repo
from ..utils.pdf import read_pdf_text

_lock = threading.Lock()
_progress: dict = {}


def _update(session: int, *, error: bool = False, skipped: bool = False):
    with _lock:
        stats = _progress.get(session)
        if stats is None:
            return
        if error:
            stats['errors'] += 1
        if skipped:
            stats['skipped'] += 1
        stats['pbar'].update(1)


@dlt.source
def pdf_text_extraction(session: int):
    """Read PDFs from disk for testimony that lacks extracted text."""

    pdf_repo = get_pdf_repo(session)

    @dlt.resource(primary_key='doc_id')
    def _pdf_ids():
        db = dba.Database(Config.DB_NAME, Config.RAW_SCHEMA, Config.STAGING_SCHEMA)
        doc_ids = db.get_unextracted_pdfs(session)

        if not doc_ids:
            tqdm.write(f'Session {session}: no PDFs to extract')
            return

        with _lock:
            _progress[session] = {
                'total': len(doc_ids), 'errors': 0, 'skipped': 0,
                'pbar': tqdm(total=len(doc_ids), desc=f'Session {session}', unit='pdf', leave=False),
            }

        pymupdf.TOOLS.mupdf_display_errors(False)

        for doc_id in doc_ids:
            yield {'doc_id': doc_id}

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True,
    )
    def testimony_full_text(item):
        doc_id = item['doc_id']
        filepath = pdf_repo / f'{doc_id}.pdf'

        if not os.path.exists(filepath):
            _update(session, skipped=True)
            return

        try:
            raw_text = read_pdf_text(str(filepath))
            json_text = json.dumps(raw_text)
            yield {
                'doc_id': doc_id,
                'session': session,
                'doc_text': json_text,
            }
            _update(session)
        except Exception as e:
            if not Config.QUIET_ERRORS:
                tqdm.write(f'  Error doc_id={doc_id}: {e}')
            yield {
                'doc_id': doc_id,
                'session': session,
                'doc_text': f"Error: {str(e)}",
            }
            _update(session, error=True)

    return _pdf_ids | testimony_full_text
