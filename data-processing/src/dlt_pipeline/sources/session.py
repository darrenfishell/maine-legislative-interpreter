import os
import json
import time
import dlt
from dlt.common import json as dlt_json
from tqdm import tqdm

from ..services.api import iterate_bill_text, get_testimony_attributes, download_document
from ..services.storage import get_pdf_repo
from ..config import Config


@dlt.source
def session_data(session: int):
    pdf_repo = get_pdf_repo(session)

    tqdm.write(f'Retrieving session bills and testimony for {session}')

    @dlt.resource(
        primary_key=['ldNumber', 'legislature', 'itemNumber'],
        parallelized=True,
    )
    def bill_text():
        for bill in iterate_bill_text(session):
            yield bill

    @dlt.transformer(
        primary_key='Id',
        max_table_nesting=0,
        parallelized=True,
    )
    def testimony_attributes(bill):
        if session < 126:
            return

        if not hasattr(testimony_attributes, '_processed_papers'):
            testimony_attributes._processed_papers = set()

        paper_number = bill.get('paperNumber')
        if paper_number in testimony_attributes._processed_papers:
            return

        testimony_attributes._processed_papers.add(paper_number)

        try:
            for row in get_testimony_attributes(paper_number, session):
                yield row
        except Exception as e:
            tqdm.write(f'Error fetching testimony for {paper_number}, Legislature {session}: {e}')

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True,
    )
    def testimony_pdfs(testimony):
        if not testimony:
            return

        doc_id = testimony.get('Id')
        filepath = pdf_repo / f'{doc_id}.pdf'

        if os.path.exists(filepath):
            return

        try:
            content = download_document(doc_id)
            tmp_path = filepath.with_suffix(filepath.suffix + '.tmp')
            with open(tmp_path, 'wb') as f:
                f.write(content)
            # Atomic replace ensures we never leave a partially written target
            os.replace(tmp_path, filepath)
        except Exception as e:
            if not Config.QUIET_ERRORS:
                tqdm.write(f'download failed for doc_id={doc_id}: {e}')
            return

        yield {
            'doc_id': doc_id,
            'session': session,
            'pdf_filepath': str(filepath),
        }

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True,
    )
    def testimony_full_text(pdf_data):
        if not pdf_data:
            return

        filepath = pdf_data.get('pdf_filepath')
        if not os.path.exists(filepath):
            time.sleep(0.2)

        try:
            # Preserve prior behavior: store JSON-encoded text
            from ..utils.pdf import read_pdf_text
            raw_text = read_pdf_text(filepath)
            json_text = json.dumps(raw_text)
            yield {
                'doc_id': pdf_data.get('doc_id'),
                'session': session,
                'doc_text': json_text,
            }
        except Exception as e:
            if not Config.QUIET_ERRORS:
                tqdm.write(f'Error processing {pdf_data.get("pdf_filepath")}: {e}')
            yield {
                'doc_id': pdf_data.get('doc_id'),
                'session': session,
                'doc_text': f"Error: {str(e)}",
            }

    testimony_attributes_res = bill_text | testimony_attributes
    testimony_pdfs_res = testimony_attributes_res | testimony_pdfs
    testimony_full_text_res = testimony_pdfs_res | testimony_full_text

    return bill_text, testimony_attributes_res, testimony_pdfs_res, testimony_full_text_res


