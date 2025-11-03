import os
import re
import json
import spacy
import dlt
import subprocess
import sys
import unicodedata
import time

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
from pathlib import Path
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

import db_access as dba
from .config import Config
from .constants import (
    BREEZE_CURRENT_LEGISLATURE_URL,
    BILL_TEXT_BASE_URL,
    TESTIMONY_BASE_URL,
    DOCUMENT_DOWNLOAD_URL,
)
from .utils.text import clean_text
from .utils.nlp import load_spacy_model
from .utils.pdf import read_pdf_text
from .services.storage import get_pdf_repo

DB_NAME = Config.DB_NAME
BRONZE_SCHEMA = Config.BRONZE_SCHEMA
SILVER_SCHEMA = Config.SILVER_SCHEMA
db = dba.Database(DB_NAME, BRONZE_SCHEMA, SILVER_SCHEMA)

def current_session():
    '''
    Returns: Latest legislative session
    '''
    base_url = BREEZE_CURRENT_LEGISLATURE_URL
    client = RESTClient(
        base_url=base_url
    )
    resp = client.get('/')
    session = dlt_json.loads(resp.text)
    return session[0]

@dlt.source
def session_data(session):
    pdf_repo = get_pdf_repo(session)

    print(f'Retrieving session bills and testimony for {session}')

    @dlt.resource(
        primary_key=['ldNumber', 'legislature', 'itemNumber'],
        parallelized=True
    )
    def bill_text():
        client = RESTClient(
            base_url=BILL_TEXT_BASE_URL,
            paginator=OffsetPaginator(
                limit=500,
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
            'sortByScore': 'false',
            'showBillText': 'false',
            'sortAscending': 'false',
            'excludeOrders': 'false'
        }

        for bill_page in client.paginate(method="GET", params=params, data_selector='hits.hits[*]._source'):
            for bill in bill_page:
                yield bill

    @dlt.transformer(
        primary_key='Id',
        max_table_nesting=0,
        parallelized=True
    )
    def testimony_attributes(bill):
        if session < 126:  # Testimony data only available starting with 126th Legislature
            pass

        # Track processed paper numbers to avoid duplicates
        if not hasattr(testimony_attributes, '_processed_papers'):
            testimony_attributes._processed_papers = set()

        paper_number = bill.get('paperNumber')

        # Skip if we've already processed this paper number
        if paper_number in testimony_attributes._processed_papers:
            return

        testimony_attributes._processed_papers.add(paper_number)

        base_params = {
            '$filter': (
                "(((Request/PaperNumber eq '{paper_number}') and "
                "(Request/Legislature eq {session})) and "
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

        client = RESTClient(base_url=TESTIMONY_BASE_URL)

        params = base_params.copy()
        params['$filter'] = params['$filter'].format(paper_number=paper_number, session=session)

        resp = client.get(path='/', params=params)

        try:
            content = dlt_json.loads(resp.text)
            for row in content:
                row['legislature'] = session
                yield row
        except Exception as e:
            print(f'Error decoding JSON for {paper_number}, Legislature {session}: {e}')
            print(f'Error JSON: {resp.text}')

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True  # Enable parallelization for PDF downloads
    )
    def testimony_pdfs(testimony):
        if not testimony:  # Skip if no testimony data
            pass

        doc_id = testimony.get('Id')
        filepath = pdf_repo / f'{doc_id}.pdf'

        if os.path.exists(filepath): # Skip request if file exists locally
            pass

        client = RESTClient(base_url=DOCUMENT_DOWNLOAD_URL)
        params = {'doctype': 'test', 'documentId': doc_id}
        resp = client.get(path='/', params=params)

        with open(filepath, 'wb') as f:
            f.write(resp.content)

        yield {
            'doc_id': doc_id,
            'session': session,
            'pdf_filepath': str(filepath)
        }

    @dlt.transformer(
        primary_key='doc_id',
        parallelized=True
    )
    def testimony_full_text(pdf_data):
        if not pdf_data:  # Skip if no PDF data
            pass

        filepath = pdf_data.get('pdf_filepath')

        if not os.path.exists(filepath):
            time.sleep(0.2)

        try:
            raw_text = read_pdf_text(filepath)
            json_text = json.dumps(raw_text)
            yield {
                'doc_id': pdf_data.get('doc_id'),
                'session': session,
                'doc_text': json_text
            }
        except Exception as e:
            print(f'Error processing {pdf_data.get("pdf_filepath")}: {e}')
            yield {
                'doc_id': pdf_data.get('doc_id'),
                'session': session,
                'doc_text': f"Error: {str(e)}"
            }

    testimony_attributes = bill_text | testimony_attributes
    testimony_pdfs = testimony_attributes | testimony_pdfs
    testimony_full_text = testimony_pdfs | testimony_full_text

    return bill_text, testimony_attributes, testimony_pdfs, testimony_full_text

@dlt.source
def text_vectorization(session):
    import torch

    print(f'Generating embeddings for {session} testimony')

    def load_spacy_model(model_name='en_core_web_sm'):
        """Load spaCy model optimized for sentence extraction only."""
        try:
            nlp = spacy.load(model_name, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner', 'parser'])
            
            if not nlp.has_pipe('sentencizer'):
                nlp.add_pipe('sentencizer')
            
            return nlp
        except OSError:
            print(f'Model {model_name} not found. Downloading...')
            subprocess.check_call([
                sys.executable, '-m', 'spacy', 'download', model_name
            ])
            try:
                nlp = spacy.load(model_name, disable=['tagger', 'attribute_ruler', 'lemmatizer', 'ner', 'parser'])
                
                if not nlp.has_pipe('sentencizer'):
                    nlp.add_pipe('sentencizer')
                
                return nlp
            except Exception as e:
                print(f'Failed to load {model_name} after download: {e}')
                raise

    def clean_text(text):
        """
        Comprehensive text cleaning function to handle PDF extraction artifacts,
        Unicode issues, and common text problems.
        """

        # Convert to string if it's not already
        if not isinstance(text, str):
            text = str(text)

        # Decode common Unicode escape sequences
        text = text.encode('utf-8', errors='ignore').decode('utf-8')

        # Handle common PDF extraction artifacts
        text = re.sub(r'u[0-9a-fA-F]{4}', '', text)  # Remove u0000, u2018, etc.
        text = re.sub(r'\\u[0-9a-fA-F]{4}', '', text)  # Remove escaped unicode
        text = re.sub(r'\\x[0-9a-fA-F]{2}', '', text)  # Remove hex escapes

        # Remove control characters and non-printable characters
        text = ''.join(char for char in text if unicodedata.category(char)[0] != 'C')

        # Normalize Unicode characters
        text = unicodedata.normalize('NFKC', text)

        # Replace common problematic characters
        replacements = {
            '\u2018': "'",  # Left single quotation mark
            '\u2019': "'",  # Right single quotation mark
            '\u201c': '"',  # Left double quotation mark
            '\u201d': '"',  # Right double quotation mark
            '\u2013': '-',  # En dash
            '\u2014': '--',  # Em dash
            '\u2022': '•',  # Bullet
            '\u00a0': ' ',  # Non-breaking space
            '\u00b0': '°',  # Degree sign
            '\u00ae': '®',  # Registered trademark
            '\u00a7': '§',  # Section sign
            '\u00bb': '»',  # Right-pointing double angle quotation mark
            '\u00ab': '«',  # Left-pointing double angle quotation mark
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        # Remove excessive whitespace and normalize line breaks
        text = re.sub(r'\n+|\\n|\r\n|\r', ' ', text)  # Replace all line breaks with space
        text = re.sub(r'\t+', ' ', text)  # Replace tabs with space
        text = re.sub(r'\s+', ' ', text)  # Replace multiple spaces with single space

        # Remove common PDF artifacts
        text = re.sub(r'[^\w\s.,!?;:()\'"\-–—•°®§»«]', '', text)  # Keep only readable characters

        # Clean up punctuation
        text = re.sub(r'\s+([.,!?;:])', r'\1', text)  # Remove space before punctuation
        text = re.sub(r'([.,!?;:])\s*([.,!?;:])', r'\1', text)  # Remove duplicate punctuation

        # Remove isolated characters and very short fragments
        text = re.sub(r'\b[a-zA-Z]\b', '', text)  # Remove single letters
        text = re.sub(r'\b\d+\b', '', text)  # Remove isolated numbers

        # Final cleanup
        text = text.strip()

        return text

    @dlt.resource(
        primary_key='doc_id',
        parallelized=True
    )
    def doc_text():
        unprocessed_docs = db.get_unprocessed_documents(BRONZE_SCHEMA, SILVER_SCHEMA, session)
        print(f'Processing {len(unprocessed_docs)} docs for {session}')

        for doc in unprocessed_docs:
            doc['doc_text'] = clean_text(doc['doc_text'])
            yield doc

    @dlt.transformer
    def doc_sentence(doc):
        if not hasattr(doc_sentence, 'nlp'):
            doc_sentence.nlp = load_spacy_model()
            if not doc_sentence.nlp.has_pipe('sentencizer'):
                doc_sentence.nlp.add_pipe('sentencizer')

        nlp = doc_sentence.nlp

        # Extract document data
        doc_id = doc.get('doc_id')
        doc_text = doc.get('doc_text', '')
        doc_length = len(doc_text)

        nlp.max_length = doc_length + 1000

        sentences = []
        with nlp.select_pipes(enable=['sentencizer']):
            processed_doc = nlp(doc_text)

            # Collect all sentences for this document
            for idx, sent in enumerate(processed_doc.sents):
                sent_text = sent.text.strip()
                if len(sent_text) > 10:
                    sentences.append({
                        'doc_id': doc_id,
                        'sentence': sent_text,
                        'sentence_index': idx
                    })

        # Return all sentences for this document as a batch
        return sentences

    def cleanup_gpu_resources():
        """Clean up GPU resources."""
        try:
            if torch.backends.mps.is_available():
                torch.mps.empty_cache()
        except Exception as e:
            print(f"GPU cleanup warning: {e}")

    @dlt.transformer(
        max_table_nesting=0,
        primary_key=['doc_id', 'sentence_index'],
        columns={
            "embedding": {"data_type": "json"}
        }
    )
    def document_sentence_vector(sentence_batch):
        model_name = 'all-MiniLM-L12-v2'

        # Load model once (single process)
        if not hasattr(document_sentence_vector, 'model'):
            # M3 Mac optimizations
            if torch.backends.mps.is_available():
                device = torch.device("mps")
                print(f"Using M3 GPU (MPS) for embeddings")
                batch_size = 128  # M3 can handle larger batches
            else:
                device = torch.device("cpu")
                print(f"MPS not available, using CPU")
                batch_size = 64

            document_sentence_vector.model = SentenceTransformer(model_name)
            document_sentence_vector.model.to(device)
            document_sentence_vector.device = device
            document_sentence_vector.batch_size = batch_size

        model = document_sentence_vector.model
        device = document_sentence_vector.device
        batch_size = document_sentence_vector.batch_size

        # Extract sentences and metadata
        sentences = []
        metadata = []

        for sentence_data in sentence_batch:
            sentence_text = sentence_data.get('sentence', '')
            if sentence_text and len(sentence_text.strip()) > 0:
                sentences.append(sentence_text)
                metadata.append(sentence_data)

        if not sentences:
            return []

        try:
            # Batch encode with M3-optimized settings
            embeddings = model.encode(
                sentences,
                show_progress_bar=False,
                device=device,
                batch_size=batch_size,
                convert_to_numpy=True,
                normalize_embeddings=False
            )

            # Return all sentences with their embeddings
            results = []
            for embedding, sentence_data in zip(embeddings, metadata):
                results.append({
                    'doc_id': sentence_data['doc_id'],
                    'sentence': sentence_data['sentence'],
                    'sentence_index': sentence_data['sentence_index'],
                    'embedding': embedding.tolist(),
                    'model_name': model_name,
                    'embedding_dimension': len(embedding)
                })

            return results

        except Exception as e:
            print(f'Error encoding sentences for doc_id {metadata[0]["doc_id"] if metadata else "unknown"}: {e}')
            return []

        finally:
            cleanup_gpu_resources()

    return (doc_text,
            doc_text | doc_sentence | document_sentence_vector)

def get_bill_range():
    last_session = db.latest_loaded_session()
    end_session = current_session()
    sessions = range(last_session, end_session + 1)
    return sessions

def main(dev_mode=False):
    import logging
    logging.getLogger("pypdf").setLevel(logging.ERROR)

    pipeline = dlt.pipeline(
        pipeline_name='me_legislation',
        destination=dlt.destinations.duckdb(db.db_path),
        progress=dlt.progress.tqdm(colour='blue'),
        dataset_name=BRONZE_SCHEMA,
        dev_mode=dev_mode
    )

    bill_range = get_bill_range()
    print(f'Bronze load — sessions {min(bill_range)}-{max(bill_range)}')

    for session in bill_range:
        bronze_load_info = pipeline.run(
            session_data(session),
            write_disposition='merge'
        )
        print(bronze_load_info)

    pipeline.dataset_name = SILVER_SCHEMA

    for session in range(126, max(bill_range)):
        silver_load_info = pipeline.run(
            text_vectorization(session),
            write_disposition='merge'
        )
        print(silver_load_info)

if __name__ == '__main__':
    main(dev_mode=False)