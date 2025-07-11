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
from pypdf import PdfReader
from streamlit import table
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

import db_access as dba

DB_NAME = 'maine-legislative-testimony'
BRONZE_SCHEMA = 'bronze'
SILVER_SCHEMA = 'silver'
GOLD_SCHEMA = 'gold'
db = dba.Database(DB_NAME, BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA)

def current_session():
    '''
    Returns: Latest legislative session
    '''
    base_url = 'https://legislature.maine.gov/backend/breeze/data/getCurrentLegislature'
    client = RESTClient(
        base_url=base_url
    )
    resp = client.get('/')
    session = dlt_json.loads(resp.text)
    return session[0]

@dlt.source
def session_data(session):
    pdf_repo = Path(__file__).parents[1] / 'testimony_pdfs' / str(session)
    os.makedirs(pdf_repo, exist_ok=True)

    @dlt.resource(
        primary_key=['ldNumber', 'legislature', 'itemNumber'],
        parallelized=True
    )
    def bill_text():
        client = RESTClient(
            base_url='https://legislature.maine.gov/mrs-search/api/billtext',
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

        client = RESTClient(
            base_url='https://legislature.maine.gov/backend/breeze/data/CommitteeTestimony'
        )

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

        client = RESTClient(
            base_url='https://legislature.maine.gov/backend/app/services/getDocument.aspx'
        )
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
            pdf = PdfReader(filepath, strict=False)
            raw_text = ' '.join([page.extract_text() for page in pdf.pages]).strip() or None
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

    bill_text = bill_text.add_limit(1)
    testimony_attributes = bill_text | testimony_attributes
    testimony_pdfs = testimony_attributes | testimony_pdfs
    testimony_full_text = testimony_pdfs | testimony_full_text

    return bill_text, testimony_attributes, testimony_pdfs, testimony_full_text

@dlt.source
def text_vectorization():

    def load_spacy_model(model_name='en_core_web_sm'):
        try:
            nlp = spacy.load(model_name)
            return nlp
        except OSError:
            print(f'Model {model_name} not found. Downloading...')
            subprocess.check_call([
                sys.executable, '-m', 'spacy', 'download', model_name
            ])
            try:
                nlp = spacy.load(model_name)
                return nlp
            except Exception as e:
                print(f'Failed to load {model_name} after download: {e}')
                raise

    @dlt.resource
    def doc_text():
        for doc in db.stream_unprocessed_documents(BRONZE_SCHEMA, SILVER_SCHEMA):
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

            doc['doc_text'] = clean_text(doc['doc_text'])

            yield doc

    @dlt.transformer
    def doc_sentence(doc):

        nlp = load_spacy_model()

        if not nlp.has_pipe('sentencizer'):
            nlp.add_pipe('sentencizer')

        # Extract document data
        doc_id = doc.get('doc_id')
        doc_text = doc.get('doc_text', '')
        
        # Process the single document
        nlp.max_length = len(doc_text)
        
        sentence_count = 0
        with nlp.select_pipes(enable=['sentencizer']):
            processed_doc = nlp(doc_text)
            
            # Generate sentences with proper indexing
            for idx, sent in enumerate(processed_doc.sents):
                sent_text = sent.text.strip()
                if len(sent_text) > 10:  # Only yield sentences with meaningful length
                    yield {
                        'doc_id': doc_id,
                        'sentence': sent_text,
                        'sentence_index': idx
                    }

    @dlt.transformer(
        max_table_nesting=0,
        primary_key=['doc_id', 'sentence_index'],
        columns={
            "embedding": {"data_type": "json"}
        }
    )
    def document_sentence_vector(sentence):
        
        model_name = 'all-MiniLM-L12-v2'
        
        if not hasattr(document_sentence_vector, 'model'):
            document_sentence_vector.model = SentenceTransformer(model_name)
        
        model = document_sentence_vector.model
        
        # Extract sentence text
        sentence_text = sentence.get('sentence', '')
        doc_id = sentence.get('doc_id')
        sentence_index = sentence.get('sentence_index')
        
        if not sentence_text or len(sentence_text.strip()) == 0:
            return None
        
        try:
            # Encode the sentence to get embeddings
            embedding = model.encode([sentence_text], show_progress_bar=False)[0]
            
            # Return the sentence with its embedding
            return {
                'doc_id': doc_id,
                'sentence': sentence_text,
                'sentence_index': sentence_index,
                'embedding': embedding.tolist(),
                'model_name': model_name,
                'embedding_dimension': len(embedding)
            }
        except Exception as e:
            print(f'Error encoding sentence for doc_id {doc_id}: {e}')
            return None

    return (doc_text,
            doc_text | doc_sentence | document_sentence_vector)

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

    last_session = db.latest_loaded_session()
    end_session = current_session()
    sessions = range(last_session, end_session + 1)

    print(f'Bronze load — sessions {last_session}-{end_session}')

    for session in sessions:
        print(f'Processing session data for {session}')

        bronze_load_info = pipeline.run(
            session_data(session),
            write_disposition='merge'
        )
        print(bronze_load_info)

    pipeline.dataset_name = SILVER_SCHEMA

    silver_load_info = pipeline.run(
        text_vectorization(),
        write_disposition='merge'
    )

    print(silver_load_info)

if __name__ == '__main__':
    main(dev_mode=False)