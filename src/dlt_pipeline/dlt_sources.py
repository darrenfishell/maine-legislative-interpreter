import os
import re
import json
import spacy
import pandas as pd
import dlt
import subprocess
import sys
import unicodedata

from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
from pathlib import Path
from pypdf import PdfReader
from streamlit import table
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# Optional: Add these for enhanced text cleaning
# import nltk
# from nltk.corpus import stopwords
# from nltk.tokenize import word_tokenize
# import ftfy  # For fixing text encoding issues

import db_access as dba
import duckdb

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
        primary_key=['ld_number', 'legislature', 'item_number']
    )
    def bill_text():
        client = RESTClient(
            base_url='https://legislature.maine.gov/mrs-search/api/billtext',
            paginator=OffsetPaginator(
                limit=200,
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
            'offset': 0,
            'pageSize': 100,
            'sortByScore': 'false',
            'showBillText': 'false',
            'sortAscending': 'false',
            'excludeOrders': 'false'
        }

        for bill_source in client.paginate(method="GET", params=params, data_selector='hits.hits[*]._source'):
            for bill in bill_source:
                yield bill

    @dlt.transformer(
        data_from=bill_text,
        primary_key='Id',
        max_table_nesting=0,
        parallelized=True  
    )
    def testimony_attributes(bill):
        if session < 126:  # Testimony data only available starting with 126th Legislature
            pass
            
        base_params = {
            '$filter': (
                "(((Request/PaperNumber eq '{paper_number}') and "
                "(Request/Legislature eq {legislature})) and "
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

        paper_number = bill.get('paperNumber')
        params = base_params.copy()
        params['$filter'] = params['$filter'].format(paper_number=paper_number, legislature=session)

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
        data_from=testimony_attributes,
        primary_key='doc_id',
        parallelized=True  # Enable parallelization for PDF downloads
    )
    def testimony_pdfs(testimony):
        if not testimony:  # Skip if no testimony data
            pass
            
        client = RESTClient(
            base_url='https://legislature.maine.gov/backend/app/services/getDocument.aspx'
        )

        doc_id = testimony.get('Id')
        params = {'doctype': 'test', 'documentId': doc_id}
        resp = client.get(path='/', params=params)

        filepath = pdf_repo / f'{doc_id}.pdf'

        with open(filepath, 'wb') as f:
            f.write(resp.content)

        yield {
            'doc_id': doc_id,
            'session': session,
            'pdf_filepath': str(filepath)
        }

    @dlt.transformer(
        data_from=testimony_pdfs,
        primary_key='doc_id',
        parallelized=True  # Enable parallelization for text extraction
    )
    def testimony_full_text(pdf_data):
        if not pdf_data:  # Skip if no PDF data
            pass
            
        filepath = pdf_data.get('pdf_filepath')

        try:
            with open(filepath, 'rb') as f:
                pdf = PdfReader(f, strict=False)
                raw_text = '\n'.join([page.extract_text() for page in pdf.pages])
                json_text = json.dumps(raw_text)
            yield {
                'doc_id': pdf_data.get('doc_id'),
                'session': session,
                'doc_text': json_text
            }
        except Exception as e:
            print(f'Error processing {pdf_data.get("pdf_filepath")}: {e}')
            pass

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
    def unprocessed_sentences(limit=None):
        # Use the Database class streaming method with optional limit
        count = 0
        for doc in db.stream_unprocessed_documents(BRONZE_SCHEMA, SILVER_SCHEMA):
            yield doc
            count += 1
            if limit and count >= limit:
                break

    @dlt.transformer
    def document_sentence(doc, limit=None):
        def clean_text(text):
            """
            Comprehensive text cleaning function to handle PDF extraction artifacts,
            Unicode issues, and common text problems.
            """
            import re
            import unicodedata
            
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
                '\u2014': '--', # Em dash
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
            
            # Remove sentences that are too short or contain mostly artifacts
            if len(text) < 10:  # Too short to be meaningful
                return ""
            
            # Check if text contains mostly readable content
            readable_chars = len(re.findall(r'[a-zA-Z]', text))
            total_chars = len(text.replace(' ', ''))
            if total_chars > 0 and readable_chars / total_chars < 0.3:  # Less than 30% readable
                return ""
            
            return text

        nlp = load_spacy_model()

        if not nlp.has_pipe('sentencizer'):
            nlp.add_pipe('sentencizer')

        # Extract document data
        doc_id = doc.get('doc_id')
        doc_text = doc.get('doc_text', '')
        
        if not doc_text or len(doc_text.strip()) == 0:
            pass
        
        # Clean the text
        cleaned_text = clean_text(doc_text)
        
        # Skip if cleaning resulted in empty text
        if not cleaned_text or len(cleaned_text.strip()) == 0:
            pass
        
        # Process the single document
        nlp.max_length = len(cleaned_text)
        
        sentence_count = 0
        with nlp.select_pipes(enable=['sentencizer']):
            processed_doc = nlp(cleaned_text)
            
            # Generate sentences with proper indexing
            for idx, sent in enumerate(processed_doc.sents):
                sent_text = sent.text.strip()
                if len(sent_text) > 10:  # Only yield sentences with meaningful length
                    yield {
                        'doc_id': doc_id,
                        'sentence': sent_text,
                        'sentence_index': idx
                    }
                    sentence_count += 1
                    if limit and sentence_count >= limit:
                        break

    @dlt.transformer(
        max_table_nesting=0,
        primary_key=['doc_id', 'sentence_index'],
        columns={
            "embedding": {"data_type": "json"}
        }
    )
    def document_sentence_vector(sentence, limit=None):
        
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

    return unprocessed_sentences.add_limit(10) | document_sentence | document_sentence_vector

def main(dev_mode=False, test_mode=False):
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

    # print(f'Bronze load — sessions {last_session}-{end_session}')
    #
    # for session in sessions:
    #     print(f'Processing session data for {session}')
    #
    #     bronze_load_info = pipeline.run(
    #         session_data(session),
    #         write_disposition='merge'
    #     )
    #     print(bronze_load_info)

    pipeline.dataset_name = SILVER_SCHEMA

    silver_load_info = pipeline.run(
        text_vectorization(),
        write_disposition='replace'
    )

    print(silver_load_info)

if __name__ == '__main__':
    main(dev_mode=False, test_mode=True)  # Set test_mode=False for production