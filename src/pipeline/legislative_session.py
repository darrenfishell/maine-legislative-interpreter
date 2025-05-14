from concurrent.futures import ProcessPoolExecutor, as_completed
from io import BytesIO
from urllib.parse import urlencode

import pandas as pd
import requests
import json
from pypdf import PdfReader

from src.dlt_pipeline import endpoints as lep
from src.dlt_pipeline import async_fetch as pb
from database import schemas

def get_current_session():
    return requests.get(lep.current_session.get('base_url')).json()[0]

class LegislativeSession:
    """
    Represents a legislative session for bill and testimony data retrieval.

    This class provides methods to retrieve bills, testimony metadata, and testimony texts
    for a specific legislative session.

    Attributes:
        session (int): The session number of the legislature.
        bills (list): A list that stores bills retrieved for the session.
        testimony_metadata (list): A list that stores testimony metadata.
        testimony_texts (list): A list that stores testimony texts.
        page_size (int): The number of items to retrieve per page (for pagination).
        retries (int): Number of retry attempts for failed requests.
        async_batch_size (int): Number of async calls made in a batch.

    Methods:
        retrieve_bills(): Retrieves bills for the legislative session.
        retrieve_testimony_metadata(): Retrieves testimony metadata for the session.
        retrieve_testimony_texts(): Retrieves testimony texts for the session.
    """
    bill_text_url = lep.bill_text_config.get('base_url')
    bill_text_params = lep.bill_text_config.get('params')
    testimony_meta_url = lep.testimony_metadata_config.get('base_url')
    testimony_meta_params = lep.testimony_metadata_config.get('params')
    testimony_docs_url = lep.testimony_text_config.get('base_url')
    testimony_docs_params = lep.testimony_text_config.get('params')

    def __init__(self, session, page_size=20, retries=5, async_batch_size=10):
        self.session = session
        self.bills = []
        self.testimony_metadata = []
        self.testimony_texts = []
        self.page_size = page_size
        self.retries = retries
        self.async_batch_size = async_batch_size

    async def retrieve_data(self):
        """
        Main method to retrieve all relevant data (bills, testimony metadata, and texts) for the legislative session.
        """
        await self.retrieve_bills()
        await self.retrieve_testimony_metadata()
        await self.retrieve_testimony_texts()
        return self

    async def retrieve_bills(self):
        """
        Retrieve and store bill texts for the session.
        """
        self.bills = await self._get_bill_texts()

    async def retrieve_testimony_metadata(self):
        """
        Retrieve and store testimony metadata based on the bills retrieved.
        """
        self.testimony_metadata = self._get_testimony_metadata()

    async def retrieve_testimony_texts(self):
        """
        Retrieve and store testimony texts (PDFs) from the testimony metadata.
        """
        self.testimony_texts = await self._get_testimony_texts()

    async def _get_bill_texts(self):
        """
        Internal method to retrieve bill texts for the legislative session.
        """
        params = self.bill_text_params.copy()
        params.update({
            'legislature': self.session,
            'pageSize': self.page_size,
            'offset': 0
        })

        url = self.bill_text_url
        query_url = f'{url}?{urlencode(params)}'

        r = requests.get(query_url)
        if r.status_code != 200:
            print(f'Error retrieving results for {self.session}, HTTP code {r.status_code}')
            return
        result_count = r.json()['hits']['total']['value']

        # Generate URLs for pagination
        session_url_list = [query_url]
        while params['offset'] < result_count:
            next_url = f'{url}?{urlencode(params)}'
            session_url_list.append(next_url)
            params['offset'] += self.page_size

        # Retrieve bill texts using async batches
        response = await pb.run_in_batches(self.async_batch_size, session_url_list)

        bill_texts = json.loads(response.content)

        bill_text_list = [hit.get('_source') for r in bill_texts for hit in r.get('hits').get('hits')]

        print(f'Retrieved {len(bill_text_list)} bills | Legislature: {self.session}')

        df = pd.json_normalize(bill_text_list)
        df = df[schemas.bill_text_schema]
        return df

    async def _get_testimony_metadata(self):
        """
        Internal method to retrieve testimony metadata for the session based on bill data.
        """
        params = self.testimony_meta_params.copy()
        params['$filter'] = params['$filter'].format(legislature=self.session, paper_number=paper_number)

        query_url = f'{self.testimony_meta_url}?{urlencode(params)}'
        print(f'Retrieving testimony for {query_url}')

        # Retrieve metadata using async batches
        testimony_metadata = requests.get(query_url)
        df = pd.json_normalize(testimony_metadata.content)
        df['legislative_session'] = self.session
        return df

    async def _get_testimony_texts(self):
        """
        Retrieve and store testimony texts (PDFs) from the testimony metadata using parallel processing.
        """
        doc_list = self.testimony_metadata['Id']

        # Parallelize the PDF extraction using ProcessPoolExecutor
        with ProcessPoolExecutor() as executor:
            future_to_doc = {executor.submit(self._extract_text_from_pdf, doc_id): doc_id for doc_id in doc_list}
            for future in as_completed(future_to_doc):
                doc_id = future_to_doc[future]
                try:
                    text = future.result
                    self.testimony_texts.append({'Id': doc_id, 'text': text})
                except Exception as exc:
                    print(f'Document {doc_id} generated an exception: {exc}')

    @staticmethod
    def _extract_text_from_pdf(doc_id):
        """
        Helper method to download and extract text from a PDF using its document ID.
        This method can be parallelized with ProcessPoolExecutor.
        """
        params = lep.testimony_text_config['params'].copy()
        url = lep.testimony_text_config['base_url']
        params['documentId'] = doc_id
        query_url = f'{url}?{urlencode(params)}'

        r = requests.get(query_url)
        r_bytes = BytesIO(r.content)

        try:
            pdf = PdfReader(r_bytes)
            doc_text = '\n'.join([page.extract_text() for page in pdf.pages])
        except Exception as e:
            print(f"Error reading PDF for document {doc_id}: {e}")
            doc_text = ''
        return doc_text