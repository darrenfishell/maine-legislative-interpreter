from typing import Dict, Iterable, List
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dlt.common import json as dlt_json
from ..constants import (
    BREEZE_CURRENT_LEGISLATURE_URL,
    BILL_TEXT_BASE_URL,
    TESTIMONY_BASE_URL,
    DOCUMENT_DOWNLOAD_URL,
)
from ..config import Config
from ..utils.concurrency import retry
from tqdm import tqdm


def get_current_legislature() -> int:
    client = RESTClient(base_url=BREEZE_CURRENT_LEGISLATURE_URL)
    resp = client.get('/')
    session = dlt_json.loads(resp.text)
    return session[0]


def iterate_bill_text(session: int) -> Iterable[Dict]:
    client = RESTClient(
        base_url=BILL_TEXT_BASE_URL,
        paginator=OffsetPaginator(limit=500, limit_param='pageSize', total_path='hits.total.value'),
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
        'excludeOrders': 'false',
    }
    for bill_page in client.paginate(method='GET', params=params, data_selector='hits.hits[*]._source'):
        for bill in bill_page:
            yield bill


def get_testimony_attributes(paper_number: str, session: int) -> List[Dict]:
    client = RESTClient(base_url=TESTIMONY_BASE_URL)
    base_filter = (
        "(((Request/PaperNumber eq '{paper_number}') and "
        "(Request/Legislature eq {session})) and "
        "(Inactive ne true)) and "
        "(not (startswith(LastName, '@') eq true))"
    )
    params = {
        '$filter': base_filter.format(paper_number=paper_number, session=session),
        '$orderby': 'LastName,FirstName,Organization',
        '$expand': 'Request',
        '$select': 'Id,SourceDocument,RequestId,FileType,FileSize,'
                   'NamePrefix,FirstName,LastName,NameSuffix,'
                   'Organization,PresentedDate,PolicyArea,Topic,Created,CreatedBy,LastEdited,LastEditedBy,Private,'
                   'Inactive,TestimonySubmissionId,HearingDate,LDNumber,Request',
    }
    resp = client.get(path='/', params=params)
    content = dlt_json.loads(resp.text)
    for row in content:
        row['legislature'] = session
    return content


def download_document(doc_id: int) -> bytes:
    client = RESTClient(base_url=DOCUMENT_DOWNLOAD_URL)
    params = {'doctype': 'test', 'documentId': doc_id}

    def _do():
        resp = client.get(path='/', params=params)
        return resp.content

    def _on_err(e: BaseException, attempt: int):
        if not Config.QUIET_ERRORS:
            tqdm.write(f"download_document failed for doc_id={doc_id} (attempt {attempt}): {e}")

    return retry(
        _do,
        exceptions=(Exception,),
        attempts=Config.RETRY_ATTEMPTS,
        backoff_sec=Config.RETRY_BACKOFF_SEC,
        on_error=_on_err,
    )


