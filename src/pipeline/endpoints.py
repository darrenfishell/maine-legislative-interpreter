bill_text_config = {
    'base_url': 'https://legislature.maine.gov/mrs-search/api/billtext',
    'params': {
        'term': '',
        'title': '',
        'legislature': None,
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
        'offset': None,
        'pageSize': None,
        'sortByScore': 'false',
        'showBillText': 'false',
        'sortAscending': 'false',
        'excludeOrders': 'false'
    }
}

testimony_metadata_config = {
    'base_url': 'https://legislature.maine.gov/backend/breeze/data/CommitteeTestimony',
    'params': {
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
                   'Inactive,TestimonySubmissionId,HearingDate,LDNumber,Request,CommitteeTestimonyDocumentContents'
    }
}

testimony_text_config = {
    'base_url': 'https://legislature.maine.gov/backend/app/services/getDocument.aspx',
    'params': {
        'doctype': 'test',
        'documentId': None
    }
}

current_session = {
    'base_url': 'https://legislature.maine.gov/backend/breeze/data/getCurrentLegislature'
}
