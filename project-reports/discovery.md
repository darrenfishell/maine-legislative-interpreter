# Project requirements and discovery

Bill text and search available from:
https://legislature.maine.gov/mrs-search/billtext.html

First session available is 120 (2001-2002), but testimony is a little more limited.

## Bill texts
Bill lists can be retrieved by modifying the `offset` parameter:

```
base_url = 'https://legislature.maine.gov/mrs-search/api/billtext'

query_params = {
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
    'offset': 0,
    'pageSize': None,
    'sortByScore': 'false',
    'showBillText': 'false',
    'sortAscending': 'false',
    'excludeOrders': 'false'
}
```

Full list of committee parameters can be seen in this query, which contains all testimony results. 

## Testimony
Begins with 126th legislature.

Perhaps just GET this whole dataset:

https://legislature.maine.gov/backend/breeze/data/CommitteeTestimony

For testimony, the document id to retrieve the full text is the `Id` field, such as 10029721 to retrieve this document from Margaret Estabrook. 

https://legislature.maine.gov/backend/app/services/getDocument.aspx?doctype=test&documentId=10029721

### Alternative
Consider instead using these pages per session: 
1. First retrieve information for a session, https://legislature.maine.gov/bills/getTestyCmte.asp?snum=126
   - Response includes committee codes, which should be cycled through to get dates
2. Committee codes should be used for each query, such as: https://legislature.maine.gov/bills/getTestyCmteDates.asp?snum=126&comCode=ACF
   - These results include all "presented dates", which then need to be reformatted for second query
3. Such as: https://legislature.maine.gov/bills/getTestyForCmteHearingDate.asp?hearingDate=2013-01-29&comCode=ACF
   - This then generates an ID that can be used in: https://legislature.maine.gov/bills/getTestimonyDoc.asp?id=472

## Lobbying information

1. Retrieve list of clients for a year 
   1. Endpoint can have set number of pages, returns total number of results
   2. https://api.mainelobbying.com/api/ClientRegistration/?pageSize=100&pageNumber=1&sortBy=name&direction=asc&year=2024
   3. From that endpoint, retrieve all client IDs
2. For each client ID, retrieve total compensation for lobbying:
   1. https://api.mainelobbying.com/api/ClientRegistration/01HK8HZAE2A8MPN97RMMDCS4T1/year/2024/totals
      2. This contains summary amounts spent on lobbying for a given client
   3. https://api.mainelobbying.com/api/ClientRegistration/01HK8HZAE2A8MPN97RMMDCS4T1/year/2024/legislative-documents?pageNumber=1&pageSize=100&sortBy=ldNumber&direction=desc
      4. This endpoint includes all LDs for the given client and lobbyist
    
## Sample for bill details

https://legislature.maine.gov/backend/breeze/data/Request?$filter=(Legislature%20eq%20131)%20and%20(PaperNumber%20eq%20%27HP0004%27)&$expand=ActionFinalDisp%2CActionCouncil%2CActionGovernor%2CActionRequest%2CActionVeto%2CActionCommittee%2CCommittee1stRef%2CCommittee2ndRef%2CCommitteeFrom%2CRequestItem%2CRequestTitleSections%2CRequestAction