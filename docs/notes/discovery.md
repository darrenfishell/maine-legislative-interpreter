# Project requirements and discovery

Bill text and search available from:
https://legislature.maine.gov/mrs-search/billtext.html

First session available is 119 (1999-2000).

Bill lists can be retrieved by modifying the `offset` parameter: https://legislature.maine.gov/mrs-search/api/billtext?term=&title=&legislature=131&lmSponsorPrimary=false&reqAmendExists=false&reqAmendAdoptH=false&reqAmendAdoptS=false&reqChapterExists=false&reqFNRequired=false&reqEmergency=false&reqGovernor=false&reqBond=false&reqMandate=false&reqPublicLand=false&showExtraParameters=false&mustHave=&mustNotHave=&offset=24&pageSize=12&sortByScore=false&showBillText=false&sortAscending=false&excludeOrders=false

## Bill statuses
Final disposition can be determined by specific disposition codes, which are visible when making requests but not apparent at the outset. Consider whether this is worth capturing and cycling through to quickly evaluate bill status, or if there's another endpoint that is better to get bill history and status: 

https://legislature.maine.gov/mrs-search/api/billtext?term=&title=&legislature=131&lmSponsorPrimary=false&reqAmendExists=false&reqAmendAdoptH=false&reqAmendAdoptS=false&reqChapterExists=false&reqCommitteeAction=&reqFinalDispositionActionId=224&reqFNRequired=false&reqEmergency=false&reqGovernor=false&reqBond=false&reqMandate=false&reqPublicLand=false&showExtraParameters=true&mustHave=&mustNotHave=&offset=0&pageSize=12&sortByScore=false&showBillText=false&sortAscending=false&excludeOrders=true

## Response
In the response, the field `requestItemType` aligns with the type of document, apparently `A` for Amendment, `O` likely for Original and `C` for chaptered law.

Final dispositions are available for a document separately, but worth investigating if this is available up front in the query, to reduce items scraped.

Max page size of 500, per site.

Parameter `reqChapterExists` aligns with "Has Law Chapter" parameter in site query.