## Extracted testimony

Dating back to 2013, with a total of 137,277 documents to download and parse as text. 

Unsure yet if there will be problems in the testimony text from OCR or other processing. 

## Assessed Maine Lobbying database information
Evaluated how to scrape the lobbying database and structure this information (need to follow up with Ethics commission about historical lobbyist compensation data)

## Evaluated method for generating similarities in testimony

Will use sentence_transformer and encoder-only modeling to develop and rate similarity scores in testimony.

This will be used to both find the most distinctive sentences for a given testifier among the rest of the testimony and also the most distinctive sentence from that testifier when compared to the rest of their testimony.

This identification may pose challenges in the case that the testifier's name does not exactly match across the database. Any name changes for an organization will also be difficult to assess and track.