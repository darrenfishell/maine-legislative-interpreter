# Check-in 1

# Preliminary results and EDA
My project will involve two primary text sources: Legislative bill texts and legislative testimony. 

To start, I've identified and ingested bill texts (including amendements, etc.) going back to the 120th legislature (2000-2001). It is promising that this long history is available from the state. 

This extraction includes text of 492,600 documents that I have stored in a local sqlite database. Importantly, this contains all of the internal document IDs used by the state's internal legislative database, which are used for subsequent API calls to return bill testimony, by document ID. I have not yet built this part of the pipeline.

## Profiling legislative documents
Total documents: 49,720

18,308 Amendments
8,488 Chaptered law
21,692 Original bill texts
772 (k) -- not sure what these documents are

## Potential weakness in data
Though bill texts are available going back to the 120th session, documentIDs that relate to testimony are only available starting with the 125th (2011-2012). 

And from preliminary review, testimony is only available from the 126th session (2013-2014) onward, which includes 3,974 original bills.

## Analysis development
Following last class, I received very helpful feedback to guide further research into more flexible methods for identifying sentence or document similarity or difference. 

I have not yet settled on a specific approach, but I will certainly use word or sentence embeddings to identify distinctive concepts or sentences among a corpus of testimony or the testimony of one party across multiple bills, rather than tf-idf analysis.