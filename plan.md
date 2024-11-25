## Technical objectives
1. Create a data pipeline for numerous legislative sources, building on [work of Philip Mathieu](https://github.com/PhilipMathieu/maine-bills/tree/ba4b82802629976f74558304838c593f49e07396), including:
	1. [Legislative bill texts](https://lldc.mainelegislature.org/Open/)
	2. Legislative testimony
	3. Historical lists of lawmakers., by session
	4. Lobbyist compensation (historical by request and 2024 on by scraping)
2. Assess and apply NLP models for labeling testimony
	1. Candidates [from HuggingFace](https://huggingface.co/poltextlab/xlm-roberta-large-english-legislative-cap-v3) would allow for running inference locally. Labeling would be built into the data pipeline as a post-processing step.
3. Build a small website that structures this data primarily by named entities in the testimony and bills, by legislative session. This should be a lightweight, proof of concept site, but likely will need a database. 
4. Build a tf-idf feature that will show how a specific party's testimony on a given bill differs from both their testimony on other bills and the testimony of others on the bill.

## Project management
Milestones are in line with technical objectives above. The aim would be to have NLP processing and labeling complete by Project v1, with planning and design for the website that will be implemented after Proposal v2. 

I aim to have bills and testimony from at least two legislative sessions integrated into a local database by the end of September, with NLP assessment to take place in early October, prior to the Oct. 18 deadline for project v1. 

Risks include bad text quality in PDFs affecting performance of labeling or other NLP tasks. 

## EDA
Initial EDA and proof-of-concept for a data pipeline already developed by Philip Mathieu in two repositories -- one for bill text and another for testimonies. 
Initial review shows that text within the PDFs contains some typos and issues (`O` represented as `0`, etc.) that will likely need to be addressed.