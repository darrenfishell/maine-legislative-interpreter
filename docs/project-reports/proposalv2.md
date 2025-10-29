# Analyzing legislative testimony and lobbying influence with large language models

## Phase 1
- Ingest data
  - [Bill texts and metadata](#bill-text) (50,355)
  - [Testimony metadata](#testimony-metadata) (128,437)
  - [Testimony text](#testimony-detail) (121,777)

- Initial EDA focused on Sierra Club
  - Testimony volume up dramatically in 130th legislature
  - Testimony touches much broader range of topics
    - Avg. 8 policy areas until 130th legislature, rose to 14

- Assessment of NLP models to use
  - Propose using [`all-MiniLM-L6-v2`](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) with sentence_transformers package
  - For bill classification, assess using [`xlm-roberta-large`](https://huggingface.co/poltextlab/xlm-roberta-large-english-legislative-cap-v3)
    - BERT encoder best for summarization/classification tasks
    - Returns logits for all probable matches against [major topic codes](https://www.comparativeagendas.net/pages/master-codebook) from Comparative Agendas Project

- Initial EDA in matplotlib and repo, as well as Tableau (for rapid prototyping)

### Database schemas
#### Bill Text
| Field Name         | Description |
|--------------------|-------------|
| url                |             |
| title              |             |
| ordering           |             |
| location           |             |
| requestId          |             |
| documentId         |             |
| ldNumber           |             |
| paperNumber        |             |
| legislature        |             |
| itemNumber         |             |
| requestItemType    |             |
| summary            |             |
| broadSubject       |             |
| majorSubject       |             |
| minorSubject       |             |
| detailSubject      |             |
| body               |             |

#### Testimony metadata
| Field Name              | Description |
|-------------------------|-------------|
| $id                     |             |
| Id                      |             |
| SourceDocument           |             |
| RequestId               |             |
| FileType                |             |
| FileSize                |             |
| NamePrefix              |             |
| FirstName               |             |
| LastName                |             |
| NameSuffix              |             |
| Organization            |             |
| PresentedDate           |             |
| PolicyArea              |             |
| Topic                   |             |
| Created                 |             |
| CreatedBy               |             |
| LastEdited              |             |
| LastEditedBy            |             |
| Private                 |             |
| Inactive                |             |
| TestimonySubmissionId   |             |
| HearingDate             |             |
| LDNumber                |             |
| Request                 |             |
| legislature             |             |

#### Testimony detail
| Field Name  | Description |
|-------------|-------------|
| query_url   |             |
| doc_text    |             |
| doc_id      |             |

## Phase 2
- Implementation of data cleaning
  -  Samples of strings for Sierra Club: `Sierra Club, Maine Lobstering Union/Sierra Club, Sierra Club Maine Chapter, Sierra Club Maine, Sierra Club of Maine, Sierra Club Maine Energy Team, Maine Rail Transit Coalition and Sierra Club Maine, Sierra Club Energy Team, Sierra Club Maine chapter, SierraClub, Sierra Club, Maine`

- Implementation of semantic search similarity scoring
  - Evaluation of tokenizer for semantic similarity scoring of bills along two dimensions -- similarity to all other testimony sentences for the bill and similarity to all other testimony sentences for that organization (dependent on prior step).

- Define and implement affinity scoring between testifying parties
  - Proposed: `Bills lobbied by A and B / (Bills lobbied by A + Bills lobbied by B)` 

- Convert Tableau dashboard prototypes to Observable HQ framework

- Development of Github Pages, to present bill testimony trends by legislature -> bills and legislature -> organizations