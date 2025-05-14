# Project

- [Plan](project-reports/plan.md)
- [Proposal](project-reports/proposal.md)
- **New**: [EDA results](project-reports/eda_phase1.md)

# Reproducibility

The initial release of this project includes the data pipeline for extracting legislative bills, associated testimony metadata and testimony texts (from raw PDFs). The existing scripts store this data in a local sqlite database in three tables: 

- `BILL_TEXT`
- `TESTIMONY_METADATA`
- `TESTIMONY_TEXT`

As a note of caution for local storage, the database constructed in this repository contains requires roughly 30GB of space. As the project moves into subsequent phases, I may look to move the database to another location or provide other options for data access. 

The associated Makefile will create the proper conda environment, extract data from the source public endpoints from the Maine Legislature's web resources, and store the data into a sqlite database within the project's `data` directory.

## Reproduction Instructions

To reproduce this data pipeline, first clone this repository, using: 

Here’s a template for reproduction instructions that you can use for cloning a Git repository and running a Makefile. You can customize it according to your project needs.

### Prerequisites

Before you begin, ensure you have the following installed:

- Git
- Make

### Cloning the Repository

	1.	Open your terminal (or command prompt).
	2.	Navigate to the directory where you want to clone the repository:

`cd /path/to/your/directory`


	3.	Clone the repository using the following command:

`git clone https://github.com/ds5500/project-darrenfishell.git`

	4.	Navigate into the cloned directory:

`cd repo-name`

Running the Makefile

	1.	If the Makefile is in the root of the cloned directory, you can run the following command:

`make`

### Example Commands
```
# Clone the repository
git clone https://github.com/username/repo-name.git

# Navigate into the directory
cd repo-name

# Run the Makefile
make

# Run a specific target
make target-name
```

There are additional scripts stored within this package that are still in development that will allow for extending this analysis to review specific subsets of the original information. This includes a class for `LegislativeSession` with methods that will ultimately replicate the data extraction functions currently within [`pipeline.py`](src/pipeline.py). 

### Reproducibility concerns for sentence embedding steps
I have small prototypes of using `sentence_transformers` to get sentence embeddings from the testimony corpus. My intention is to attempt to build and run inference on this corpus locally, using GPU acceleration for Apple Silicon. The procedures I write will be adaptable to other GPUs, but will require some customization as I will not be able to test the performance of this step on other hardware. 

I may include guidance for generating these sentence embeddings with cloud resources such as Google Colab and an NVIDIA GPU. This would be run and tested on a subset of the full testimony corpus. 

## Data availability
Availability of the data relies on resources from the Maine State Legislature, which may be intermittently unavailable. During development, I did encounter maintenance windows that prevented access of underlying endpoints.

The scripts are written to make requests asynchronously, but in batches of 10 by default. This can be modified within `pipeline.py` in the calls to `fetch.run_in_batches()`, using the named argument `batch_size`.

## Data quality
Within the testimony metadata, there are occasionally JSON formatting issues that will be reported out during the execution of `load_testimonies()`. I am still assessing the impacts of these unterminated strings on the completeness of testimony documents across all sessions. 

## Visualizations
All visualizations are generated after construction of the database, showing some basic patterns and data quality profiling issues, within the `figs` directory.