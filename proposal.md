# Analyzing legislative testimony and lobbying influence with large language models and network analysis

### Team members
- Darren Fishell

## Goal/contribution
This project aims to apply a variety of data science methods to extract, structure and unlock insights from legislative testimony, with a specific focus on environmental laws. 
The project aims not only to summarize and make legislative testimony more easily available to the public, but also to explore relationships between named entities in the texts and bills.

## Project selection
I selected the [Maine Legislation](https://github.com/ds5500/fall-2024/blob/main/project-list.md#maine-legislation) project from `project.md`. After discussion with the stakeholder, I have decided to focus this project specifically on legislative testimony and other metadata about legislation.

## Data
The source data for this comes from the Maine Legislature and the Maine Ethics Commission. Most of the data is unstructured text, as it is legislative testimony. 
The project will require structuring and feature engineering, much of which has already been built or started from the stakeholder, Philip Mathieu.

## Primary objectives
This project aims to build a replicable pipeline for extracting and structuring legislative testimony into a form that can be used for NLP analysis and network analysis, based on demographic and geographic features.
The stakeholder is seeking to develop a better detection and analysis system for prior legislation. Likely, the focus of this project will be on historical analysis and not detection going forward.
This will also involve creating a data pipeline for lobbying data from the Maine Ethics Commission, which this year released a new website for presenting lobbying information. I have also communicated with the Ethics Commission and been told that historical information is available. I am awaiting a meeting or further details on how to acccess this data.

## Potential analysis
NLP will be used to categorize topics for specific legislation from bill text, but testimony will provide most of the context. 
One key feature of the analysis will likely be tf-idf analysis on a few different dimensions, such as showing how a specific person who testifies differs from others who testified on a given bill and how it differs from their own prior testimony.
Affinity and network analysis will also develop some measures of "frequent fliers", or parties who are often testifying on the same bills. 