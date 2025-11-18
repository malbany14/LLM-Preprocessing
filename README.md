## Introduction
A common step in language model training is pre-processing. Before training on text data, that data needs to be cleaned in order to remove low-quality text.

The goal of this pre-processing pipeline is to create a quality multi-domain english language dataset.

The pipeline is designed from scratch in a modular way. The pipeline class orchestrates different pipeline steps which handle the preprocessing. Validation is handled throughout the pipeline steps through a validator class.

The pipeline preprocessing steps are implemented in the following order
1. Null removal
2. Utf-8 cleaning
3. Html cleaning
4. Special character cleaning
5. Quality filtering
6. Language cleaning
7. Exact deduplication
8. Fuzzy deduplication
9. Pii removal
10. Toxicity removal
11. Case normalisation

Finally a tokeniser is used on the cleaned data.

## Setup Data

This project is originally based on the `mainpipe_data_v1.jsonl` dataset.

Download the dataset from: 
   [https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl](https://s3.us-east-1.amazonaws.com/mainpipe.maincode.com/mainpipe_data_v1.jsonl)

Place it in the `data/raw/` folder:

Alternatively place any raw data ready for pre-processing in the data/raw folder. The data structure this works on is ['text']['url']

If using more than 1,000,000 rows of data, update MAX_ROWS in main.py to set above the number of text records.


### Install Dependencies

Use a virtual environment (windows bash):

python -m venv venv

venv\Scripts\activate

pip install --upgrade pip

pip install -r requirements.txt

## Running the Pipeline

Once the dataset is in `data/raw/`, you can run the full preprocessing pipeline. The main orchestration script is located in `mainpipe/Pipeline/main.py`.

## Output

The tokenised dataset will be generated under 'data/cleaned' as 'cleaned_csv_test_tokens.npy'

The cleaned text dataset will also be generated in 'data/cleaned' as 'cleaned_csv_test.JSONL'

The overall pipeline report as well as csv's of dropped rows will be generated in 'reports'

## Data Exploration

See '1.1 - EDA Writeup.ipynb' under mainpipe/mAlbany jupyter notebooks