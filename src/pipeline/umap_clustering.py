from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import normalize
from sklearn.model_selection import ParameterGrid, ParameterSampler
from joblib import Parallel, delayed
from hdbscan import HDBSCAN
from hdbscan import validity_index
from umap import UMAP
from sentence_transformers import SentenceTransformer
from collections import Counter
from tqdm import tqdm
from datetime import datetime
from pathlib import Path

import pickle as pkl
import pandas as pd
import numpy as np
import os

## Disable tokenizer parallelism, to accommodate pipeline use
os.environ["TOKENIZERS_PARALLELISM"] = 'false'

class SentenceTransformerEncoder(BaseEstimator, TransformerMixin):
    def __init__(self, model=None, normalize_embeddings=True):
        if model is None:
            self.model = SentenceTransformer('all-MiniLM-L6-v2')
        else:
            self.model = model
        self.normalize_embeddings = normalize_embeddings

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        embeddings = self.model.encode(X)

        if self.normalize_embeddings:
            embeddings = normalize(embeddings, norm='l2', axis=1)

        return embeddings


def evaluate_clustering(X, labels):
    X = X.astype(np.float64)
    if len(np.unique(labels)) <= 1:  # No valid clusters
        return -np.inf
    return validity_index(X, labels)

# Post-Processing: Assign Representative Labels
def assign_representative_labels(X, labels):
    """
    Assign representative labels to clusters based on the most frequent string in each cluster.

    Parameters:
    - data: List or array of strings (original dataset)
    - labels: 1D array of cluster labels

    Returns:
    - cluster_representatives: Dictionary mapping cluster_id -> representative label
    """
    cluster_representatives = {}
    data_with_labels = pd.DataFrame({'ORIGINAL_STRING': X, 'CLUSTER_ID': labels})

    # Group by cluster and find the most frequent string in each
    for cluster_id, group in data_with_labels.groupby('CLUSTER_ID'):
        if cluster_id == -1:  # Skip noise points
            continue
        most_common_string = Counter(group['ORIGINAL_STRING']).most_common(1)[0][0]
        cluster_representatives[cluster_id] = most_common_string

    data_with_labels['CLUSTER_LABEL'] = data_with_labels['CLUSTER_ID'].map(cluster_representatives)

    return data_with_labels


def create_clustering_pipeline(distance_metric='euclidean'):
    umap_kwargs = dict(
        metric=distance_metric,
        init='random',
    )

    hdbscan_kwargs = dict(
        metric=distance_metric
    )

    if distance_metric == 'cosine':
        hdbscan_kwargs['algorithm'] = 'brute'

    pipeline = Pipeline([
        ('encoder', SentenceTransformerEncoder()),
        ('umap', UMAP(**umap_kwargs)),
        ('hdbscan', HDBSCAN(**hdbscan_kwargs))
    ])

    return pipeline

# Define a function to evaluate parameters
def evaluate_params(params, pipeline, X):
    """
    Fits the pipeline with the given parameters and computes the DBCV score.

    Args:
        params (dict): Hyperparameters to set in the pipeline.
        pipeline (Pipeline): The clustering pipeline (UMAP + HDBSCAN).
        X (array-like): Input data for clustering.

    Returns:
        tuple: (DBCV score, parameter dictionary)
    """
    pipeline.set_params(**params)
    pipeline.fit(X)

    embedding = pipeline.named_steps['umap'].embedding_
    embedding = embedding.astype(np.float64)

    labels = pipeline.named_steps['hdbscan'].labels_

    # Check for valid clustering (HDBSCAN may label everything as noise)
    if len(np.unique(labels)) <= 1:  # All noise or single cluster
        return -np.inf, params

    labeled_df = assign_representative_labels(X, labels)

    # Compute DBCV score
    score = validity_index(embedding, labels)
    return {
        'params': params,
        'score': score,
        'embeddings': embedding,
        'labels': labels,
        'model': pipeline,
        'labeled_df': labeled_df
    }

# Main search function
def clustering_hyperparameter_search(pipeline, param_dist, X, n_iter=20, n_jobs=-1, search_type='random', sample_frac=None, cluster_col=None, persist=True):
    """
    Perform hyperparameter search using DBCV as the evaluation metric.

    Args:
        pipeline (Pipeline): The clustering pipeline (UMAP + HDBSCAN).
        param_dist (dict): Parameter grid or distribution for hyperparameter search.
        X (array-like): Input data for clustering.
        n_iter (int, optional): Number of samples for random search (ignored for grid search).
        n_jobs (int): Number of parallel jobs for evaluation.
        search_type (str): 'random' for random search or 'grid' for grid search.

    Returns:
        list: Sorted list of results (higher scores first).
    """
    full_search_space = ParameterGrid(param_dist)
    full_search_space_length = len(full_search_space)
    if search_type == 'random':
        search_space = ParameterSampler(param_dist, n_iter=n_iter, random_state=42)
        search_space_length = n_iter
    elif search_type == 'grid':
        search_space = full_search_space
        search_space_length = full_search_space_length
    else:
        raise ValueError("search_type must be 'random' or 'grid'")

    if sample_frac is not None:
        print(f'Sampling {sample_frac*100} percent of {len(X)} observations')
        X = X.sample(frac=sample_frac, random_state=42).reset_index(drop=True)

    print(f'Evaluating {search_space_length} of {full_search_space_length} hyperparameter configurations over {len(X)} observations')

    if cluster_col is None:
        print(f'No column specified, using first column')
        X = X.iloc[:, 0]
    else:
        X = X[cluster_col]

    # Parallel evaluation
    results = Parallel(n_jobs=n_jobs)(
        delayed(evaluate_params)(params, pipeline, X) for params in tqdm(search_space,
    desc="Hyperparameter Search")
    )

    results_df = pd.DataFrame(results)

    # Sort results by DBCV score (higher is better)
    sorted_results = results_df.sort_values(by='score', ascending=False)

    if persist:
        persist_experiment(sorted_results)

    # Display best results
    best_result = sorted_results.iloc[0]
    print(f"Best parameters: {best_result['params']}")
    print(f"Best score: {best_result['score']}")

    return sorted_results

def persist_experiment(results_df):

    filename = f'umap_clustering_experiment_{datetime.now()}.pkl'
    filepath = Path('../../experiments/', filename)

    with filepath.open('wb') as f:
        pkl.dump(results_df, f)