import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

from sklearn.decomposition import PCA
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import normalize

def get_embeddings(df, string_col, model_dir):
    # Load sentence transformer model
    model = SentenceTransformer(model_dir)

    # Generate embeddings for unique organization names
    org_names = df[string_col].unique()
    embeddings = np.array([model.encode(name) for name in org_names])
    normalized_embeddings = normalize(embeddings, norm='l2', axis=1)

    # Create a mapping of organization names to embeddings
    name_to_embedding = dict(zip(org_names, normalized_embeddings))

    # Generate weighted embeddings based on counts
    embeddings = np.vstack([
        name_to_embedding[name] for name in df['organization']
    ])
    return embeddings

def cluster_strings(df, embeddings, eps=0.3, min_samples=4):
    """
        Cluster organization names using DBSCAN on sentence embeddings

        Parameters:
        df: DataFrame with 'count' column indicating frequency
        cluster_col: column name to cluster
        eps: DBSCAN epsilon parameter (distance threshold)
        min_samples: DBSCAN min_samples parameter

        Returns:
        DataFrame with original data plus cluster labels and representative names
        """
    # Perform DBSCAN clustering
    clustering = DBSCAN(eps=eps, min_samples=min_samples, metric='cosine').fit(embeddings)

    # Map DBSCAN labels back to the original dataframe
    df['cluster'] = clustering.labels_

    cluster_representatives = {}
    for cluster_id in set(clustering.labels_):
        if cluster_id != -1:  # Skip noise points
            cluster_mask = df['cluster'] == cluster_id
            cluster_df = df[cluster_mask]
            representative = cluster_df.loc[cluster_df['count'].idxmax(), 'organization']
            cluster_representatives[cluster_id] = representative

    # Map cluster labels to representative names
    df['grouped_name'] = df['cluster'].map(cluster_representatives)
    # Fill in noise points with original org value
    df.loc[df['cluster'] == -1, 'grouped_name'] = df.loc[df['cluster'] == -1, 'organization']
    return df

def plot_with_pca(embedding_mat, idf):
    # Reduce embeddings to 2D using PCA
    pca = PCA(n_components=2)
    reduced_embeddings = pca.fit_transform(embedding_mat)

    # Visualize the clusters
    plt.figure(figsize=(10, 8))

    # Plot each point with color based on cluster label
    scatter = plt.scatter(reduced_embeddings[:, 0], reduced_embeddings[:, 1], c=idf['cluster'], cmap='viridis', marker='o')

    # # Add labels for each point (optional)
    # for i, name in enumerate(org_names):
    #     plt.text(reduced_embeddings[i, 0], reduced_embeddings[i, 1], name, fontsize=9, ha='right', va='bottom')

    # Customize plot
    plt.title("PCA Projection of Organization Names Clusters")
    plt.xlabel("PCA Component 1")
    plt.ylabel("PCA Component 2")
    plt.colorbar(scatter, label='Cluster Label')
    plt.show()

# def evaluate_clustering():
