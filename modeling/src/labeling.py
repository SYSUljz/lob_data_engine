import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import config

def get_clustering_features(df):
    """Extract features used for clustering market moves."""
    # Ensure no NaNs in clustering features
    features = ['bs_zscore', 'quantity', 'volatility', 'cvd_zscore']
    return df[features].fillna(0)

def apply_clustering_labeling(vbars):
    """
    Apply K-Means clustering to distinguish informed moves from noise.
    
    Logic:
    1. Filter for Up-moves (Ret > Threshold)
    2. Cluster them into 2 groups (Informed vs Noise) based on microstructure.
    3. Label 'Informed' as BUY (2), rest as Neutral (1).
    4. Repeat for Down-moves (Sell).
    """
    df = vbars.copy()
    
    # Calculate future returns if not present
    if 'target_ret' not in df.columns:
        df['target_ret'] = np.log(df['price'].shift(-config.HORIZON) / df['price'])
    
    # Initialize all as Neutral (0)
    df['label'] = 0
    
    # Features for clustering
    cluster_cols = ['bs_zscore', 'quantity', 'volatility', 'cvd_zscore']
    
    # --- Process UP MOVES ---
    up_mask = df['target_ret'] > config.LABEL_THRESHOLD
    up_data = df.loc[up_mask].copy()
    
    if len(up_data) > 50: # Minimum samples to cluster
        X_up = up_data[cluster_cols].fillna(0)
        kmeans_up = KMeans(n_clusters=2, random_state=42, n_init=10)
        clusters_up = kmeans_up.fit_predict(X_up)
        
        # Sort clusters by bs_zscore (ascending) to differentiate regimes
        # bs_zscore: High = Strong Buy Pressure
        cluster_centers = kmeans_up.cluster_centers_
        bs_idx = cluster_cols.index('bs_zscore')
        sorted_indices = np.argsort(cluster_centers[:, bs_idx]) # [Low BS, High BS]
        
        # Map: Low BS (Weak Buy) -> 3, High BS (Strong Buy) -> 4
        # Create a mapping dict
        cluster_map = {old_label: new_label for old_label, new_label in zip(sorted_indices, [3, 4])}
        
        new_labels = np.vectorize(cluster_map.get)(clusters_up)
        df.loc[up_mask, 'label'] = new_labels
        
        c3 = (new_labels == 3).sum()
        c4 = (new_labels == 4).sum()
        print(f"UP Moves: Clustered into Label 3 (Weak Buy: {c3}) and Label 4 (Strong Buy: {c4}).")
    else:
        # Fallback
        print("Not enough UP samples for clustering. Labeling all as 3 (Weak Buy).")
        df.loc[up_mask, 'label'] = 3

    # --- Process DOWN MOVES ---
    down_mask = df['target_ret'] < -config.LABEL_THRESHOLD
    down_data = df.loc[down_mask].copy()
    
    if len(down_data) > 50:
        X_down = down_data[cluster_cols].fillna(0)
        kmeans_down = KMeans(n_clusters=2, random_state=42, n_init=10)
        clusters_down = kmeans_down.fit_predict(X_down)
        
        # Sort clusters by bs_zscore (ascending)
        # bs_zscore: Low (Negative) = Strong Sell Pressure, High (Less Neg) = Weak Sell
        cluster_centers = kmeans_down.cluster_centers_
        bs_idx = cluster_cols.index('bs_zscore')
        sorted_indices = np.argsort(cluster_centers[:, bs_idx]) # [Strong Sell, Weak Sell]
        
        # Map: Strong Sell -> 1, Weak Sell -> 2
        cluster_map = {old_label: new_label for old_label, new_label in zip(sorted_indices, [1, 2])}
        
        new_labels = np.vectorize(cluster_map.get)(clusters_down)
        df.loc[down_mask, 'label'] = new_labels
        
        c1 = (new_labels == 1).sum()
        c2 = (new_labels == 2).sum()
        print(f"DOWN Moves: Clustered into Label 1 (Strong Sell: {c1}) and Label 2 (Weak Sell: {c2}).")
    else:
        print("Not enough DOWN samples for clustering. Labeling all as 2 (Weak Sell).")
        df.loc[down_mask, 'label'] = 2
        
    return df
