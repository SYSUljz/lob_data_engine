import iisignature
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import os
import config
from . import features
from . import labeling

def preprocess_df(df):
    """Common preprocessing for raw trade dataframes."""
    if df.empty:
        return pd.DataFrame()
        
    # Format conversion
    df['tradetime'] = pd.to_datetime(df['transact_time'], unit='ms')
    cols = ["price", "quantity"]
    df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=cols + ["tradetime"]).sort_values("tradetime")
    
    # Basic tick features needed for aggregation
    df['dir'] = np.where(df['is_buyer_maker'], -1, 1)
    df['pv'] = df['price'] * df['quantity']
    df['ti_raw'] = df['dir'] * np.log(df['pv'] + 1e-8)
    df['buyer_pv'] = np.where(~df['is_buyer_maker'], df['pv'], 0)
    df['seller_pv'] = np.where(df['is_buyer_maker'], df['pv'], 0)
    
    return df

def aggregate_chunk(df_chunk, threshold=config.VOLUME_BAR_THRESHOLD):
    """
    Aggregate chunk into volume bars, returning completed bars and residual ticks.
    Returns: (vbars_df, residue_df)
    """
    if df_chunk.empty:
        return pd.DataFrame(), pd.DataFrame()
        
    # Calculate accumulated volume groups
    cum_pv = df_chunk['pv'].cumsum()
    groups = (cum_pv // threshold).astype(int)
    
    # The last group index is potentially incomplete
    max_group = groups.max()
    
    # Mask for completed bars (strictly less than max_group)
    # Exception: If max_group is 0 (first bar not even full), everything is residue
    completed_mask = groups < max_group
    
    # Residue is everything else
    residue = df_chunk[~completed_mask].copy()
    
    if not completed_mask.any():
        return pd.DataFrame(), residue
        
    # Aggregate completed bars
    # We use 'groups' as key. 
    # Note: reset_index() will verify we have unique bar rows
    vbars = df_chunk[completed_mask].groupby(groups[completed_mask]).agg(
        tradetime=('tradetime', 'last'),
        open=('price', 'first'),
        high=('price', 'max'),
        low=('price', 'min'),
        price=('price', 'last'),  # Keep 'price' as Close for compatibility
        pv=('pv', 'sum'),
        quantity=('quantity', 'sum'),
        ti_raw=('ti_raw', 'sum'),
        buyer_pv=('buyer_pv', 'sum'),
        seller_pv=('seller_pv', 'sum')
    ).reset_index(drop=True)
    
    # ==========================
    # Signature Transform Calculation
    # ==========================
    signature_data = []
    
    # Subset for completed bars
    df_completed = df_chunk[completed_mask]
    groups_completed = groups[completed_mask]
    
    # Helper for robust Min-Max scaling
    def min_max_scale(arr):
        min_val, max_val = arr.min(), arr.max()
        range_val = max_val - min_val
        if range_val == 0:
            return np.zeros_like(arr)
        return (arr - min_val) / range_val

    # Iterate over groups
    # Note: groupby yields in sorted order of keys, consistent with vbars reset_index
    for g_id, g_df in df_completed.groupby(groups_completed):
        # 1. Structure Path Components
        # [log_price, cum_cvd]
        log_price = np.log(g_df['price'].values)
        cum_cvd = g_df['ti_raw'].cumsum().values
        
        # 2. Apply Min-Max Scaling (Per Bar)
        # effectively normalizes the "shape" into a unit box
        scaled_price = min_max_scale(log_price)
        scaled_cvd = min_max_scale(cum_cvd)
        
        path = np.column_stack([scaled_price, scaled_cvd])
        
        # 3. Center Path (Translation Invariance)
        # Even after Min-Max, we want the start of the path to be the reference point (0,0)
        if len(path) > 0:
            path -= path[0]
            sig = iisignature.sig(path, config.SIG_LEVEL)
        else:
            sig = np.zeros(config.SIG_FEATURE_COUNT)
            
        signature_data.append(sig)
        
    # Append to vbars
    if signature_data:
        sig_df = pd.DataFrame(signature_data, columns=config.SIG_FEATURES)
        vbars = pd.concat([vbars, sig_df], axis=1)
    
    return vbars, residue

def process_pipeline(raw_path, row_groups=None, batch_size=5, filters=None, chunk_size=1_000_000):
    """
    Full ETL pipeline with Chunked Processing. Supports single Parquet and Hive Dataset.
    """
    print(f"Starting Pipeline (Path: {raw_path})...")
    
    # 1. Initialize Dataset
    if os.path.isdir(raw_path):
        # Hive dataset
        dataset = ds.dataset(raw_path, partitioning="hive")
        
        # Convert list of tuples to pyarrow Expression if needed
        filter_expr = None
        if filters:
            for col, op, val in filters:
                cond = (ds.field(col) == val) if op == '==' else \
                       (ds.field(col) >= val) if op == '>=' else \
                       (ds.field(col) <= val) if op == '<=' else \
                       (ds.field(col) > val) if op == '>' else \
                       (ds.field(col) < val) if op == '<' else None
                if filter_expr is None:
                    filter_expr = cond
                else:
                    filter_expr &= cond
                    
        scanner = dataset.scanner(filter=filter_expr)
        
        # Generator to group small RecordBatches into larger DataFrames
        def hive_batch_generator(target_rows=chunk_size):
            batch_buffer = []
            current_rows = 0
            for rb in scanner.to_batches():
                df_rb = rb.to_pandas()
                if df_rb.empty:
                    continue
                batch_buffer.append(df_rb)
                current_rows += len(df_rb)
                
                if current_rows >= target_rows:
                    yield pd.concat(batch_buffer, ignore_index=True)
                    batch_buffer = []
                    current_rows = 0
            
            if batch_buffer:
                yield pd.concat(batch_buffer, ignore_index=True)

        batches = hive_batch_generator()
        print(f"Using Hive Dataset streaming (Buffered batches, target ~{chunk_size} rows)...")
    else:
        # Single Parquet file
        pf = pq.ParquetFile(raw_path)
        if row_groups is None:
            row_groups = range(pf.num_row_groups)
        
        # Create batches of row group indices for manual batching
        batch_indices_list = [row_groups[i:i + batch_size] for i in range(0, len(row_groups), batch_size)]
        
        def single_file_generator():
            for batch_indices in batch_indices_list:
                df_list = []
                for idx in batch_indices:
                    df_list.append(pf.read_row_group(idx).to_pandas())
                yield pd.concat(df_list, ignore_index=True)
        
        batches = single_file_generator()
        print(f"Using single file chunked reading ({len(batch_indices_list)} batches)...")

    all_vbars = []
    current_residue = None
    
    for i, batch in enumerate(batches):
        if isinstance(batch, pa.RecordBatch):
            df_batch = batch.to_pandas()
        else:
            df_batch = batch # Already a DataFrame from our generator
            
        if df_batch.empty:
            continue
            
        # Preprocess (sort, types, etc.)
        df_batch = preprocess_df(df_batch)
        
        # Prepend Residue from previous batch
        if current_residue is not None and not current_residue.empty:
            df_batch = pd.concat([current_residue, df_batch], ignore_index=True)
            # Ensure sorted after merging residue
            df_batch = df_batch.sort_values("tradetime")
            
        # 3. Aggregate
        vbars_chunk, next_residue = aggregate_chunk(df_batch)
        
        if not vbars_chunk.empty:
            all_vbars.append(vbars_chunk)
            
        current_residue = next_residue
        print(f"Processed batch {i+1}, generated {len(vbars_chunk)} bars.")
        
        # Explicit GC
        del df_batch
        
    # 4. Concatenate all volume bars
    if not all_vbars:
        raise ValueError("No volume bars generated. Threshold might be too high.")
        
    print("Concatenating all Volume Bars...")
    full_vbars = pd.concat(all_vbars, ignore_index=True)
    
    # 5. Features (Now safe to run on full VBar dataset)
    print("Calculating Features...")
    full_vbars = features.calculate_flow_features(full_vbars)
    full_vbars = features.calculate_advanced_features(full_vbars)
    
    # 6. Labeling
    print("Applying Clustering-based Labeling...")
    full_vbars['target_ret'] = np.log(full_vbars['price'].shift(-config.HORIZON) / full_vbars['price'])
    full_vbars = labeling.apply_clustering_labeling(full_vbars)
    
    # 7. Final Cleanup
    valid_cols = config.FEATURE_COLS + ['label', 'target_ret']
    ml_data = full_vbars.dropna(subset=valid_cols)
    
    return ml_data

def save_split_data(df, output_dir, train_ratio=config.TRAIN_RATIO):
    """Split into Train/Validation and save as Parquet."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    split_idx = int(len(df) * train_ratio)
    
    train_df = df.iloc[:split_idx]
    val_df = df.iloc[split_idx:]
    
    train_path = os.path.join(output_dir, "train.parquet")
    val_path = os.path.join(output_dir, "val.parquet")
    
    print(f"Saving Train set ({len(train_df)} rows) to {train_path}...")
    train_df.to_parquet(train_path)
    
    print(f"Saving Val set ({len(val_df)} rows) to {val_path}...")
    val_df.to_parquet(val_path)
