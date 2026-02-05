import pandas as pd
import numpy as np
import pyarrow.parquet as pq
import os
import gc
from typing import List, Tuple, Optional

# Local imports
import config
from . import features
from . import labeling
from .utils import setup_logger, timer

class LOBPipeline:
    """
    ETL Pipeline for Limit Order Book Data.
    Handles: Raw Loading -> Volume Aggregation -> Feature Engineering -> Labeling -> Saving.
    """
    
    def __init__(self, raw_path: str, output_dir: str):
        self.raw_path = raw_path
        self.output_dir = output_dir
        self.logger = setup_logger("LOBPipeline")

    def _load_raw_row_groups(self, pf: pq.ParquetFile, row_group_indices: range) -> pd.DataFrame:
        """Load specific row groups from ParquetFile."""
        df_list = []
        for i in row_group_indices:
            if i < pf.num_row_groups:
                df_list.append(pf.read_row_group(i).to_pandas())
                
        if not df_list:
            return pd.DataFrame()
            
        df = pd.concat(df_list, ignore_index=True)
        
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

    def _aggregate_chunk(self, df_chunk: pd.DataFrame, threshold: float) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Aggregate chunk into volume bars.
        Returns: (completed_bars, residual_ticks)
        """
        if df_chunk.empty:
            return pd.DataFrame(), pd.DataFrame()
            
        cum_pv = df_chunk['pv'].cumsum()
        groups = (cum_pv // threshold).astype(int)
        max_group = groups.max()
        
        # Completed bars are those < max_group
        # (Unless max_group is 0, then everything is residue)
        completed_mask = groups < max_group
        
        residue = df_chunk[~completed_mask].copy()
        
        if not completed_mask.any():
            return pd.DataFrame(), residue
            
        vbars = df_chunk[completed_mask].groupby(groups[completed_mask]).agg(
            tradetime=('tradetime', 'last'),
            open=('price', 'first'),
            high=('price', 'max'),
            low=('price', 'min'),
            price=('price', 'last'),
            pv=('pv', 'sum'),
            quantity=('quantity', 'sum'),
            ti_raw=('ti_raw', 'sum'),
            buyer_pv=('buyer_pv', 'sum'),
            seller_pv=('seller_pv', 'sum')
        ).reset_index(drop=True)
        
        return vbars, residue

    def run(self, groups: int = None, batch_size: int = 5):
        """
        Execute the pipeline.
        
        Args:
            groups: Number of row groups to process (None = all).
            batch_size: Number of groups per batch.
        """
        self.logger.info(f"Starting ETL Pipeline on {self.raw_path}")
        
        pf = pq.ParquetFile(self.raw_path)
        total_groups = pf.num_row_groups
        
        if groups is None or groups > total_groups:
            group_range = range(total_groups)
        else:
            group_range = range(groups)
            
        batches = [group_range[i:i + batch_size] for i in range(0, len(group_range), batch_size)]
        
        all_vbars = []
        current_residue = None
        
        with timer("Chunked Processing", self.logger):
            for i, batch_indices in enumerate(batches):
                self.logger.info(f"Processing Batch {i+1}/{len(batches)} (Groups: {batch_indices})...")
                
                # Load
                df_batch = self._load_raw_row_groups(pf, batch_indices)
                if df_batch.empty: continue
                
                # Append Residue
                if current_residue is not None and not current_residue.empty:
                    df_batch = pd.concat([current_residue, df_batch], ignore_index=True)
                
                # Aggregate
                vbars_chunk, next_residue = self._aggregate_chunk(df_batch, config.VOLUME_BAR_THRESHOLD)
                
                if not vbars_chunk.empty:
                    all_vbars.append(vbars_chunk)
                    
                current_residue = next_residue
                del df_batch
                gc.collect()
        
        if not all_vbars:
            self.logger.error("No volume bars generated!")
            return
            
        self.logger.info("Concatenating all Volume Bars...")
        full_vbars = pd.concat(all_vbars, ignore_index=True)
        
        # Feature Engineering
        with timer("Feature Engineering", self.logger):
            full_vbars = features.calculate_flow_features(full_vbars)
            full_vbars = features.calculate_advanced_features(full_vbars)
            
        # Labeling (Multi-Class)
        with timer("Labeling (Multi-Cluster)", self.logger):
            full_vbars['target_ret'] = np.log(full_vbars['price'].shift(-config.HORIZON) / full_vbars['price'])
            full_vbars = labeling.apply_clustering_labeling(full_vbars)
            
        # Clean & Save
        valid_cols = config.FEATURE_COLS + ['label', 'target_ret']
        
        # Debug: Check NaNs
        null_counts = full_vbars[valid_cols].isnull().sum()
        if null_counts.sum() > 0:
            self.logger.warning(f"NaNs detected before dropna:\n{null_counts[null_counts > 0]}")
            
        ml_data = full_vbars.dropna(subset=valid_cols)
        
        self.logger.info(f"Pipeline complete. Generated {len(ml_data)} samples.")
        self._save_splits(ml_data)

    def _save_splits(self, df: pd.DataFrame):
        """Split and save Train/Val sets."""
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            
        split_idx = int(len(df) * config.TRAIN_RATIO)
        train_df = df.iloc[:split_idx]
        val_df = df.iloc[split_idx:]
        
        train_path = os.path.join(self.output_dir, "train.parquet")
        val_path = os.path.join(self.output_dir, "val.parquet")
        
        self.logger.info(f"Saving Train ({len(train_df)}) -> {train_path}")
        train_df.to_parquet(train_path)
        
        self.logger.info(f"Saving Val ({len(val_df)}) -> {val_path}")
        val_df.to_parquet(val_path)
