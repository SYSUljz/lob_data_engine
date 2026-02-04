import pandas as pd
import numpy as np
from ..core.base import LOBProcessor
import ast

class BinanceLOBProcessor(LOBProcessor):
    def __init__(self, data_dir: str, depth: int = 10):
        super().__init__(data_dir)
        self.depth = depth

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize Binance LOB data. 
        Currently supports 'depthPartial' snapshots.
        """
        if df.empty:
            return pd.DataFrame()
        
        # Determine if we have independent snapshots (depthPartial) or stream updates
        # Based on user sample, we focus on depthPartial (snapshots)
        
        # Rename time
        # E is Event Time, usually close to matching engine time. T is transaction time.
        # depthPartial often has E.
        if 'E' in df.columns:
            df = df.rename(columns={'E': 'exchange_time'})
        elif 'T' in df.columns:
            df = df.rename(columns={'T': 'exchange_time'})
            
        return df

    def flatten_book(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Flatten 'b' and 'a' columns which contain lists of [price, size].
        """
        # Helper to safely parse if string, or return if already list
        def parse_col(col_data):
            # Check first element to see if it's string or list
            first = col_data.iloc[0]
            if isinstance(first, str):
                # Use ast.literal_eval for safe parsing of "[[...], [...]]"
                return col_data.apply(ast.literal_eval)
            return col_data

        bids = parse_col(df['b'])
        asks = parse_col(df['a'])

        # Create result dictionary for bulk DataFrame creation
        data = {
            'exchange_time': df['exchange_time'].values
        }

        # Vectorized-ish approach: Process all rows for each level
        # This is faster than row-iteration but depends on data consistency
        # Assuming all rows have at least 'depth' levels. 
        # If not, we might need a more robust loop or pad with NaNs.
        
        # Warning: This simplistic approach might fail if lists vary in length < depth
        # For robustness, we'll use a loop over the top N levels.
        
        # Convert series of lists to list of lists for easier indexing
        # Taking top 'depth' levels
        bids_list = bids.tolist()
        asks_list = asks.tolist()

        n_rows = len(df)
        
        for i in range(self.depth):
            # Price and Size for Bids
            # Try/Except for variable length handling (fill 0 or NaN)
            try:
                data[f'bid_px_{i}'] = [float(row[i][0]) if i < len(row) else np.nan for row in bids_list]
                data[f'bid_sz_{i}'] = [float(row[i][1]) if i < len(row) else np.nan for row in bids_list]
            except Exception:
                 data[f'bid_px_{i}'] = [np.nan] * n_rows
                 data[f'bid_sz_{i}'] = [np.nan] * n_rows

            # Price and Size for Asks
            try:
                data[f'ask_px_{i}'] = [float(row[i][0]) if i < len(row) else np.nan for row in asks_list]
                data[f'ask_sz_{i}'] = [float(row[i][1]) if i < len(row) else np.nan for row in asks_list]
            except Exception:
                 data[f'ask_px_{i}'] = [np.nan] * n_rows
                 data[f'ask_sz_{i}'] = [np.nan] * n_rows

        return pd.DataFrame(data)

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df_norm = self.normalize(df)
        # Flatten
        df_flat = self.flatten_book(df_norm)
        return df_flat
