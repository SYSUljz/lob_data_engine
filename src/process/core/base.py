from abc import ABC, abstractmethod
import pandas as pd
import os
import glob
from typing import Optional, List, Union

class DataProcessor(ABC):
    """
    Abstract Base Class for all data processors.
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir

    def load_parquet(self, path: str) -> pd.DataFrame:
        """Safe load of parquet file."""
        try:
            return pd.read_parquet(path)
        except Exception as e:
            print(f"Error loading {path}: {e}")
            return pd.DataFrame()

    @abstractmethod
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert raw exchange-specific columns to standard internal format.
        """
        pass

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply feature engineering or additional processing.
        """
        pass
    
    def save(self, df: pd.DataFrame, output_path: str):
        """Save dataframe to parquet."""
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, engine='pyarrow', index=False)
        print(f"Saved to {output_path}")

class TradeProcessor(DataProcessor):
    """Base class for Trade data processing."""
    
    # Standard columns for internal use
    STD_COLS = ['exchange_time', 'symbol', 'price', 'size', 'side']

    @abstractmethod
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

class LOBProcessor(DataProcessor):
    """Base class for Order Book processing."""

    @abstractmethod
    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    def flatten_book(self, df: pd.DataFrame, depth: int = 10) -> pd.DataFrame:
        """
        Generic helper to flatten array columns if needed. 
        Specific implementations might override or use this.
        """
        # Placeholder for common logic if applicable across exchanges
        return df
