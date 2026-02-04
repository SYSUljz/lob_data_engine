import pandas as pd
import numpy as np
from ..core.base import TradeProcessor

class BinanceTradeProcessor(TradeProcessor):
    def __init__(self, data_dir: str):
        super().__init__(data_dir)

    def normalize(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize Binance AggTrades to standard format.
        Input cols: [agg_trade_id, price, quantity, first_trade_id, last_trade_id, transact_time, is_buyer_maker]
        Output cols: [exchange_time, price, size, side]
        """
        if df.empty:
            return pd.DataFrame(columns=self.STD_COLS)

        # Mapping
        # copy to avoid SettingWithCopy warnings if slice
        out = df.copy()
        
        # Rename columns
        out.rename(columns={
            'transact_time': 'exchange_time',
            'quantity': 'size'
        }, inplace=True)

        # Ensure numeric
        out['price'] = out['price'].astype(float)
        out['size'] = out['size'].astype(float)

        # Map side
        # is_buyer_maker = True -> Sell (Maker was Buyer)
        # is_buyer_maker = False -> Buy (Maker was Seller)
        out['side'] = np.where(out['is_buyer_maker'], 'sell', 'buy')

        # Select standard columns (plus original IDs if needed, but keeping it clean for now)
        # We can keep 'agg_trade_id' if helpful for debugging
        cols_to_keep = ['exchange_time', 'price', 'size', 'side']
        return out[cols_to_keep]

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main processing pipeline for trades.
        """
        df_norm = self.normalize(df)
        # Add any additional derived features here if needed in future
        return df_norm
