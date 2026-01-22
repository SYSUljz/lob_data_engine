import sys
import os

# Add the src path to sys.path to ensure we can import 'process'
# Adjust as needed based on where this script is run
sys.path.append('/home/jack_li/python/LOB_research/lob_data_engine/src')

from process.traders.binance import BinanceTradeProcessor
from process.lob.binance import BinanceLOBProcessor

def test_binance_processors():
    trade_path = '/home/jack_li/python/LOB_research/lob_data_engine/data_test/BTCUSD_PERP-aggTrades-2026-01-21.parquet'
    lob_path = '/home/jack_li/python/LOB_research/lob_data_engine/perp_data/2026-01-21/BTCUSDT/depthPartial/filled.parquet'

    print("=== Testing BinanceTradeProcessor ===")
    if os.path.exists(trade_path):
        tp = BinanceTradeProcessor(data_dir="")
        df_raw = tp.load_parquet(trade_path)
        print(f"Loaded Raw Trades: {df_raw.shape}")
        
        df_proc = tp.process(df_raw)
        print(f"Processed Trades: {df_proc.shape}")
        print(df_proc.head())
        print(df_proc.dtypes)
    else:
        print(f"Trade file not found: {trade_path}")

    print("\n=== Testing BinanceLOBProcessor ===")
    if os.path.exists(lob_path):
        lp = BinanceLOBProcessor(data_dir="", depth=5) # testing depth=5
        df_raw = lp.load_parquet(lob_path)
        print(f"Loaded Raw LOB: {df_raw.shape}")
        
        df_proc = lp.process(df_raw)
        print(f"Processed LOB: {df_proc.shape}")
        print(df_proc.head())
        # Check columns
        expected_cols = [f'bid_px_{i}' for i in range(5)]
        print("Expected columns present:", all(c in df_proc.columns for c in expected_cols))
    else:
        print(f"LOB file not found: {lob_path}")

if __name__ == "__main__":
    test_binance_processors()
