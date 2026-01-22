import pandas as pd

trade_path = '/home/jack_li/python/LOB_research/lob_data_engine/data_test/BTCUSD_PERP-aggTrades-2026-01-21.parquet'
lob_path = '/home/jack_li/python/LOB_research/lob_data_engine/perp_data/2026-01-21/BTCUSDT/depthPartial/filled.parquet'

print("--- Trade Data ---")
try:
    df_trade = pd.read_parquet(trade_path)
    print(df_trade.columns.tolist())
    print(df_trade.dtypes)
    print(df_trade.head(3))
except Exception as e:
    print(e)

print("\n--- LOB Data ---")
try:
    df_lob = pd.read_parquet(lob_path)
    print(df_lob.columns.tolist())
    print(df_lob.dtypes)
    print(df_lob.head(3))
    # Check if nested lists or strings
    if 'bids' in df_lob.columns:
        print("Bids sample:", df_lob['bids'].iloc[0])
except Exception as e:
    print(e)
