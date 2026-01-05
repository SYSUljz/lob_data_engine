import os

WS_URL = "wss://api.hyperliquid.xyz/ws"
API_URL = "https://api.hyperliquid.xyz/info"
TARGET_COINS = ["BTC", "ETH", "SOL"]
DATA_DIR = os.getenv("DATA_DIR", "./data")
FLUSH_INTERVAL = 300
BATCH_SIZE = 5000

# Coin specific configuration (e.g. to adjust tick aggregation)
# nSigFigs: 2-5 (default 5 usually). Lower means coarser ticks (larger range).
# Can be an int (e.g. 4) or a list of ints (e.g. [4, 5]) to subscribe to multiple granularities.
# mantissa: 2 or 5 (optional, used with nSigFigs)
COIN_CONFIG = {
    "SOL": {"nSigFigs": [5]},  # Aggregate SOL to increase visible price range
    "BTC": {"nSigFigs": [5]}, # Fetch both 4 and 5 sig figs
    "ETH": {"nSigFigs": [5]},
}