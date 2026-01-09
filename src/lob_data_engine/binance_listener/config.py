import os

# Binance Spot WebSocket URL
WS_URL = "wss://stream.binance.com:9443/ws"

# Target symbols (should be uppercase for internal logic, converted to lowercase for stream)
# Using USDT pairs as proxies for the "BTC", "ETH", "SOL" interest
TARGET_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

DATA_DIR = os.getenv("DATA_DIR", "./data")
FLUSH_INTERVAL = 300
BATCH_SIZE = 10000

# Configuration for subscription
# Binance depth update speed (100ms or 1000ms)
DEPTH_SPEED = "100ms"

# Stream type: 'diff' (incremental updates, default) or 'partial' (top N snapshot)
DEPTH_STREAM_TYPE = "diff" 

# Levels for partial stream (5, 10, 20). Ignored if DEPTH_STREAM_TYPE is 'diff'
#DEPTH_LEVELS = 20

# Interval to fetch full snapshot and validate/resync local order book (seconds)
# Default: 3600 (1 hour)
VALIDATION_INTERVAL = 3600
