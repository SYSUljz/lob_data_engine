import os

# Binance Perpetual WebSocket URL
WS_URL = "wss://fstream.binance.com/ws"

# Target symbols
TARGET_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

DATA_DIR = os.getenv("DATA_DIR", "./perp_data")
FLUSH_INTERVAL = 300
BATCH_SIZE = 10000

# Configuration for subscription
# Binance depth update speed (100ms, 250ms, or 500ms for Futures)
DEPTH_SPEED = "100ms"

# Stream type: 'diff' (incremental updates, default) or 'partial' (top N snapshot)
DEPTH_STREAM_TYPE = "partial" 

# Levels for partial stream (5, 10, 20). Ignored if DEPTH_STREAM_TYPE is 'diff'
#DEPTH_LEVELS = 20

# Interval to fetch full snapshot and validate/resync local order book (seconds)
# Default: 3600 (1 hour)
VALIDATION_INTERVAL = 3600

# Channel Subscription Toggles
ENABLE_TRADE = False
ENABLE_AGGTRADE = False
ENABLE_FORCE_ORDER = True
