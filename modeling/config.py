import os

# ==========================================
# Paths
# ==========================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # notebook/modeling/ -> notebook/
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
PROCESSED_DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# Raw Data Path
RAW_DATA_PATH = os.path.join(DATA_DIR, "BTCUSDT-aggTrades-2025-09-01_to_2026-01-25.parquet")
RAW_DATA_HIVE = os.path.join(DATA_DIR, "symbol=BTCUSDT")

# Data Filtering for Hive
DATA_FILTERS = [
    # ('symbol', '==', 'BTCUSDT'), # No longer needed if we point directly to the folder
    ('type', '==', 'aggTrades'),
    # Add date range filters here if needed:
    # ('date', '>=', '2025-09-01'),
    # ('date', '<=', '2026-01-25'),
]

# ==========================================
# Data Processing & Sampling
# ==========================================
VOLUME_BAR_THRESHOLD = 100_000_000  # 10M USDT per bar
WINDOW_CVD_FAST = 300
WINDOW_ROLLING_STATS = 2000

# ==========================================
# Feature Engineering - FFD
# ==========================================
FFD_D_VALUE = 0.4
FFD_THRES = 1e-4

# ==========================================
# Feature Engineering - Lags
# ==========================================
NUM_LAGS = 5
LAG_FEATURES = ['price_ffd', 'volatility', 'cvd_zscore', 'bs_zscore', 'log_ret_close', 'bar_amplitude']

# ==========================================
# Feature Engineering - Signature
# ==========================================
import iisignature
SIG_LEVEL = 2
SIG_INPUT_DIM = 2 # [log_price, cum_cvd]
SIG_FEATURE_COUNT = iisignature.siglength(SIG_INPUT_DIM, SIG_LEVEL)
SIG_FEATURES = [f"sig_{i}" for i in range(SIG_FEATURE_COUNT)]

# Rolling Signature
SIG_ROLLING_WINDOW = 20
SIG_ROLLING_LEVEL = 2
SIG_ROLLING_DIM = 3 # [price_ffd, cvd, time]
SIG_ROLLING_FEATURE_COUNT = iisignature.siglength(SIG_ROLLING_DIM, SIG_ROLLING_LEVEL)
SIG_ROLLING_FEATURES = [f"sig_roll_{i}" for i in range(SIG_ROLLING_FEATURE_COUNT)]

# ==========================================
# Labeling & Training
# ==========================================
HORIZON = 5               # Predict 5 bars ahead
LABEL_THRESHOLD = 0.0005  # 0.05% change for label
TRAIN_RATIO = 0.8

# Base features
BASE_FEATURES = [
    'pv', 'quantity', 'ti_raw', 'bs_log', 'bs_zscore', 
    'cvd_detrended', 'cvd_zscore', 'price_ffd',
    'volatility', 'vwap_distortion',
    'log_ret_close', 'log_ret_high', 'log_ret_low',
    'shadow_upper', 'shadow_lower', 'body_ratio', 'bar_amplitude'
]

# Generate Lag Column Names
LAG_COLS = [f"{feat}_lag_{i}" for feat in LAG_FEATURES for i in range(1, NUM_LAGS + 1)]

# Features to be used in training
FEATURE_COLS = BASE_FEATURES + LAG_COLS + SIG_FEATURES + SIG_ROLLING_FEATURES
