import iisignature
import numpy as np
import pandas as pd
import config

def get_weights_ffd(d, thres, size):
    """Calculate weights for Fractional Differentiation."""
    w = [1.0]
    for k in range(1, size):
        w_k = -w[-1] * (d - k + 1) / k
        if abs(w_k) < thres: break
        w.append(w_k)
    return np.array(w[::-1]).reshape(-1, 1)

def frac_diff_ffd(series, d, thres=1e-4):
    """Apply Fractional Differentiation to a pandas Series."""
    # Ensure no NaNs before processing
    df_series = series.ffill().dropna()
    weights = get_weights_ffd(d, thres, len(df_series))
    width = len(weights) - 1
    
    output = []
    series_values = df_series.values.reshape(-1, 1)
    
    # Convolution-like application of weights
    for i in range(width, len(df_series)):
        val = np.dot(weights.T, series_values[i-width : i+1])[0, 0]
        output.append(val)
    
    # Realign index
    res = pd.Series(index=df_series.index[width:], data=output, dtype=float)
    return res.reindex(series.index)

def calculate_flow_features(vbars):
    """Calculate stationary flow and price features."""
    df = vbars.copy()
    
    # 1. Buy/Sell Imbalance (Stationary)
    df['bs_log'] = np.log(df["buyer_pv"] + 1) - np.log(df["seller_pv"] + 1)
    # Remove integral (non-stationary), use Z-Score instead
    # Using a shorter window for immediate imbalance signal
    df['bs_zscore'] = (
        df['bs_log'] - df['bs_log'].rolling(window=config.WINDOW_ROLLING_STATS).mean()
    ) / df['bs_log'].rolling(window=config.WINDOW_ROLLING_STATS).std()
    
    # 2. CVD Features (Stationary)
    df['cvd'] = df['ti_raw'].cumsum() # Helper for detrending, not a feature
    
    # Detrending CVD
    df['cvd_mean'] = df['cvd'].rolling(window=config.WINDOW_ROLLING_STATS).mean()
    df['cvd_detrended'] = df['cvd'] - df['cvd_mean']
    df['cvd_zscore'] = df['cvd_detrended'] / df['cvd'].rolling(window=config.WINDOW_ROLLING_STATS).std()
    
    # 3. Volatility
    df['returns'] = df['price'].pct_change()
    df['volatility'] = df['returns'].rolling(window=50).std()
    
    # 4. VWAP Distortion (Price vs Volume Weighted Price)
    # VWAP = Cumulative(Price * Vol) / Cumulative(Vol) over a window? 
    # Or just simple VWAP of the bar (already is price?) No, vbars['price'] is 'last'.
    # meaningful VWAP needs to be calculated from raw data or rolling vbar ap.
    # Let's approximate rolling VWAP over the stats window.
    roll_pv = df['pv'].rolling(window=config.WINDOW_ROLLING_STATS).sum()
    roll_vol = df['quantity'].rolling(window=config.WINDOW_ROLLING_STATS).sum()
    df['vwap_rolling'] = roll_pv / roll_vol
    df['vwap_distortion'] = (df['price'] - df['vwap_rolling']) / df['vwap_rolling']
    
    return df

def add_lag_features(df):
    """Add lagged features for time-series context."""
    df_lagged = df.copy()
    for feat in config.LAG_FEATURES:
        for i in range(1, config.NUM_LAGS + 1):
            df_lagged[f"{feat}_lag_{i}"] = df_lagged[feat].shift(i)
    return df_lagged

def min_max_scale_array(arr):
    min_val, max_val = arr.min(), arr.max()
    range_val = max_val - min_val
    if range_val == 0:
        return np.zeros_like(arr)
    return (arr - min_val) / range_val

def calculate_rolling_signature(df, window=config.SIG_ROLLING_WINDOW):
    """
    Calculate Rolling Signature Transform over a sliding window of bars.
    Path Dimension: 3 [Price_FFD, CVD, Cumulative_Time]
    """
    print(f"Calculating Rolling Signature (Window={window}, Level={config.SIG_ROLLING_LEVEL})...")
    
    # 1. Prepare Source Series
    # Price FFD (Stationary)
    # Note: price_ffd might have NaNs at the beginning due to FFD window
    s_price = df['price_ffd'].fillna(0).values
    
    # CVD (Cumulative Volume Delta) - This is 'cvd' column if it exists, else construct
    if 'cvd' in df.columns:
        s_cvd = df['cvd'].values
    else:
        # Reconstruct if missing (should be in calculate_flow_features)
        s_cvd = df['ti_raw'].cumsum().values
        
    # Time (Cumulative seconds from start)
    # Convert tradetime to milliseconds for better resolution
    time_seq = df['tradetime'].astype(np.int64) // 10**6 # ns to ms
    s_time = time_seq.values
    
    # OUTPUT CONTAINER
    # Initialize with NaNs (first window-1 rows will be NaN)
    n_bars = len(df)
    sig_features = np.full((n_bars, config.SIG_ROLLING_FEATURE_COUNT), np.nan)
    
    # 2. Sliding Window Loop
    # Efficient enough for <1M bars. For HFT scale, use Numba.
    for i in range(window, n_bars):
        # Extract Window
        w_price = s_price[i-window : i]
        w_cvd = s_cvd[i-window : i]
        w_time = s_time[i-window : i]
        
        # 3. Normalize (Min-Max within Window)
        # Critical for mixing units (Price ~0.1, CVD ~1000, Time ~Seconds)
        norm_price = min_max_scale_array(w_price)
        norm_cvd = min_max_scale_array(w_cvd)
        norm_time = min_max_scale_array(w_time) # effectively 0 to 1 linear ramp mostly
        
        # 4. Construct Path
        path = np.column_stack([norm_price, norm_cvd, norm_time])
        
        # 5. Center Path (Translation Invariance)
        # Even with Min-Max (which maps min->0), strict centering means start->0
        path -= path[0]
        
        # 6. Compute Signature
        sig = iisignature.sig(path, config.SIG_ROLLING_LEVEL)
        sig_features[i] = sig
        
    # 3. Merge Result
    sig_df = pd.DataFrame(sig_features, columns=config.SIG_ROLLING_FEATURES, index=df.index)
    return pd.concat([df, sig_df], axis=1)

def calculate_advanced_features(vbars):
    """Apply advanced transformations like FFD, Lags, and OHLC Geometry."""
    df = vbars.copy()
    
    # 1. OHLC Stationary Features (Geometry & Returns)
    # Ensure no division by zero for body_ratio
    epsilon = 1e-8
    range_hl = df['high'] - df['low']
    range_hl = range_hl.replace(0, epsilon)
    
    # Log Returns relative to Open
    df['log_ret_close'] = np.log(df['price'] / df['open'])
    df['log_ret_high'] = np.log(df['high'] / df['open'])
    df['log_ret_low'] = np.log(df['low'] / df['open'])
    
    # Bar Geometry
    # Upper Shadow: (High - max(Open, Close)) / Price
    df['shadow_upper'] = (df['high'] - df[['open', 'price']].max(axis=1)) / df['price']
    
    # Lower Shadow: (min(Open, Close) - Low) / Price
    df['shadow_lower'] = (df[['open', 'price']].min(axis=1) - df['low']) / df['price']
    
    # Body Ratio: |Open - Close| / (High - Low)
    df['body_ratio'] = np.abs(df['open'] - df['price']) / range_hl
    
    # Amplitude: ln(High / Low)
    # Handle Low=0? Unlikely for price, but safety first
    df['bar_amplitude'] = np.log((df['high'] + epsilon) / (df['low'] + epsilon))
    
    # FFD on Price
    print(f"Calculating FFD (d={config.FFD_D_VALUE})...")
    df['price_ffd'] = frac_diff_ffd(df['price'], d=config.FFD_D_VALUE, thres=config.FFD_THRES)
    
    # Rolling Signature Transform
    # Must be done *after* Price FFD
    df = calculate_rolling_signature(df)
    
    # Add Lags
    print(f"Generating {config.NUM_LAGS} lags for {config.LAG_FEATURES}...")
    df = add_lag_features(df)
    
    return df

def make_label(ret):
    """generate label based on return threshold."""
    if ret > config.LABEL_THRESHOLD: return 2   # Buy
    elif ret < -config.LABEL_THRESHOLD: return 0 # Sell
    else: return 1                        # Neutral

def add_labels(vbars):
    """Add target labels based on future returns."""
    df = vbars.copy()
    # Calculate future return
    df['target_ret'] = np.log(df['price'].shift(-config.HORIZON) / df['price'])
    
    # Apply labeling
    df['label'] = df['target_ret'].apply(make_label)
    
    return df
