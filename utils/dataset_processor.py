import os
import pandas as pd
from datetime import datetime, timedelta
import shutil
import warnings

# Ignore warnings for cleaner output
warnings.filterwarnings('ignore')

# Adjust python path if necessary or assume running from project root
# import sys
# sys.path.append(os.getcwd())

from fetch_data.src.process.process_lob import merge_parquet, process_lob_parquet
from fetch_data.src.process.process_trades import sample_lob_by_dollar_volume
from fetch_data.src.process.barrier_labeling import apply_triple_barrier_labeling, get_volatility

# Default Configuration
DATA_ROOT = os.path.join(os.getcwd(), 'fetch_data/data')
DEFAULT_N_SIG_FIGS = '5.0'
DOLLAR_VOLUME_THRESHOLD = 5000

def get_date_range(start_date: str, end_date: str):
    """Generate a list of date strings between start and end (inclusive)."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    return [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(delta.days + 1)]

def process_period_data(start_date, end_date, symbol, data_dir=DATA_ROOT, n_sig_figs=DEFAULT_N_SIG_FIGS, dollar_volume_threshold=DOLLAR_VOLUME_THRESHOLD, vertical_barrier_n=50, min_ret=0.0015):
    """
    Process LOB and Trades data for a given period and symbol.
    
    Steps:
    1. Merge daily parquet files for LOB and Trades.
    2. Process and combine daily LOB data (calculating features, labels, session IDs).
    3. Combine daily Trades data.
    4. Sample LOB by Dollar Volume (using Trades).
    5. Apply Triple Barrier Labeling.
    """
    print(f"--- Processing Period: {start_date} to {end_date} for {symbol} ---")
    days = get_date_range(start_date, end_date)
    
    # Create a temporary directory for intermediate files
    temp_dir = os.path.join(data_dir, "temp_processing", f"{symbol}_{start_date}_{end_date}")
    os.makedirs(temp_dir, exist_ok=True)
    
    combined_lob_path = os.path.join(temp_dir, "combined_lob.parquet")
    
    # Clean up previous run's combined file if exists
    if os.path.exists(combined_lob_path):
        os.remove(combined_lob_path)

    global_session_id = 0
    daily_trade_dfs = []
    
    # 1. Iterate over days to prepare daily data
    for day in days:
        # --- Prepare LOB ---
        day_lob_dir = os.path.join(data_dir, day, symbol, f"nSigFigs={n_sig_figs}")
        if os.path.exists(day_lob_dir):
            day_lob_merged = os.path.join(day_lob_dir, "merged.parquet")
            # Merge daily LOB parts if needed
            print(f"[{day}] Checking LOB data...")
            merge_parquet(day_lob_dir, day_lob_merged)
            
            # Process LOB (add features, etc.) and append to combined file
            if os.path.exists(day_lob_merged):
                global_session_id = process_lob_parquet(day_lob_merged, combined_lob_path, start_session_id=global_session_id)
        else:
            print(f"[{day}] Warning: LOB directory not found: {day_lob_dir}")

        # --- Prepare Trades ---
        day_trades_dir = os.path.join(data_dir, day, symbol, "trades")
        if os.path.exists(day_trades_dir):
            day_trades_merged = os.path.join(day_trades_dir, "merged.parquet")
            # Merge daily Trades parts if needed
            print(f"[{day}] Checking Trades data...")
            merge_parquet(day_trades_dir, day_trades_merged)
            
            # Read Trade data into memory for later concatenation
            if os.path.exists(day_trades_merged):
                try:
                    df_t = pd.read_parquet(day_trades_merged)
                    daily_trade_dfs.append(df_t)
                except Exception as e:
                    print(f"[{day}] Error reading trades: {e}")
        else:
            print(f"[{day}] Warning: Trades directory not found: {day_trades_dir}")

    # 2. Combine Trades
    print("Combining Trades Data...")
    if not daily_trade_dfs:
        print("Error: No trades data found for the entire period.")
        return None
    
    df_trades_all = pd.concat(daily_trade_dfs, axis=0, ignore_index=True)
    df_trades_all = df_trades_all.dropna(axis=1, how='all')
    
    # 3. Load Combined LOB
    print("Loading Combined LOB Data...")
    if not os.path.exists(combined_lob_path):
        print("Error: Combined LOB file was not created.")
        return None
        
    df_lob_all = pd.read_parquet(combined_lob_path)
    
    # 4. Dollar Volume Sampling
    print(f"Sampling LOB by Dollar Volume (Threshold={dollar_volume_threshold})...")
    
    # Convert timestamps
    df_lob_all['datetime'] = pd.to_datetime(df_lob_all['exchange_time'], unit='ms')
    df_lob_all = df_lob_all.set_index('datetime').sort_index()
    
    df_trades_all['datetime'] = pd.to_datetime(df_trades_all['exchange_time'], unit='ms')
    df_trades_all = df_trades_all.set_index('datetime').sort_index()
    
    # Ensure 'values' column exists (Price * Size)
    if 'values' not in df_trades_all.columns:
        df_trades_all['values'] = df_trades_all['px'].astype(float) * df_trades_all['sz'].astype(float)
        
    df_sampled = sample_lob_by_dollar_volume(df_lob_all, df_trades_all, dollar_volume_threshold)
    
    if df_sampled.empty:
        print("Warning: Sampled dataframe is empty.")
        return None

    # 5. Labeling
    print("Applying Triple Barrier Labeling...")
    # Calculate volatility if needed
    if 'mid_price' not in df_sampled.columns:
    # 检查你的列名是 ask_px_0 还是 ask_px_1
        ap = 'ask_px_0'
        bp = 'bid_px_0'
        df_sampled['mid_price'] = (df_sampled[ap] + df_sampled[bp]) / 2
    if 'volatility' not in df_sampled.columns:
        print("Calculating Volatility...")
        df_sampled['volatility'] = get_volatility(df_sampled, span=50)
    
    df_labeled = apply_triple_barrier_labeling(
        df_sampled, 
        vertical_barrier_n=vertical_barrier_n, 
        min_ret=min_ret, 
        pt=1, 
        sl=1
    )
    
    # Clean up temp files
    try:
        shutil.rmtree(temp_dir)
        print("Temporary files cleaned up.")
    except Exception as e:
        print(f"Warning: Could not remove temp dir {temp_dir}: {e}")
        
    return df_labeled

def prepare_datasets(train_start, train_end, val_start, val_end, symbol, output_dir=None, dollar_volume_threshold=DOLLAR_VOLUME_THRESHOLD, vertical_barrier_n=50, min_ret=0.0015):
    """
    Main entry point to prepare Train and Validation datasets.
    """
    if output_dir is None:
        # Default to a 'processed' directory in data root
        output_dir = os.path.join(DATA_ROOT, "processed_datasets")
    
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"========== STARTING DATASET PREPARATION for {symbol} ==========")
    
    # --- Process Training Set ---
    print(f"\n>>> Generating Training Set ({train_start} -> {train_end})")
    df_train = process_period_data(train_start, train_end, symbol, 
                                     dollar_volume_threshold=dollar_volume_threshold, 
                                     vertical_barrier_n=vertical_barrier_n, 
                                     min_ret=min_ret)
    if df_train is not None:
        train_path = os.path.join(output_dir, f"{symbol}_train.parquet")
        df_train.to_parquet(train_path)
        print(f"SUCCESS: Training data saved to: {train_path} (Shape: {df_train.shape})")
    else:
        print("FAILED: Training data generation failed.")

    # --- Process Validation Set ---
    print(f"\n>>> Generating Validation Set ({val_start} -> {val_end})")
    df_val = process_period_data(val_start, val_end, symbol, 
                                   dollar_volume_threshold=dollar_volume_threshold, 
                                   vertical_barrier_n=vertical_barrier_n, 
                                   min_ret=min_ret)
    if df_val is not None:
        val_path = os.path.join(output_dir, f"{symbol}_val.parquet")
        df_val.to_parquet(val_path)
        print(f"SUCCESS: Validation data saved to: {val_path} (Shape: {df_val.shape})")
    else:
        print("FAILED: Validation data generation failed.")
        
    print("\n========== ALL TASKS COMPLETED ==========")

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare dataset for crypto modeling")

    parser.add_argument("--symbol", type=str, required=True, help="Symbol, e.g., BTC")
    parser.add_argument("--train-start", type=str, required=True, help="Training start date YYYY-MM-DD")
    parser.add_argument("--train-end", type=str, required=True, help="Training end date YYYY-MM-DD")
    parser.add_argument("--val-start", type=str, required=True, help="Validation start date YYYY-MM-DD")
    parser.add_argument("--val-end", type=str, required=True, help="Validation end date YYYY-MM-DD")

    # 可调参数
    parser.add_argument("--dollar-volume-threshold", type=float, default=7000)
    parser.add_argument("--vertical-barrier-n", type=int, default=60)
    parser.add_argument("--min-ret", type=float, default=0.002)

    args = parser.parse_args()

    # 调用你的函数
    prepare_datasets(
        args.train_start,
        args.train_end,
        args.val_start,
        args.val_end,
        args.symbol,
        dollar_volume_threshold=args.dollar_volume_threshold,
        vertical_barrier_n=args.vertical_barrier_n,
        min_ret=args.min_ret
    )